// Package leader 实现 M2a 写者选主（ADR-0007）：PG advisory lock 会话租约。
// controller（reconcile+放置）只在持锁实例运行，备实例只读待命；
// 锁随连接断开自动释放，无脑裂。
//
// P2-4：持锁连接周期心跳（SELECT 1）。PG 端可能静默释放连接（网络分区、
// PG 重启、连接被杀），此时本实例仍以为自己是 leader，而锁已可被他人
// 获取——双 controller 并跑。心跳失败立即辞任（cancel leaseCtx），把
// 脑裂窗口收敛到一个心跳周期。
//
// 生产就绪 P1：选主使用独立于业务池的专用连接（pgx.ConnectConfig）。
// 业务池耗尽（慢查询/连接泄漏）不得卡住选主的抢锁/心跳/解锁路径；
// 该连接不服务任何业务 SQL，天然不会被业务负载饿死。
package leader

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Key 是全局选主键（所有实例共用）。
const Key = "firepaas:leader"

// RetryEvery 是未抢到锁时的重试间隔。
const RetryEvery = 5 * time.Second

// HeartbeatEvery 是持锁连接的心跳周期（P2-4）。
const HeartbeatEvery = 10 * time.Second

// heartbeatTimeout 是单次心跳与解锁的执行时限。
const heartbeatTimeout = 5 * time.Second

// connectTimeout 是专用连接拨号时限（dsn 未显式配置时生效）。
const connectTimeout = 5 * time.Second

// Elect 阻塞直到 ctx 取消：反复尝试抢锁，成功后运行 onLeader
// （controller+nodemanager），锁丢失/ctx 取消后释放并重试。
// dsn 用于建立选主专用连接（不经业务池）。
func Elect(ctx context.Context, dsn string, key string, onLeader func(ctx context.Context) error) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse leader dsn: %w", err)
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = connectTimeout
	}
	for ctx.Err() == nil {
		// 每次抢锁用独立 leaseCtx：onLeader 返回/进程退出/心跳失败时都释放锁。
		leaseCtx, cancel := context.WithCancel(ctx)
		acquired, err := tryAcquire(leaseCtx, cancel, cfg, key)
		if err != nil {
			cancel()
			slog.Error("leader acquire", "error", err)
		}
		if acquired {
			slog.Info("leader acquired", "key", key)
			err := runGuarded(leaseCtx, onLeader)
			cancel() // 触发后台解锁
			slog.Warn("leader lease ended", "key", key, "error", err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(RetryEvery):
		}
	}
	return ctx.Err()
}

// runGuarded 兜住 onLeader 的 panic：log 后按普通错误返回，走既有的辞任
// 与重试路径（panic 不升级为进程崩溃，租约释放语义不变）。
func runGuarded(ctx context.Context, onLeader func(ctx context.Context) error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("leader callback panic: %v", p)
		}
	}()
	return onLeader(ctx)
}

// tryAcquire 在专用连接上抢锁；成功后立即返回 true。cancel 是 leaseCtx 的
// 取消函数：持锁 goroutine 心跳失败时调用它辞任（P2-4），锁由该 goroutine
// 持有到辞任/退出，连接断开自动释放。
func tryAcquire(ctx context.Context, cancel context.CancelFunc, cfg *pgx.ConnConfig, key string) (bool, error) {
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return false, fmt.Errorf("dial leader conn: %w", err)
	}

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&got); err != nil {
		_ = conn.Close(context.Background())
		return false, fmt.Errorf("pg_try_advisory_lock: %w", err)
	}
	if !got {
		_ = conn.Close(context.Background())
		return false, nil
	}

	go func() {
		defer func() {
			// 解锁加时限：连接所在会话可能已僵死，无限期阻塞会泄漏专用连接
			// 与 goroutine；超时后由 Close 收尾（服务端会话销毁即放锁）。
			uCtx, uCancel := context.WithTimeout(context.WithoutCancel(ctx), heartbeatTimeout)
			_, _ = conn.Exec(uCtx, `SELECT pg_advisory_unlock(hashtext($1))`, key)
			uCancel()
			_ = conn.Close(context.Background())
		}()
		ticker := time.NewTicker(HeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return // 正常卸任：解锁由 defer 完成
			case <-ticker.C:
				// P2-4 心跳：连接在服务端已死时 Exec 立即报错（TCP RST），
				// 半开连接由心跳时限免底。失败即辞任，消除静默双主。
				hbCtx, hbCancel := context.WithTimeout(context.WithoutCancel(ctx), heartbeatTimeout)
				_, err := conn.Exec(hbCtx, `SELECT 1`)
				hbCancel()
				if err != nil {
					slog.Error("leader heartbeat failed, resigning", "key", key, "error", err)
					cancel() // 触发 onLeader 退出 + defer 解锁（best-effort）
					return
				}
			}
		}
	}()
	return true, nil
}
