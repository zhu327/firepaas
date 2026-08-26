// Package leader 实现 M2a 写者选主（ADR-0007）：PG advisory lock 会话租约。
// controller（reconcile+放置）只在持锁实例运行，备实例只读待命；
// 锁随连接断开自动释放，无脑裂。
//
// P2-4：持锁连接周期心跳（SELECT 1）。PG 端可能静默释放连接（网络分区、
// PG 重启、连接被杀），此时本实例仍以为自己是 leader，而锁已可被他人
// 获取——双 controller 并跑。心跳失败立即辞任（cancel leaseCtx），把
// 脑裂窗口收敛到一个心跳周期。
package leader

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Key 是全局选主键（所有实例共用）。
const Key = "firepaas:leader"

// RetryEvery 是未抢到锁时的重试间隔。
const RetryEvery = 5 * time.Second

// HeartbeatEvery 是持锁连接的心跳周期（P2-4）。
const HeartbeatEvery = 10 * time.Second

// Elect 阻塞直到 ctx 取消：反复尝试抢锁，成功后运行 onLeader
// （controller+nodemanager），锁丢失/ctx 取消后释放并重试。
func Elect(ctx context.Context, pool *pgxpool.Pool, key string, onLeader func(ctx context.Context) error) error {
	for ctx.Err() == nil {
		// 每次抢锁用独立 leaseCtx：onLeader 返回/进程退出/心跳失败时都释放锁。
		leaseCtx, cancel := context.WithCancel(ctx)
		acquired, err := tryAcquire(leaseCtx, cancel, pool, key)
		if err != nil {
			cancel()
			slog.Error("leader acquire", "error", err)
		}
		if acquired {
			slog.Info("leader acquired", "key", key)
			err := onLeader(leaseCtx)
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

// tryAcquire 在专用连接上抢锁；成功后立即返回 true。cancel 是 leaseCtx 的
// 取消函数：持锁 goroutine 心跳失败时调用它辞任（P2-4），锁由该 goroutine
// 持有到辞任/退出，连接断开自动释放。
func tryAcquire(ctx context.Context, cancel context.CancelFunc, pool *pgxpool.Pool, key string) (bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire pg conn: %w", err)
	}

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&got); err != nil {
		conn.Release()
		return false, fmt.Errorf("pg_try_advisory_lock: %w", err)
	}
	if !got {
		conn.Release()
		return false, nil
	}

	go func() {
		defer func() {
			_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, key)
			conn.Release()
		}()
		ticker := time.NewTicker(HeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return // 正常卸任：解锁由 defer 完成
			case <-ticker.C:
				// P2-4 心跳：连接在服务端已死时 Exec 立即报错（TCP RST/超时由
				// pool 的 conn lifetime 兜底）。失败即辞任，消除静默双主。
				hbCtx, hbCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
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
