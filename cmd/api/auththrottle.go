// auththrottle.go（R2 评审 P1，401 限流）：无效/已撤销/已过期 key 的 401
// 尝试按来源 IP 计入进程内令牌桶；桶空时对后续尝试直接 429，且不触碰 PG
// （只做 hash + 桶判定）——暴力探测在到达数据库之前就被短路。
//
// 实现风格对齐 internal/controlplane/ratelimit（速率+突发令牌桶），但刻意
// 不依赖 Redis：认证失败保护必须在任何外部依赖不可用时仍生效。
//
// 并发语义：peek（hasCapacity）与 consume（record）是两个锁操作，高并发下
// 少数请求可能在桶将空时同时穿过 peek 再做一次无效 key 查询——这是可接受
// 的软上限（每次无效尝试仍只消费一枚令牌，耗尽后 429 路径稳定）。
package main

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// authFailureThrottle 是来源 IP 的令牌桶集合（进程内、有界）。
type authFailureThrottle struct {
	ratePerSec float64
	burst      float64

	mu      sync.Mutex
	buckets map[string]throttleBucket
}

type throttleBucket struct {
	tokens float64
	at     time.Time
}

// newAuthFailureThrottle：ratePerMinute > 0 才启用；<=0 关闭（nil 语义在调用方）。
func newAuthFailureThrottle(ratePerMinute int) *authFailureThrottle {
	if ratePerMinute <= 0 {
		return nil
	}
	return &authFailureThrottle{
		ratePerSec: float64(ratePerMinute) / 60.0,
		burst:      float64(ratePerMinute),
		buckets:    map[string]throttleBucket{},
	}
}

// clientIP 取限流键：只信 RemoteAddr（不信任 X-Forwarded-For——那是客户端
// 自报面，容易被拿来绕限流）。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// bucketAt 读取（并按经过时间补发）某 IP 的当前桶。调用方持锁。
func (t *authFailureThrottle) bucketAt(ip string, now time.Time) throttleBucket {
	b := t.buckets[ip]
	if b.at.IsZero() {
		return throttleBucket{tokens: t.burst, at: now}
	}
	refilled := b.tokens + now.Sub(b.at).Seconds()*t.ratePerSec
	if refilled > t.burst {
		refilled = t.burst
	}
	return throttleBucket{tokens: refilled, at: now}
}

// sweep 清空长期不活跃的桶，防止 map 无界增长。阈值之下的 map 保留现状。
func (t *authFailureThrottle) sweepLocked(now time.Time) {
	const maxEntries = 100000
	const idleFor = time.Hour
	if len(t.buckets) < maxEntries {
		return
	}
	for ip, b := range t.buckets {
		if now.Sub(b.at) > idleFor {
			delete(t.buckets, ip)
		}
	}
	if len(t.buckets) >= maxEntries {
		// 极端洪泛：全清比无界增长更安全（代价是旧桶额度重置；rs2 评审已登记
		// 该权衡，与 edge rate-limiter 的同类决策一致）。
		slog.Warn("auth failure throttle map saturated; resetting all buckets")
		t.buckets = map[string]throttleBucket{}
	}
}

// hasCapacity 判定该 IP 是否还有试错余量（不消耗令牌）。
// false → 调用方直接 429，不查库。
func (t *authFailureThrottle) hasCapacity(ip string) bool {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweepLocked(now)
	return t.bucketAt(ip, now).tokens >= 1
}

// record 确认一次 401 后消耗一枚令牌。
func (t *authFailureThrottle) record(ip string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.bucketAt(ip, now)
	if b.tokens < 1 {
		b.tokens = 0
		t.buckets[ip] = b
		return
	}
	b.tokens--
	t.buckets[ip] = b
}

// 使用契约：auth 入口对非-root bearer 先 hasCapacity（桶空 → 429，不查库）；
// 仅在确认为失败尝试（hash 未命中 / apikey 后端未装配）后 record 消耗令牌。
// 合法 key 与 503（后端故障）路径不消耗令牌。

func parseThrottleRate(raw string, def int) int {
	if raw == "" {
		return def
	}
	if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
		return n
	}
	slog.Warn("invalid auth failure throttle rate, keeping default", "value", raw, "default", def)
	return def
}
