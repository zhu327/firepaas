// Package ratelimit 实现 v1.2-E（ADR-0035）的 API 限流：按
// project × route_class（read / mutation / runtime-stream）的令牌桶，
// 桶状态存 Redis（原子 Lua），配置来自 PG（调用方注入 loader，内存短缓存）。
//
// Redis 故障语义由调用方按 class 决策：read fail-open、mutation/stream
// fail-closed（高成本/敏感操作不得在限流失效时绕过配额保护）。
package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Class 是路由分类。
type Class string

const (
	Read     Class = "read"
	Mutation Class = "mutation"
	Stream   Class = "runtime-stream"
)

// Limits 是单个 class 的令牌桶参数（rate = 每秒令牌，burst = 桶容量）。
type Limits struct{ Rate, Burst float64 }

// Config 是一个 project 的三类参数。
type Config struct {
	Read     Limits
	Mutation Limits
	Stream   Limits
}

// DefaultConfig 与 migration 0019 / store.DefaultRateLimitConfig 对齐。
func DefaultConfig() Config {
	return Config{
		Read:     Limits{Rate: 100, Burst: 200},
		Mutation: Limits{Rate: 20, Burst: 40},
		Stream:   Limits{Rate: 5, Burst: 10},
	}
}

func (c Config) limits(class Class) Limits {
	switch class {
	case Mutation:
		return c.Mutation
	case Stream:
		return c.Stream
	default:
		return c.Read
	}
}

// Limiter 是限流器。loader 由调用方注入（PG 读），结果缓存 cacheTTL。
type Limiter struct {
	rdb      *redis.Client
	loader   func(ctx context.Context, project string) (Config, error)
	cacheTTL time.Duration

	mu    sync.RWMutex
	cache map[string]cachedConfig
}

type cachedConfig struct {
	cfg     Config
	expires time.Time
}

// New 构造 Limiter。loader 为 nil 时全部使用默认配置（限流仍生效）。
func New(rdb *redis.Client, loader func(ctx context.Context, project string) (Config, error), cacheTTL time.Duration) *Limiter {
	if cacheTTL <= 0 {
		cacheTTL = 10 * time.Second
	}
	return &Limiter{rdb: rdb, loader: loader, cacheTTL: cacheTTL, cache: map[string]cachedConfig{}}
}

// SetConfig 更新内存缓存（admin 修改限流配置后立即生效）。
func (l *Limiter) SetConfig(project string, cfg Config) {
	l.mu.Lock()
	l.cache[project] = cachedConfig{cfg: cfg, expires: time.Now().Add(l.cacheTTL)}
	l.mu.Unlock()
}

func (l *Limiter) config(ctx context.Context, project string) Config {
	l.mu.RLock()
	c, ok := l.cache[project]
	l.mu.RUnlock()
	if ok && time.Now().Before(c.expires) {
		return c.cfg
	}
	cfg := DefaultConfig()
	if l.loader != nil {
		if loaded, err := l.loader(ctx, project); err == nil {
			cfg = loaded
		} else {
			// PG 故障时回退默认值（fail-open 语义）；观测面靠日志。
			slog.Warn("ratelimit config load failed, using defaults", "project", project, "error", err)
		}
	}
	l.mu.Lock()
	l.cache[project] = cachedConfig{cfg: cfg, expires: time.Now().Add(l.cacheTTL)}
	l.mu.Unlock()
	return cfg
}

// tokenBucket 原子取一个令牌；返回需等待的毫秒数（0 = 放行）。
var tokenBucket = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local b = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(b[1])
if tokens == nil then tokens = burst end
local ts = tonumber(b[2])
if ts == nil then ts = now end
tokens = math.min(burst, tokens + (now - ts) / 1000 * rate)
if tokens < 1 then
  redis.call('HSET', key, 'tokens', tokens, 'ts', now)
  redis.call('PEXPIRE', key, ttl)
  return math.ceil((1 - tokens) / rate * 1000)
end
redis.call('HSET', key, 'tokens', tokens - 1, 'ts', now)
redis.call('PEXPIRE', key, ttl)
return 0`)

// Allow 取一个令牌。ok=false 时 retryAfter 是建议等待时长。
// err 非 nil = Redis 不可用（调用方按 class fail-open/closed）。
func (l *Limiter) Allow(ctx context.Context, project string, class Class) (ok bool, retryAfter time.Duration, err error) {
	lim := l.config(ctx, project).limits(class)
	if lim.Rate <= 0 || lim.Burst <= 0 {
		return true, 0, nil // 显式 0 = 该维度不限
	}
	key := fmt.Sprintf("rl:%s:%s", project, class)
	waitMS, err := tokenBucket.Run(ctx, l.rdb,
		[]string{key}, lim.Rate, lim.Burst,
		time.Now().UnixMilli(), (time.Duration(lim.Burst/lim.Rate)*time.Second + time.Minute).Milliseconds()).Int()
	if err != nil {
		return false, 0, fmt.Errorf("ratelimit %s %s: %w", project, class, err)
	}
	if waitMS > 0 {
		return false, time.Duration(waitMS) * time.Millisecond, nil
	}
	return true, 0, nil
}

// sessionAcquire 是按 project 共享的原子租约信号量。过期成员先清理；租约
// 自带 TTL，因此 API 崩溃不会永久泄漏名额。
var sessionAcquire = redis.NewScript(`
local key = KEYS[1]
local token = ARGV[1]
local now = tonumber(ARGV[2])
local expires = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) >= limit then
  return 0
end
redis.call('ZADD', key, expires, token)
redis.call('PEXPIRE', key, expires - now)
return 1`)

var sessionRenew = redis.NewScript(`
local key = KEYS[1]
local token = ARGV[1]
local expires = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
if redis.call('ZSCORE', key, token) == false then return 0 end
redis.call('ZADD', key, expires, token)
redis.call('PEXPIRE', key, ttl)
return 1`)

var sessionRelease = redis.NewScript(`return redis.call('ZREM', KEYS[1], ARGV[1])`)

// AcquireSession 获取跨 API 实例共享的 runtime-session 名额。Redis 错误
// 原样返回，由 runtime 路径 fail closed。release 会停止续租并删除 lease。
func (l *Limiter) AcquireSession(ctx context.Context, project string, limit int64, ttl time.Duration, onLostOpt ...func()) (release func(), active int64, err error) {
	var onLost func()
	if len(onLostOpt) > 0 {
		onLost = onLostOpt[0]
	}
	if l == nil || l.rdb == nil {
		return nil, 0, fmt.Errorf("runtime session limiter unavailable")
	}
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	if limit <= 0 {
		return nil, 0, fmt.Errorf("runtime session limit must be positive")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, 0, fmt.Errorf("runtime session lease token: %w", err)
	}
	token := hex.EncodeToString(id[:])
	key := "runtime-sessions:" + project
	now := time.Now().UnixMilli()
	ok, err := sessionAcquire.Run(ctx, l.rdb, []string{key}, token, now, now+ttl.Milliseconds(), limit).Int()
	if err != nil {
		return nil, 0, fmt.Errorf("runtime session acquire %s: %w", project, err)
	}
	active, countErr := l.rdb.ZCard(ctx, key).Result()
	if countErr != nil {
		_ = sessionRelease.Run(context.Background(), l.rdb, []string{key}, token).Err()
		return nil, 0, fmt.Errorf("runtime session count %s: %w", project, countErr)
	}
	if ok == 0 {
		return nil, active, nil
	}
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				renewCtx, cancel := context.WithTimeout(context.Background(), ttl/3)
				renewed, renewErr := sessionRenew.Run(renewCtx, l.rdb, []string{key}, token,
					now.Add(ttl).UnixMilli(), ttl.Milliseconds()).Int()
				cancel()
				if renewErr != nil || renewed == 0 {
					if onLost != nil {
						onLost()
					}
					return
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(stop)
			releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = sessionRelease.Run(releaseCtx, l.rdb, []string{key}, token).Err()
		})
	}, active, nil
}
