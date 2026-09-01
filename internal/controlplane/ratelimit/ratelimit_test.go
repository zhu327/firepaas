package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testLimiter(t *testing.T) *Limiter {
	t.Helper()
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS to run ratelimit tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, nil, 10*time.Second)
}

// 令牌桶：burst 内全放行，耗尽后拒绝并给出等待时长，静止后恢复。
func TestTokenBucketAllowsBurstThenLimits(t *testing.T) {
	l := testLimiter(t)
	ctx := context.Background()
	_ = l.rdb.Del(ctx, "rl:p-bucket:read").Err()
	l.SetConfig("p-bucket", Config{Read: Limits{Rate: 100, Burst: 3}, Mutation: Limits{Rate: 1, Burst: 1}, Stream: Limits{Rate: 1, Burst: 1}})

	for i := 0; i < 3; i++ {
		ok, _, err := l.Allow(ctx, "p-bucket", Read)
		if err != nil || !ok {
			t.Fatalf("burst token %d must pass: ok=%v err=%v", i, ok, err)
		}
	}
	ok, retry, err := l.Allow(ctx, "p-bucket", Read)
	if err != nil || ok {
		t.Fatalf("4th token must be limited: ok=%v retry=%v err=%v", ok, retry, err)
	}
	if retry <= 0 || retry > time.Second {
		t.Fatalf("retryAfter out of sane range: %v", retry)
	}
	// 不同 class 独立计数。
	ok2, _, err2 := l.Allow(ctx, "p-bucket", Mutation)
	if err2 != nil || !ok2 {
		t.Fatalf("independent class bucket: ok=%v err=%v", ok2, err2)
	}
	// 不同 project 独立计数。
	ok3, _, err3 := l.Allow(ctx, "p-other", Read)
	if err3 != nil || !ok3 {
		t.Fatalf("independent project bucket: ok=%v err=%v", ok3, err3)
	}
}

// 配置显式 0 = 该维度不限流。
func TestTokenBucketExpiryUsesMilliseconds(t *testing.T) {
	l := testLimiter(t)
	ctx := context.Background()
	key := "rl:p-ttl:read"
	_ = l.rdb.Del(ctx, key).Err()
	l.SetConfig("p-ttl", Config{Read: Limits{Rate: 1, Burst: 1}})
	if ok, _, err := l.Allow(ctx, "p-ttl", Read); err != nil || !ok {
		t.Fatalf("allow: ok=%v err=%v", ok, err)
	}
	ttl, err := l.rdb.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < 59*time.Second {
		t.Fatalf("bucket TTL = %v, want milliseconds-scale TTL near 61s", ttl)
	}
}

func TestSessionLeaseSharedAndReleased(t *testing.T) {
	l1 := testLimiter(t)
	l2 := New(l1.rdb, nil, time.Second)
	ctx := context.Background()
	_ = l1.rdb.Del(ctx, "runtime-sessions:p-session").Err()
	release, active, err := l1.AcquireSession(ctx, "p-session", 1, time.Minute)
	if err != nil || release == nil || active != 1 {
		t.Fatalf("first acquire: release=%v active=%d err=%v", release != nil, active, err)
	}
	if release2, active2, err := l2.AcquireSession(ctx, "p-session", 1, time.Minute); err != nil || release2 != nil || active2 != 1 {
		t.Fatalf("shared limit: release=%v active=%d err=%v", release2 != nil, active2, err)
	}
	release()
	if release3, _, err := l2.AcquireSession(ctx, "p-session", 1, time.Minute); err != nil || release3 == nil {
		t.Fatalf("acquire after release: release=%v err=%v", release3 != nil, err)
	} else {
		release3()
	}
}

func TestSessionLeaseLossSignalsCancellation(t *testing.T) {
	l := testLimiter(t)
	ctx := context.Background()
	key := "runtime-sessions:p-session-lost"
	_ = l.rdb.Del(ctx, key).Err()
	lost := make(chan struct{}, 1)
	release, _, err := l.AcquireSession(ctx, "p-session-lost", 1, 90*time.Millisecond, func() {
		select {
		case lost <- struct{}{}:
		default:
		}
	})
	if err != nil || release == nil {
		t.Fatalf("acquire: release=%v err=%v", release != nil, err)
	}
	defer release()
	if err := l.rdb.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("lease member loss did not signal stream cancellation")
	}
}

func TestZeroConfigDisablesClass(t *testing.T) {
	l := testLimiter(t)
	ctx := context.Background()
	_ = l.rdb.Del(ctx, "rl:p-off:read").Err()
	l.SetConfig("p-off", Config{Read: Limits{Rate: 0, Burst: 0}, Mutation: Limits{Rate: 1, Burst: 1}, Stream: Limits{Rate: 1, Burst: 1}})
	for i := 0; i < 100; i++ {
		ok, _, err := l.Allow(ctx, "p-off", Read)
		if err != nil || !ok {
			t.Fatalf("zero-rate class must always allow: i=%d ok=%v err=%v", i, ok, err)
		}
	}
}

// Redis 不可达：Allow 返回 err，由调用方按 class 分级（read fail-open /
// mutation fail-closed）。
func TestRedisFailureSurfacesError(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_REDIS") == "" {
		t.Skip("set FIREPAAS_TEST_REDIS to run ratelimit tests")
	}
	l := New(testLimiter(t).rdb, nil, time.Second)
	// 独立 client 指向不存在的地址。
	bad := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, ReadTimeout: 50 * time.Millisecond, WriteTimeout: 50 * time.Millisecond})
	defer bad.Close()
	l.rdb = bad
	if _, _, err := l.Allow(context.Background(), "p", Read); err == nil {
		t.Fatal("redis failure must surface error")
	}
}
