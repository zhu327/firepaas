// auththrottle_test.go：401 限流（R2 评审 P1）的行为测试。
package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// 桶语义：burst 内放行、耗尽有阈值、随时间补发、不同 IP 隔离。
func TestAuthFailureThrottleBucket(t *testing.T) {
	th := newAuthFailureThrottle(60) // 1/s，burst 60
	if th == nil {
		t.Fatal("positive rate must construct throttle")
	}
	if !th.hasCapacity("1.1.1.1") {
		t.Fatal("fresh bucket must have capacity")
	}
	// 耗尽 burst。
	for i := 0; i < 60; i++ {
		th.record("1.1.1.1")
	}
	if th.hasCapacity("1.1.1.1") {
		t.Fatal("bucket must be exhausted after burst worth of failures")
	}
	// 其他 IP 不受影响。
	if !th.hasCapacity("2.2.2.2") {
		t.Fatal("other IPs must not be throttled")
	}
	// 负值构造 = 关闭。
	if newAuthFailureThrottle(0) != nil {
		t.Fatal("non-positive rate must disable throttle")
	}
}

// 补发：手动把桶时间戳拨到过去，令牌应随经过时间回补。
func TestAuthFailureThrottleRefills(t *testing.T) {
	th := newAuthFailureThrottle(60)
	th.record("3.3.3.3")
	// 拨回一个 burst 以前：桶自然满。
	th.mu.Lock()
	b := th.buckets["3.3.3.3"]
	b.at = time.Now().Add(-10 * time.Minute)
	th.buckets["3.3.3.3"] = b
	th.mu.Unlock()
	for i := 0; i < 60; i++ {
		th.record("3.3.3.3")
	}
	if th.hasCapacity("3.3.3.3") {
		t.Fatal("refilled bucket must drain again")
	}
}

// auth 集成：429 短路发生在 apikey 后端查询之前（hash only）。
// 桶 rate=2/min → burst=2；第三次从同一 IP 的失败尝试必须 429。
func TestAuthInvalidKeyThrottled429(t *testing.T) {
	a := &API{apiToken: "root-token", authThrottle: newAuthFailureThrottle(2)}
	// apiKeys == nil：每一个非 root bearer 都是确定的 401（不查库）。
	next := a.auth(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })

	call := func() int {
		req := httptest.NewRequest("GET", "/v1/machines", nil)
		req.Pattern = "GET /v1/machines"
		req.RemoteAddr = "203.0.113.7:12345"
		req.Header.Set("Authorization", "Bearer fp_guess")
		rec := httptest.NewRecorder()
		next.ServeHTTP(rec, req)
		return rec.Code
	}

	if got := call(); got != 401 {
		t.Fatalf("first attempt status = %d, want 401", got)
	}
	if got := call(); got != 401 {
		t.Fatalf("second attempt status = %d, want 401", got)
	}
	if got := call(); got != 429 {
		t.Fatalf("over-limit attempt status = %d, want 429", got)
	}
	// root token 从同 IP 发起不受失败桶影响（永不消耗令牌）。
	req := httptest.NewRequest("GET", "/v1/machines", nil)
	req.Pattern = "GET /v1/machines"
	req.RemoteAddr = "203.0.113.7:12345"
	req.Header.Set("Authorization", "Bearer root-token")
	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, req)
	if rec.Code != 204 {
		t.Fatalf("root token status = %d, want 204 (throttle must not block valid auth)", rec.Code)
	}
}

// 未带凭据不变行为（401 立即，不计桶——恶意空请求不放大）。
func TestAuthThrottleEnvParsing(t *testing.T) {
	if got := parseThrottleRate("", 20); got != 20 {
		t.Fatalf("empty env → default 20, got %d", got)
	}
	if got := parseThrottleRate("bogus", 20); got != 20 {
		t.Fatalf("invalid env → default 20, got %s", strconv.Itoa(got))
	}
	if got := parseThrottleRate("0", 20); got != 0 {
		t.Fatalf("explicit 0 = disabled, got %d", got)
	}
	if got := parseThrottleRate("120", 20); got != 120 {
		t.Fatalf("120/min parse → 120, got %d", got)
	}
}
