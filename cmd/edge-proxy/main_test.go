package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/firepaas/internal/controlplane/catalog"
)

func backend(id string, port int) catalog.Backend {
	return catalog.Backend{MachineID: id, ExecutionID: "e-" + id, AppPort: port, Readiness: "READY"}
}

// v1.1（ADR-0020）：least-inflight 选择——busy 实例靠后；同分随机打散；
// 最闲者仍 ≥ hard 上限时受控拒绝。
func TestSelectLeastInflight(t *testing.T) {
	ed := newEdge(nil, nil, nil, nil, &counters{}, nil, 8, map[int]bool{8081: true})
	eligible := []catalog.Backend{backend("a", 80), backend("b", 80), backend("c", 80)}

	// 全部空闲：随机打散，任何选择都合法。
	got, over := ed.selectLeastInflight(eligible)
	if over || (got.MachineID != "a" && got.MachineID != "b" && got.MachineID != "c") {
		t.Fatalf("idle selection: %+v over=%v", got, over)
	}
	ed.inflight.release(got.MachineID)

	// a 有 2 个在途：必选更闲的 b/c。
	ed.inflight.acquire("a")
	ed.inflight.acquire("a")
	got, over = ed.selectLeastInflight(eligible)
	if over || got.MachineID == "a" {
		t.Fatalf("busy instance must be deprioritized: %+v", got)
	}

	// 全部超过 hard（8）：受控拒绝。
	for i := 0; i < 9; i++ {
		ed.inflight.acquire("a")
		ed.inflight.acquire("b")
		ed.inflight.acquire("c")
	}
	if _, over := ed.selectLeastInflight(eligible); !over {
		t.Fatal("all backends over hard limit must be rejected")
	}
}

// v1.1（ADR-0020）：inflight 生命周期——release 后归零；snapshot 剔除
// 长期空闲条目（防泄漏）。
// Selection and reservation are one critical section: exactly hard callers may
// reserve the only backend, and every later caller must be rejected.
func TestSelectAndAcquireHonorsHardLimitAtomically(t *testing.T) {
	tr := newInflightTracker()
	eligible := []catalog.Backend{backend("m1", 80)}
	const hard = int64(8)
	const callers = 64
	results := make(chan bool, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, over := tr.selectAndAcquire(eligible, hard)
			results <- !over
		}()
	}
	successes := 0
	for i := 0; i < callers; i++ {
		if <-results {
			successes++
		}
	}
	if successes != int(hard) {
		t.Fatalf("reserved %d slots, want exactly %d", successes, hard)
	}
	if got := tr.load("m1"); got != hard {
		t.Fatalf("inflight = %d, want %d", got, hard)
	}
}

func TestInflightTrackerLifecycle(t *testing.T) {
	tr := newInflightTracker()
	tr.acquire("m1")
	tr.acquire("m1")
	if got := tr.load("m1"); got != 2 {
		t.Fatalf("load = %d, want 2", got)
	}
	tr.release("m1")
	tr.release("m1")
	if got := tr.load("m1"); got != 0 {
		t.Fatalf("load after release = %d, want 0", got)
	}
	snap := tr.snapshot()
	if len(snap) != 0 {
		t.Fatalf("zero-inflight entry must not appear in snapshot: %v", snap)
	}
	// Cleanup happens when the request finishes, independent of metrics scraping.
	if tr.entries["m1"] != nil {
		t.Fatal("zero-inflight entry must be removed on release")
	}
}

// v1.1（ADR-0022）：附加端口解析。
func TestParseExtraPorts(t *testing.T) {
	if got, _ := parseExtraPorts(""); len(got) != 0 {
		t.Fatalf("empty spec: %v", got)
	}
	got, err := parseExtraPorts("8081, 9000-9002")
	if err != nil || len(got) != 4 {
		t.Fatalf("parse: %v %v", got, err)
	}
	if got[0] != 8081 || got[1] != 9000 || got[3] != 9002 {
		t.Fatalf("unexpected ports: %v", got)
	}
	if _, err := parseExtraPorts("70000"); err == nil {
		t.Fatal("out-of-range port must be rejected")
	}
	if _, err := parseExtraPorts("9002-9000"); err == nil {
		t.Fatal("inverted range must be rejected")
	}
}

// v1.1（ADR-0020）：请求端口判定——Host 显式端口（非主监听端口）按端口
// 查路由；主监听端口/无端口 = 主 service。
func TestRequestRoutePort(t *testing.T) {
	// edge 自身监听：主明文 8081 + TLS 8447；app service 端口走 Host 显式声明。
	ed := newEdge(nil, nil, nil, nil, &counters{}, nil, 8, map[int]bool{8081: true, 8447: true})
	mkReq := func(host string) *http.Request {
		return httptest.NewRequest("GET", "http://"+host+"/", nil)
	}
	if got := ed.requestRoutePort(mkReq("app.test")); got != 0 {
		t.Fatalf("no port: %d", got)
	}
	if got := ed.requestRoutePort(mkReq("app.test:8081")); got != 0 {
		t.Fatalf("main listener port is edge addressing: %d", got)
	}
	if got := ed.requestRoutePort(mkReq("app.test:8447")); got != 0 {
		t.Fatalf("TLS listener port is edge addressing: %d", got)
	}
	if got := ed.requestRoutePort(mkReq("app.test:80")); got != 80 {
		t.Fatalf("explicit app service port: %d", got)
	}
	if got := ed.requestRoutePort(mkReq("app.test:9000")); got != 9000 {
		t.Fatalf("extra service port: %d", got)
	}
}

// v1.1（ADR-0022）：缓存键按 (hostname, port) 分离。
func TestRouteCacheKeySeparatesPorts(t *testing.T) {
	if routeCacheKey("h", 0) == routeCacheKey("h", 8081) {
		t.Fatal("primary and extra port keys must differ")
	}
	if routeCacheKey("h", 8081) != "h|8081" {
		t.Fatalf("key format: %s", routeCacheKey("h", 8081))
	}
}

func TestModifyResponseRetriesOnlyAgentMarked502AndStripsMarker(t *testing.T) {
	ed := newEdge(nil, nil, nil, nil, &counters{}, nil, 8, nil)
	request := httptest.NewRequest(http.MethodGet, "http://app.test/", nil)
	request = request.WithContext(context.WithValue(request.Context(), backendKey{}, backend("m1", 80)))

	t.Run("agent marked routing failure is suppressed for retry", func(t *testing.T) {
		resp := &http.Response{StatusCode: http.StatusBadGateway, Request: request,
			Header: http.Header{headerProxyRetryable: []string{retryableProxyValue}}, Body: io.NopCloser(strings.NewReader("gone"))}
		if err := ed.proxy.ModifyResponse(resp); !errors.Is(err, errRetryProxyRoute) {
			t.Fatalf("ModifyResponse error = %v, want retry marker", err)
		}
		if got := resp.Header.Get(headerProxyRetryable); got != "" {
			t.Fatalf("internal marker leaked: %q", got)
		}
		if got := resp.Header.Get(headerMachineID); got != "m1" {
			t.Fatalf("machine header = %q, want m1", got)
		}
	})

	t.Run("unmarked workload 502 is terminal", func(t *testing.T) {
		resp := &http.Response{StatusCode: http.StatusBadGateway, Request: request,
			Header: make(http.Header), Body: io.NopCloser(strings.NewReader("workload failed"))}
		if err := ed.proxy.ModifyResponse(resp); err != nil {
			t.Fatalf("unmarked workload 502 must not retry: %v", err)
		}
	})

	t.Run("marker on another status is stripped but ignored", func(t *testing.T) {
		resp := &http.Response{StatusCode: http.StatusInternalServerError, Request: request,
			Header: http.Header{headerProxyRetryable: []string{retryableProxyValue}}, Body: io.NopCloser(strings.NewReader("failure"))}
		if err := ed.proxy.ModifyResponse(resp); err != nil {
			t.Fatalf("non-502 marker must not retry: %v", err)
		}
		if got := resp.Header.Get(headerProxyRetryable); got != "" {
			t.Fatalf("internal marker leaked: %q", got)
		}
	})
}

func TestHandleProxyErrorRetriesInitialBodylessAgentMarked502(t *testing.T) {
	ed := newEdge(nil, nil, nil, nil, &counters{}, nil, 8, nil)
	for name, tc := range map[string]struct {
		body  io.Reader
		retry bool
	}{
		"bodyless initial request is withheld": {retry: true},
		"body request is terminal":             {body: strings.NewReader("payload")},
	} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "http://app.test/", tc.body)
			r = r.WithContext(context.WithValue(r.Context(), transportRetryKey{}, true))
			rr := httptest.NewRecorder()
			ed.handleProxyError(rr, r, errRetryProxyRoute)
			defer retryFlags.Delete(r)
			if tc.retry {
				if rr.Code != http.StatusOK || rr.Body.Len() != 0 {
					t.Fatalf("retryable marker wrote response: code=%d body=%q", rr.Code, rr.Body.String())
				}
				if got, _ := retryFlags.Load(r); got != retryTransport {
					t.Fatalf("retry reason = %v, want transport", got)
				}
				return
			}
			if rr.Code != http.StatusBadGateway {
				t.Fatalf("code = %d, want 502", rr.Code)
			}
		})
	}
}

func TestHandleProxyErrorRetriesOnlyInitialBodylessTransportFailure(t *testing.T) {
	ed := newEdge(nil, nil, nil, nil, &counters{}, nil, 8, nil)
	transportErr := errors.New("dial tcp: connection refused")

	t.Run("initial bodyless request is withheld", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://app.test/", nil)
		r = r.WithContext(context.WithValue(r.Context(), transportRetryKey{}, true))
		rr := httptest.NewRecorder()
		ed.handleProxyError(rr, r, transportErr)
		defer retryFlags.Delete(r)
		if rr.Code != http.StatusOK || rr.Body.Len() != 0 {
			t.Fatalf("retryable transport error wrote response: code=%d body=%q", rr.Code, rr.Body.String())
		}
		if got, _ := retryFlags.Load(r); got != retryTransport {
			t.Fatalf("retry reason = %v, want transport", got)
		}
	})

	t.Run("request with body fails closed", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "http://app.test/", bytes.NewBufferString("payload"))
		r = r.WithContext(context.WithValue(r.Context(), transportRetryKey{}, true))
		rr := httptest.NewRecorder()
		ed.handleProxyError(rr, r, transportErr)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("code = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		if _, ok := retryFlags.Load(r); ok {
			retryFlags.Delete(r)
			t.Fatal("body request must not be marked retryable")
		}
	})

	t.Run("second transport error remains final", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "http://app.test/", nil)
		rr := httptest.NewRecorder()
		ed.handleProxyError(rr, r, transportErr)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("code = %d, want %d", rr.Code, http.StatusBadGateway)
		}
		if _, ok := retryFlags.Load(r); ok {
			retryFlags.Delete(r)
			t.Fatal("second attempt must not be marked retryable")
		}
	})
}
