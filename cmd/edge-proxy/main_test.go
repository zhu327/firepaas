package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	edgesvc "github.com/zhu327/firepaas/internal/edge"
)

func TestParseExtraPorts(t *testing.T) {
	if got, _ := parseExtraPorts(""); len(got) != 0 {
		t.Fatalf("empty spec: %v", got)
	}
	got, err := parseExtraPorts("8081, 9000-9002")
	if err != nil || len(got) != 4 || got[0] != 8081 || got[3] != 9002 {
		t.Fatalf("parse: %v %v", got, err)
	}
	if _, err := parseExtraPorts("70000"); err == nil {
		t.Fatal("out-of-range port must be rejected")
	}
	if _, err := parseExtraPorts("9002-9000"); err == nil {
		t.Fatal("inverted range must be rejected")
	}
}

// P0#2：edge→agent mTLS 材料缺失时必须启动失败（fail-closed），
// 不得静默退化为明文 HTTP。
func TestLoadAgentTLSFailClosed(t *testing.T) {
	t.Setenv("FIREPAAS_EDGE_TLS_CERT", "")
	t.Setenv("FIREPAAS_EDGE_TLS_KEY", "")
	t.Setenv("FIREPAAS_EDGE_TLS_CA", "")
	t.Setenv("FIREPAAS_EDGE_ALLOW_INSECURE_DEV", "")
	if cfg, mgr, err := loadAgentTLS(0, newCertExpiryGauges()); err == nil || cfg != nil || mgr != nil {
		t.Fatalf("missing materials must fail closed: cfg=%v mgr=%v err=%v", cfg, mgr, err)
	}

	// 显式开发模式是唯一例外（契约 C-2）：返回 nil 配置（明文）但无错误。
	t.Setenv("FIREPAAS_EDGE_ALLOW_INSECURE_DEV", "true")
	cfg, mgr, err := loadAgentTLS(0, newCertExpiryGauges())
	if err != nil || cfg != nil || mgr != nil {
		t.Fatalf("dev mode must allow plaintext: cfg=%v mgr=%v err=%v", cfg, mgr, err)
	}

	// 部分设置 = 配置错误，dev 开关不得掩盖。
	t.Setenv("FIREPAAS_EDGE_TLS_CERT", "/nonexistent/cert.pem")
	if _, _, err := loadAgentTLS(0, newCertExpiryGauges()); err == nil ||
		!strings.Contains(err.Error(), "must be set together") {
		t.Fatalf("partial materials: err=%v", err)
	}
}

func TestCertExpiryGaugesWritePrometheus(t *testing.T) {
	g := newCertExpiryGauges()
	expiry := time.Unix(1_800_000_000, 0)
	g.set("/certs/edge.crt", expiry)
	var sb strings.Builder
	g.WritePrometheus(&sb)
	want := "firepaas_tls_cert_not_after_seconds{file=\"/certs/edge.crt\"} 1800000000"
	if !strings.Contains(sb.String(), want) {
		t.Fatalf("missing gauge %q in:\n%s", want, sb.String())
	}
}

func TestListenerPorts(t *testing.T) {
	got := listenerPorts("8080", ":8443", []int{9000})
	for _, port := range []int{8080, 8443, 9000} {
		if !got[port] {
			t.Fatalf("port %d missing: %v", port, got)
		}
	}
}

// W1.1：metrics Bearer 鉴权（FIREPAAS_EDGE_METRICS_TOKEN 非空时启用）。
func TestEdgeMetricsAuthorized(t *testing.T) {
	withAuth := func(v string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if v != "" {
			r.Header.Set("Authorization", v)
		}
		return r
	}
	if !edgeMetricsAuthorized(withAuth("Bearer s3cret"), "s3cret") {
		t.Fatal("correct bearer token must be authorized")
	}
	for name, v := range map[string]string{
		"missing header": "",
		"wrong token":    "Bearer wrong",
		"wrong scheme":   "Token s3cret",
		"empty bearer":   "Bearer ",
	} {
		if edgeMetricsAuthorized(withAuth(v), "s3cret") {
			t.Fatalf("%s must be rejected", name)
		}
	}
}

// W1.1：FIREPAAS_EDGE_METRICS_LABEL_MACHINE 默认 1（保持兼容），=0 关闭。
func TestEdgeMetricsLabelMachine(t *testing.T) {
	if !edgeMetricsLabelMachine() {
		t.Fatal("unset must default to per-machine labels (compat)")
	}
	t.Setenv("FIREPAAS_EDGE_METRICS_LABEL_MACHINE", "1")
	if !edgeMetricsLabelMachine() {
		t.Fatal("=1 must keep per-machine labels")
	}
	t.Setenv("FIREPAAS_EDGE_METRICS_LABEL_MACHINE", "0")
	if edgeMetricsLabelMachine() {
		t.Fatal("=0 must disable per-machine labels")
	}
}

// W1.1：LABEL_MACHINE=0 时输出无标签总量；无数据时不输出（与原行为一致）。
func TestWriteInflightAggregatedEmpty(t *testing.T) {
	h := newTestAggregateHandler()
	var sb strings.Builder
	writeInflightAggregated(&sb, h)
	if sb.Len() != 0 {
		t.Fatalf("empty inflight must produce no output, got %q", sb.String())
	}
}

func TestVersionedHealthz(t *testing.T) {
	called := false
	h := withVersionedHealthz(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status=%d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body not JSON: %v (%q)", err, rec.Body.String())
	}
	if body["status"] != "ok" || body["version"] != version {
		t.Fatalf("healthz body=%v", body)
	}
	if called {
		t.Fatal("healthz must not reach the data-plane handler")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("non-healthz path must delegate to the data-plane handler")
	}
}

// newTestAggregateHandler 构造空 inflight 的 Handler（聚合输出测试用）。
func newTestAggregateHandler() *edgesvc.Handler {
	return edgesvc.NewHandler(edgesvc.Config{})
}

// G710：HTTP→HTTPS 跳转沿用请求 Host 时必须拒绝可改变 URL 结构的 Host；
// 合法 Host 保持原跳转行为（含非 443 端口后缀与 RequestURI 透传）。
func TestRedirectHandlerHostValidation(t *testing.T) {
	serve := func(h http.Handler, host, target string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	h := redirectHandler(":443")
	rec := serve(h, "app.example.com", "/a?b=1")
	if rec.Code != http.StatusPermanentRedirect {
		t.Fatalf("valid host status=%d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "https://app.example.com/a?b=1" {
		t.Fatalf("valid host location=%q", loc)
	}

	rec = serve(h, "app.example.com:8080", "/a")
	if loc := rec.Header().Get("Location"); loc != "https://app.example.com/a" {
		t.Fatalf("host with port location=%q", loc)
	}

	h8443 := redirectHandler(":8443")
	rec = serve(h8443, "app.example.com", "/")
	if loc := rec.Header().Get("Location"); loc != "https://app.example.com:8443/" {
		t.Fatalf("non-443 suffix location=%q", loc)
	}

	for _, bad := range []string{"", "evil.com@good.com", "a/b", `a\b`, "a?b", "a#b", "a b", "a\tb"} {
		if rec := serve(h, bad, "/"); rec.Code != http.StatusBadRequest {
			t.Fatalf("host %q must be rejected with 400, got %d", bad, rec.Code)
		}
	}

	if rec := serve(h, "app.example.com", "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("healthz status=%d", rec.Code)
	}
}
