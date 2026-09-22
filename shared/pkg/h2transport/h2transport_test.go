package h2transport

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestIsGRPC 覆盖内容类型判定（含变体/参数/大小写/缺头）。
func TestIsGRPC(t *testing.T) {
	cases := []struct {
		ct   string
		want bool
	}{
		{"application/grpc", true},
		{"application/grpc+proto", true},
		{"application/grpc-web", true},
		{"application/grpc-web-text", true},
		{"application/grpc; charset=utf-8", true},
		{"Application/GRPC", true},
		{"application/json", false},
		{"text/event-stream", false},
		{"", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "http://x/", nil)
		if c.ct != "" {
			r.Header.Set("Content-Type", c.ct)
		}
		if got := IsGRPC(r); got != c.want {
			t.Errorf("IsGRPC(%q)=%v want %v", c.ct, got, c.want)
		}
	}
}

// TestRoundTripSplitsGRPCToH2C：gRPC 明文走 h2c（后端看到 HTTP/2），
// 非 gRPC 走 base（后端看到 HTTP/1.1），https gRPC 走 base（ALPN 路径）。
func TestRoundTripSplitsGRPCToH2C(t *testing.T) {
	var gotProto int
	var gotPath string
	// 标准库原生 h2c 服务端（同端口双协议；见 ServerProtocols）。
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProto = r.ProtoMajor
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	backend.Config.Protocols = ServerProtocols(nil)
	backend.Start()
	defer backend.Close()

	rt := New(nil)

	// 1) gRPC 明文 → h2c，后端必须看到 HTTP/2。
	req := httptest.NewRequest(http.MethodPost, backend.URL+"/grpc.Service/Method", strings.NewReader("payload"))
	req.RequestURI = ""
	req.URL.Scheme = "http"
	req.URL.Host = backend.Listener.Addr().String()
	req.Header.Set("Content-Type", "application/grpc")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("grpc roundtrip: %v", err)
	}
	_ = resp.Body.Close()
	if gotProto != 2 {
		t.Fatalf("grpc backend proto=%d want 2 (h2c)", gotProto)
	}
	if gotPath != "/grpc.Service/Method" {
		t.Fatalf("grpc path=%q", gotPath)
	}

	// 2) 非 gRPC 明文 → base（HTTP/1.1），同一 h2c 后端仍可服务（h2c 兼容 HTTP/1.1）。
	req2 := httptest.NewRequest(http.MethodGet, backend.URL+"/health", nil)
	req2.RequestURI = ""
	req2.URL.Scheme = "http"
	req2.URL.Host = backend.Listener.Addr().String()
	resp2, err := rt.RoundTrip(req2)
	if err != nil {
		t.Fatalf("plain roundtrip: %v", err)
	}
	_ = resp2.Body.Close()
	if gotProto != 1 {
		t.Fatalf("plain backend proto=%d want 1 (base HTTP/1.1)", gotProto)
	}
}

// TestServerProtocols：明文返回 HTTP/1+h2c 配置；TLS 返回 nil（ALPN 默认，
// 不收窄——收窄会关掉 TLS 上的 H2）。
func TestServerProtocols(t *testing.T) {
	if got := ServerProtocols(nil); got == nil || !got.HTTP1() || !got.UnencryptedHTTP2() {
		t.Fatalf("cleartext protocols = %v, want HTTP/1+h2c", got)
	}
	if got := ServerProtocols(&tls.Config{}); got != nil {
		t.Fatalf("TLS protocols = %v, want nil (ALPN default)", got)
	}
}
