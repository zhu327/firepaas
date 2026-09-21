package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type contextServerStream struct{ ctx context.Context }

func (s contextServerStream) SetHeader(metadata.MD) error  { return nil }
func (s contextServerStream) SendHeader(metadata.MD) error { return nil }
func (s contextServerStream) SetTrailer(metadata.MD)       {}
func (s contextServerStream) Context() context.Context     { return s.ctx }
func (s contextServerStream) SendMsg(any) error            { return nil }
func (s contextServerStream) RecvMsg(any) error            { return nil }

func tlsPeerContext(cn string) context.Context {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
	info := credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
		VerifiedChains:   [][]*x509.Certificate{{cert}},
	}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
}

func TestStreamServerIdentityInterceptor(t *testing.T) {
	interceptor := StreamServerIdentityInterceptor([]string{"control-plane"})
	called := false
	handler := func(any, grpc.ServerStream) error { called = true; return nil }

	err := interceptor(nil, contextServerStream{ctx: tlsPeerContext("edge-proxy")}, nil, handler)
	if status.Code(err) != codes.PermissionDenied || called {
		t.Fatalf("wrong CN: code=%v called=%v", status.Code(err), called)
	}

	err = interceptor(nil, contextServerStream{ctx: tlsPeerContext("control-plane")}, nil, handler)
	if err != nil || !called {
		t.Fatalf("allowed CN: err=%v called=%v", err, called)
	}
}

// W3.1：三处 TLS 配置必须协商 TLS 1.3（回滚方案见 mtls.go ServerConfig 注释）。
func TestTLSConfigsRequireTLS13(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "test", time.Now().Add(time.Hour))
	caFile := certFile // 自签名：自身即 CA 足以做配置级断言

	srv, err := ServerConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if srv.MinVersion != tls.VersionTLS13 {
		t.Fatalf("ServerConfig MinVersion = %x, want TLS 1.3", srv.MinVersion)
	}

	cm, err := NewCertManager(certFile, keyFile, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	srvM, err := ServerConfigWithManager(cm, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if srvM.MinVersion != tls.VersionTLS13 {
		t.Fatalf("ServerConfigWithManager MinVersion = %x, want TLS 1.3", srvM.MinVersion)
	}

	cli, err := ClientConfig(certFile, keyFile, caFile, "agentd")
	if err != nil {
		t.Fatal(err)
	}
	if cli.MinVersion != tls.VersionTLS13 {
		t.Fatalf("ClientConfig MinVersion = %x, want TLS 1.3", cli.MinVersion)
	}

	cliM, err := cm.ClientTLSConfig(caFile, "agentd")
	if err != nil {
		t.Fatal(err)
	}
	if cliM.MinVersion != tls.VersionTLS13 {
		t.Fatalf("CertManager.ClientTLSConfig MinVersion = %x, want TLS 1.3", cliM.MinVersion)
	}
}

// W3.1：客户端 ServerName 为空必须 fail-closed（构造期拒绝）。
func TestClientConfigRejectsEmptyServerName(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "test", time.Now().Add(time.Hour))
	for _, name := range []string{"", "   "} {
		if _, err := ClientConfig(certFile, keyFile, certFile, name); err == nil {
			t.Fatalf("ClientConfig must reject empty server name %q", name)
		}
	}
	cm, err := NewCertManager(certFile, keyFile, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	if _, err := cm.ClientTLSConfig(certFile, ""); err == nil {
		t.Fatal("CertManager.ClientTLSConfig must reject empty server name")
	}
}

// W3.1：unary 拦截器锁定 5108 语义——仅 control-plane 放行，非法 CN 拒绝。
func TestUnaryServerIdentityInterceptor(t *testing.T) {
	interceptor := UnaryServerIdentityInterceptor([]string{"control-plane"})
	called := false
	handler := func(context.Context, any) (any, error) { called = true; return nil, nil }

	if _, err := interceptor(tlsPeerContext("edge-proxy"), nil, nil, handler); status.Code(
		err,
	) != codes.PermissionDenied ||
		called {
		t.Fatalf("wrong CN: code=%v called=%v", status.Code(err), called)
	}
	if _, err := interceptor(context.Background(), nil, nil, handler); status.Code(err) != codes.Unauthenticated ||
		called {
		t.Fatalf("no peer: code=%v called=%v", status.Code(err), called)
	}
	if _, err := interceptor(tlsPeerContext("control-plane"), nil, nil, handler); err != nil || !called {
		t.Fatalf("allowed CN: err=%v called=%v", err, called)
	}
}

func httpPeerRequest(cn string, verified bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://agent/", nil)
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	if verified {
		state.VerifiedChains = [][]*x509.Certificate{{cert}}
	}
	r.TLS = &state
	return r
}

// W3.1：5107 HTTP 侧锁定 edge-proxy 语义——无证/未验证/非法 CN 拒绝。
func TestRequireClientIdentity(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := RequireClientIdentity(next, []string{"edge-proxy"})

	cases := []struct {
		name string
		req  *http.Request
		want int
	}{
		{"no TLS", httptest.NewRequest(http.MethodGet, "http://agent/", nil), http.StatusUnauthorized},
		{"unverified chain", httpPeerRequest("edge-proxy", false), http.StatusUnauthorized},
		{"wrong CN", httpPeerRequest("control-plane", true), http.StatusForbidden},
		{"allowed CN", httpPeerRequest("edge-proxy", true), http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, tc.req)
			if rr.Code != tc.want {
				t.Fatalf("status = %d, want %d", rr.Code, tc.want)
			}
		})
	}
}
