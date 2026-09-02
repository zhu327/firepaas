package agentclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/security/mtls"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// 本文件测试依赖进程级状态（env、NotAfterHook），不得 parallel。

var agentEnvKeys = []string{
	"FIREPAAS_AGENT_TLS_CERT",
	"FIREPAAS_AGENT_TLS_KEY",
	"FIREPAAS_AGENT_TLS_CA",
	"FIREPAAS_AGENT_TLS_ALLOW_INSECURE",
	"FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL",
}

// clearAgentEnv 固定全部相关 env 为空，隔离测试进程的外部环境。
func clearAgentEnv(t *testing.T) {
	t.Helper()
	for _, k := range agentEnvKeys {
		t.Setenv(k, "")
	}
}

func setNotAfterHook(t *testing.T, hook func(time.Time)) {
	t.Helper()
	old := NotAfterHook
	NotAfterHook = hook
	t.Cleanup(func() { NotAfterHook = old })
}

// fakeInfoServer 返回固定响应；mTLS 会话下回传客户端证书 CN（观测 mTLS
// 客户端确已出示证书，而非静默走明文）。
type fakeInfoServer struct {
	pb.UnimplementedInfoServiceServer
	nodeID string
}

func (f *fakeInfoServer) ServiceInfo(ctx context.Context, _ *emptypb.Empty) (*pb.ServiceInfoResponse, error) {
	resp := &pb.ServiceInfoResponse{NodeId: f.nodeID}
	if cn, err := mtls.PeerCN(ctx); err == nil {
		resp.Labels = map[string]string{"peer_cn": cn}
	}
	return resp, nil
}

// startFakeAgent 在 127.0.0.1:0 起 Info gRPC server（repo 既有 fake server 模式）。
func startFakeAgent(t *testing.T, opts ...grpc.ServerOption) (string, *fakeInfoServer) {
	t.Helper()
	fake := &fakeInfoServer{nodeID: "node-test"}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(opts...)
	pb.RegisterInfoServiceServer(gs, fake)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String(), fake
}

// (a)+(d)：insecure dev env 下 Dial 连通并完成 Info RPC 往返；(e) nil
// certMgr 时 Close 安全。
func TestDialInsecureRoundTrip(t *testing.T) {
	clearAgentEnv(t)
	t.Setenv("FIREPAAS_AGENT_TLS_ALLOW_INSECURE", "true")
	addr, _ := startFakeAgent(t)

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if c.Addr() != addr {
		t.Fatalf("Addr=%q want %q", c.Addr(), addr)
	}
	if c.RawConn() == nil || c.Machines == nil || c.Images == nil {
		t.Fatal("conn and service clients must be wired")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.ServiceInfo(ctx)
	if err != nil {
		t.Fatalf("ServiceInfo: %v", err)
	}
	if resp.NodeId != "node-test" {
		t.Fatalf("NodeId=%q", resp.NodeId)
	}
	if _, ok := resp.Labels["peer_cn"]; ok {
		t.Fatal("insecure session must not present a client certificate")
	}
	if c.certMgr != nil {
		t.Fatal("insecure mode must not create a CertManager")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// (b) 失联 agent：grpc.NewClient 惰性连接，RPC 必须返回错误而不是 panic/
// 永久挂起。
func TestServiceInfoUnreachableAgent(t *testing.T) {
	clearAgentEnv(t)
	t.Setenv("FIREPAAS_AGENT_TLS_ALLOW_INSECURE", "true")
	// 占一个端口立即关闭，得到确定不可达的地址。
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatal(err)
	}

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial must be lazy: %v", err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.ServiceInfo(ctx); status.Code(err) != codes.Unavailable {
		t.Fatalf("ServiceInfo err=%v want Unavailable", err)
	}
	if _, err := c.List(ctx, "p1"); status.Code(err) != codes.Unavailable {
		t.Fatalf("List err=%v want Unavailable", err)
	}
}

// (c) fail-closed：没有 mTLS 材料且未显式允许 insecure → Dial 报错并指明
// 需要证书 env，不得静默降级为明文。
func TestDialFailClosedWithoutTLSMaterials(t *testing.T) {
	t.Run("no materials at all", func(t *testing.T) {
		clearAgentEnv(t)
		if _, err := Dial("127.0.0.1:1"); err == nil {
			t.Fatal("Dial must fail closed without mTLS materials")
		} else if !strings.Contains(err.Error(), "FIREPAAS_AGENT_TLS_CERT") {
			t.Fatalf("error must name required cert env: %v", err)
		}
	})
	t.Run("partial materials still fail", func(t *testing.T) {
		clearAgentEnv(t)
		t.Setenv("FIREPAAS_AGENT_TLS_CERT", "/nonexistent/tls.crt")
		if _, err := Dial("127.0.0.1:1"); err == nil {
			t.Fatal("partial cert env must fail closed")
		} else if !strings.Contains(err.Error(), "FIREPAAS_AGENT_TLS_CERT") {
			t.Fatalf("error must name required cert env: %v", err)
		}
	})
}

// (c) 三个 env 都给了但证书文件不存在 → fail-closed，错误指向证书加载。
func TestDialMissingCertFilesFails(t *testing.T) {
	clearAgentEnv(t)
	dir := t.TempDir()
	t.Setenv("FIREPAAS_AGENT_TLS_CERT", filepath.Join(dir, "tls.crt"))
	t.Setenv("FIREPAAS_AGENT_TLS_KEY", filepath.Join(dir, "tls.key"))
	t.Setenv("FIREPAAS_AGENT_TLS_CA", filepath.Join(dir, "ca.pem"))
	if _, err := Dial("127.0.0.1:1"); err == nil {
		t.Fatal("missing cert files must fail closed")
	} else if !strings.Contains(err.Error(), "agent mTLS") {
		t.Fatalf("error must point at mTLS material: %v", err)
	}
}

// newTestCA 与 writeSignedCert 在 dir 下生成 throwaway CA 和由它签发的
// 证书（仅测试使用，对齐 internal/security/mtls 测试模式）。
func newTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "firepaas-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key, der
}

func writeSignedCert(
	t *testing.T,
	dir, name, cn string,
	ca *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	notAfter time.Time,
	server bool,
) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	usage := x509.ExtKeyUsageClientAuth
	if server {
		usage = x509.ExtKeyUsageServerAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     []string{cn}, // CN 不再用于 SAN 校验，ServerName 需这里覆盖。
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// (f) mTLS 全链路：throwaway CA + 双方证书，require-and-verify-client-cert
// server；验证 RPC 往返、服务端看到客户端 CN、NotAfterHook 以客户端证书
// 到期时间触发一次。
func TestDialMTLSRoundTripWithThrowawayCerts(t *testing.T) {
	clearAgentEnv(t)
	setNotAfterHook(t, nil)
	dir := t.TempDir()
	ca, caKey, caDER := newTestCA(t)
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey := writeSignedCert(t, dir, "agentd", "agentd", ca, caKey, time.Now().Add(24*time.Hour), true)
	clientExpiry := time.Now().Add(12 * time.Hour).Truncate(time.Second)
	clientCert, clientKey := writeSignedCert(t, dir, "control-plane", "control-plane", ca, caKey, clientExpiry, false)

	tlsConf, err := mtls.ServerConfig(serverCert, serverKey, caFile)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := startFakeAgent(t, grpc.Creds(credentials.NewTLS(tlsConf)))

	t.Setenv("FIREPAAS_AGENT_TLS_CERT", clientCert)
	t.Setenv("FIREPAAS_AGENT_TLS_KEY", clientKey)
	t.Setenv("FIREPAAS_AGENT_TLS_CA", caFile)
	t.Setenv("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL", "0")

	var mu sync.Mutex
	var hookExpiries []time.Time
	setNotAfterHook(t, func(expiry time.Time) {
		mu.Lock()
		defer mu.Unlock()
		hookExpiries = append(hookExpiries, expiry)
	})

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if c.certMgr == nil {
		t.Fatal("mTLS mode must own a CertManager for hot reload")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.ServiceInfo(ctx)
	if err != nil {
		t.Fatalf("ServiceInfo over mTLS: %v", err)
	}
	if resp.Labels["peer_cn"] != "control-plane" {
		t.Fatalf("server must observe client cert CN control-plane, labels=%v", resp.Labels)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(hookExpiries) != 1 {
		t.Fatalf("NotAfterHook must fire exactly once on initial load, got %d", len(hookExpiries))
	}
	if !hookExpiries[0].Equal(clientExpiry) {
		t.Fatalf("hook expiry=%v want client cert expiry %v", hookExpiries[0], clientExpiry)
	}
}

// 热重载周期 env：默认 1m、合法值透传、非法值回退默认。
func TestCertReloadIntervalEnv(t *testing.T) {
	clearAgentEnv(t)
	if got := certReloadInterval(); got != time.Minute {
		t.Fatalf("default=%v want 1m", got)
	}
	t.Setenv("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL", "30s")
	if got := certReloadInterval(); got != 30*time.Second {
		t.Fatalf("30s parse=%v", got)
	}
	t.Setenv("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL", "0")
	if got := certReloadInterval(); got != 0 {
		t.Fatalf("zero disables reload, got %v", got)
	}
	t.Setenv("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL", "not-a-duration")
	if got := certReloadInterval(); got != time.Minute {
		t.Fatalf("invalid value must fall back to default, got %v", got)
	}
}

// (e) Close 与 certMgr：mTLS 客户端 Close 必须同时停掉热重载 goroutine，
// 反复调用不挂死（stopOnce + conn.Close 幂等语义由 grpc 保证首调用）。
func TestCloseStopsCertManagerReload(t *testing.T) {
	clearAgentEnv(t)
	dir := t.TempDir()
	ca, caKey, caDER := newTestCA(t)
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	clientCert, clientKey := writeSignedCert(
		t,
		dir,
		"control-plane",
		"control-plane",
		ca,
		caKey,
		time.Now().Add(time.Hour),
		false,
	)
	t.Setenv("FIREPAAS_AGENT_TLS_CERT", clientCert)
	t.Setenv("FIREPAAS_AGENT_TLS_KEY", clientKey)
	t.Setenv("FIREPAAS_AGENT_TLS_CA", caFile)
	t.Setenv("FIREPAAS_AGENT_TLS_CERT_RELOAD_INTERVAL", "10ms")

	c, err := Dial("127.0.0.1:1") // 无需真实 server；只验证 Close 生命周期。
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = c.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close must stop the reload goroutine promptly")
	}
}
