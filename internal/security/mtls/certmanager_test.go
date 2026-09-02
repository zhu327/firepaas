package mtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// writeTestCert 在 dir 下生成一张自签名 ECDSA 证书（仅测试使用），
// 返回 (certFile, keyFile)。cn/notAfter 用于区分轮换前后的证书。
func writeTestCert(t *testing.T, dir, cn string, notAfter time.Time) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	// 文件名固定，便于轮换（覆盖同一路径）。
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
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

func certCN(t *testing.T, cert *tls.Certificate) string {
	t.Helper()
	if cert == nil || cert.Leaf == nil {
		t.Fatal("nil certificate/leaf")
	}
	return cert.Leaf.Subject.CommonName
}

func TestCertManagerInitialLoadAndHook(t *testing.T) {
	dir := t.TempDir()
	expiry := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	certFile, keyFile := writeTestCert(t, dir, "old", expiry)

	var hookCalls atomic.Int64
	var hookExpiry atomic.Value
	cm, err := NewCertManager(certFile, keyFile, 0, nil, func(exp time.Time) {
		hookCalls.Add(1)
		hookExpiry.Store(exp)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	serverCert, err := cm.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || certCN(t, serverCert) != "old" {
		t.Fatalf("GetCertificate: %v %v", serverCert, err)
	}
	clientCert, err := cm.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil || certCN(t, clientCert) != "old" {
		t.Fatalf("GetClientCertificate: %v %v", clientCert, err)
	}
	if !cm.NotAfter().Equal(expiry) {
		t.Fatalf("NotAfter=%v want %v", cm.NotAfter(), expiry)
	}
	if hookCalls.Load() != 1 || !hookExpiry.Load().(time.Time).Equal(expiry) {
		t.Fatalf("hook must fire once with expiry: calls=%d", hookCalls.Load())
	}
}

func TestCertManagerInvalidInitialLoadFails(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewCertManager(filepath.Join(dir, "nope.crt"), filepath.Join(dir, "nope.key"), 0, nil, nil); err == nil {
		t.Fatal("missing keypair must fail startup (fail-closed)")
	}
}

// 轮换：覆盖同一路径后 reload 原子换证并再次触发 hook。
func TestCertManagerReloadRotatesCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "old", time.Now().Add(24*time.Hour))
	var hookCalls atomic.Int64
	cm, err := NewCertManager(certFile, keyFile, 0, nil, func(time.Time) { hookCalls.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	newExpiry := time.Now().Add(48 * time.Hour).Truncate(time.Second)
	writeTestCert(t, dir, "new", newExpiry)
	if err := cm.reload(); err != nil {
		t.Fatal(err)
	}
	current, err := cm.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || certCN(t, current) != "new" {
		t.Fatalf("after reload: %v %v", certCN(t, current), err)
	}
	if !cm.NotAfter().Equal(newExpiry) {
		t.Fatalf("NotAfter=%v want %v", cm.NotAfter(), newExpiry)
	}
	if hookCalls.Load() != 2 {
		t.Fatalf("hook must fire on initial+reload, calls=%d", hookCalls.Load())
	}
}

// 损坏的新文件：reload 失败保留旧证书，不触发 hook。
func TestCertManagerCorruptReloadKeepsOldCertificate(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "good", time.Now().Add(24*time.Hour))
	var hookCalls atomic.Int64
	cm, err := NewCertManager(certFile, keyFile, 0, nil, func(time.Time) { hookCalls.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	if err := os.WriteFile(certFile, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cm.reload(); err == nil {
		t.Fatal("corrupt keypair must fail reload")
	}
	current, err := cm.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || certCN(t, current) != "good" {
		t.Fatalf("corrupt reload must keep old cert: %v %v", certCN(t, current), err)
	}
	if hookCalls.Load() != 1 {
		t.Fatalf("hook must not fire on failed reload, calls=%d", hookCalls.Load())
	}
}

// 后台 ticker 路径：短周期重载，轮询观察换证（带截止，非裸 sleep 断言）。
func TestCertManagerBackgroundReload(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "v1", time.Now().Add(24*time.Hour))
	cm, err := NewCertManager(certFile, keyFile, 10*time.Millisecond, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeTestCert(t, dir, "v2", time.Now().Add(48*time.Hour))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current, err := cm.GetCertificate(&tls.ClientHelloInfo{})
		if err == nil && certCN(t, current) == "v2" {
			cm.Close()
			cm.Close() // Close 幂等
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cm.Close()
	t.Fatal("background reload did not pick up rotated certificate before deadline")
}

func TestCertManagerClientTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCert(t, dir, "edge", time.Now().Add(24*time.Hour))
	cm, err := NewCertManager(certFile, keyFile, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	caFile := filepath.Join(dir, "ca.pem")
	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := cm.ClientTLSConfig(caFile, "agentd")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "agentd" || cfg.RootCAs == nil || cfg.GetClientCertificate == nil {
		t.Fatalf("client config: %+v", cfg)
	}
	cert, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil || certCN(t, cert) != "edge" {
		t.Fatalf("client cert via config: %v %v", cert, err)
	}
	if _, err := cm.ClientTLSConfig(filepath.Join(dir, "missing-ca.pem"), "agentd"); err == nil {
		t.Fatal("missing CA must fail")
	}
}
