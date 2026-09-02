package main

import (
	"strings"
	"testing"
	"time"
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
