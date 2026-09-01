package machine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net/netip"
	"strings"
	"testing"
)

func datasetArchive(t *testing.T, name string, typ byte, body []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: typ, Size: int64(len(body)), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if len(body) > 0 {
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestValidateDatasetArchiveRejectsUnsafeEntries(t *testing.T) {
	cases := []struct {
		name string
		typ  byte
	}{{"../escape", tar.TypeReg}, {"link", tar.TypeSymlink}, {"hard", tar.TypeLink}, {"dev", tar.TypeChar}, {"pipe", tar.TypeFifo}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateDatasetArchive(bytes.NewReader(datasetArchive(t, tc.name, tc.typ, nil)), 1024, 10); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestValidateDatasetArchiveLimits(t *testing.T) {
	good := datasetArchive(t, "dir/file", tar.TypeReg, []byte("hello"))
	if err := validateDatasetArchive(bytes.NewReader(good), 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateDatasetArchive(bytes.NewReader(good), 4, 1); err == nil {
		t.Fatal("expected expanded-size rejection")
	}
	if err := validateDatasetArchive(bytes.NewReader(good), 5, 0); err == nil {
		t.Fatal("expected file-count rejection")
	}
	// maxFiles bounds every tar entry, not only regular files.
	if err := validateDatasetArchive(bytes.NewReader(datasetArchive(t, "dir", tar.TypeDir, nil)), 5, 0); err == nil {
		t.Fatal("expected directory entry-count rejection")
	}
	deep := strings.Repeat("d/", maxDatasetPathDepth) + "file"
	if err := validateDatasetArchive(bytes.NewReader(datasetArchive(t, deep, tar.TypeReg, nil)), 5, 1); err == nil {
		t.Fatal("expected path-depth rejection")
	}
	long := strings.Repeat("x", maxDatasetPathBytes+1)
	if err := validateDatasetArchive(bytes.NewReader(datasetArchive(t, long, tar.TypeReg, nil)), 5, 1); err == nil {
		t.Fatal("expected path-length rejection")
	}
}

func TestValidateDatasetURL(t *testing.T) {
	if err := validateDatasetURL("https://objects.example/d.tar.gz", false); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"http://objects.example/d.tar.gz", "file:///tmp/d", "https://user:pass@objects.example/d", "ftp://objects.example/d"} {
		if err := validateDatasetURL(raw, false); err == nil {
			t.Fatalf("expected rejection for %s", raw)
		}
	}
	// v1.4（ADR-0030）：query/fragment 可能携带签名或会话凭证，必须拒绝。
	for _, raw := range []string{
		"https://objects.example/d.tar.gz?X-Amz-Signature=abc",
		"https://objects.example/d.tar.gz#frag",
		"https://objects.example/d.tar.gz?sig=1#both",
		"https://user@objects.example/d",
	} {
		if err := validateDatasetURL(raw, false); err == nil {
			t.Fatalf("expected rejection for %s", raw)
		}
	}
	if err := validateDatasetURL("http://127.0.0.1:8080/d", true); err != nil {
		t.Fatal(err)
	}
}

// v1.4：下载失败错误不得携带来源 URL（含 host）。
func TestRedactDatasetSource(t *testing.T) {
	secret := "https://objects.internal.example/a/b/d.tar.gz"
	err := redactDatasetSource(secret, fmt.Errorf("Get %q: dial tcp: lookup objects.internal.example: no such host", secret))
	msg := err.Error()
	if strings.Contains(msg, "objects.internal.example") || strings.Contains(msg, "d.tar.gz") {
		t.Fatalf("error leaks dataset origin: %s", msg)
	}
	if !strings.Contains(msg, "no such host") {
		t.Fatalf("redaction must preserve the underlying cause: %s", msg)
	}
	if err := redactDatasetSource(secret, nil); err != nil {
		t.Fatalf("nil error must stay nil: %v", err)
	}
}

func TestPublicDatasetAddrRejectsAllSpecialUseRanges(t *testing.T) {
	blocked := []string{
		"100.64.0.1", "198.18.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1",
		"192.0.0.9", "169.254.169.254", "2001:db8::1",
		"2001:2::1", "3fff::1", "64:ff9b::1", "100::1",
	}
	for _, raw := range blocked {
		if publicDatasetAddr(netip.MustParseAddr(raw), false) {
			t.Errorf("special-use address %s was allowed", raw)
		}
	}
	// 4-in-6 形态在入口层被拒绝（dial 层对 resolver 产物先 Unmap 再判真实地址）。
	if publicDatasetAddr(netip.MustParseAddr("::ffff:169.254.169.254"), false) {
		t.Error("IPv4-mapped IPv6 address was allowed")
	}
	if publicDatasetAddr(netip.MustParseAddr("::ffff:8.8.8.8"), false) {
		t.Error("IPv4-mapped IPv6 address was allowed")
	}
	// Unmap 后按真实地址判定（dial 层语义）。
	if !publicDatasetAddr(netip.MustParseAddr("::ffff:8.8.8.8").Unmap(), false) {
		t.Error("public address after unmap was rejected")
	}
	for _, raw := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !publicDatasetAddr(netip.MustParseAddr(raw), false) {
			t.Errorf("public address %s was rejected", raw)
		}
	}
}
