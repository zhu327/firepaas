package main

import (
	"strings"
	"testing"
)

// v1.4（ADR-0030）：DATASET_RO 只接受无 userinfo/query/fragment 的
// credential-free HTTPS 来源（loopback 例外仅供密闭 e2e）。
func TestNormalizeDatasetSourceURL(t *testing.T) {
	for _, raw := range []string{
		"https://objects.example/datasets/a.tar.gz",
		"https://objects.example:8443/datasets/a.tar.gz",
	} {
		got, err := normalizeDatasetSourceURL(raw)
		if err != nil {
			t.Fatalf("normalizeDatasetSourceURL(%q): %v", raw, err)
		}
		if strings.Contains(got, "?") || strings.Contains(got, "#") {
			t.Fatalf("normalized URL keeps query/fragment: %q", got)
		}
	}
	for _, raw := range []string{
		"https://user:pass@objects.example/d.tar.gz",
		"https://objects.example/d.tar.gz?X-Amz-Signature=abc",
		"https://objects.example/d.tar.gz#frag",
		"http://objects.example/d.tar.gz",
		"file:///tmp/d.tar.gz",
		"not a url",
		"",
	} {
		if _, err := normalizeDatasetSourceURL(raw); err == nil {
			t.Fatalf("normalizeDatasetSourceURL(%q) must reject", raw)
		}
	}
	// hermetic e2e loopback 例外。
	if _, err := normalizeDatasetSourceURL("http://127.0.0.1:9000/d.tar.gz"); err != nil {
		t.Fatalf("loopback test exception: %v", err)
	}
}

func TestDatasetSourceDigestStableAndShort(t *testing.T) {
	a := datasetSourceDigest("https://objects.example/a.tar.gz")
	b := datasetSourceDigest("https://objects.example/a.tar.gz")
	c := datasetSourceDigest("https://objects.example/b.tar.gz")
	if a != b {
		t.Fatal("digest must be stable")
	}
	if a == c {
		t.Fatal("digest must distinguish sources")
	}
	if len(a) != 16 {
		t.Fatalf("digest length = %d, want 16", len(a))
	}
}
