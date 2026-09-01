package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadAtomicallyPreservesExistingFileOnTruncation(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "out")
	if err := os.WriteFile(dst, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	resp := &http.Response{ContentLength: 10, Body: io.NopCloser(strings.NewReader("short"))}
	if err := downloadAtomically(resp, dst); err == nil {
		t.Fatal("expected truncation error")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original" {
		t.Fatalf("existing file changed: %q", got)
	}
}

func TestImagesCoverageURLIsEncoded(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)
	image := "registry.example/team/app@sha256:" + strings.Repeat("a", 64)
	if err := runImagesCoverage([]string{"--image", image, "--node-pool", "pool a&b"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gotQuery, "pool a&b") || !strings.Contains(gotQuery, "node_pool=pool+a%26b") {
		t.Fatalf("query not encoded: %q", gotQuery)
	}
}

func TestDownloadAtomicallyRejectsOversizeHeader(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "out")
	resp := &http.Response{ContentLength: runtimeMaxDownload + 1, Body: io.NopCloser(strings.NewReader(""))}
	if err := downloadAtomically(resp, dst); err == nil {
		t.Fatal("expected size error")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("destination created: %v", err)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, ".fpctl-download-*"))
	if len(matches) != 0 {
		t.Fatalf("temporary files leaked: %v", matches)
	}
}
