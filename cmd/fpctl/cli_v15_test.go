// cli_v15_test.go：v1.5 新增 CLI 的无网络单测（usage 错误路径 + 幂等头透传）。
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewCommandsUsageWithoutNetwork(t *testing.T) {
	cases := [][]string{
		{"project"},
		{"nodes"},
		{"events"},
		{"machines"},
		{"wait"},
		{"ttl"},
		{"snapshot"},
		{"volume"},
		{"snapshot", "create"},
		{"snapshot", "fork", "snap-1"},
		{"snapshot", "rescue", "m-1"},
		{"volume", "create"},
		{"volume", "attach", "m-1"},
		{"wait", "machine", "m-1"},
		{"wait", "rollout", "r-1"},
		{"ttl", "set", "m-1"},
		{"nodes", "drain"},
		{"project", "quota"},
		{"apikey"},
		{"apikey", "create"},
		{"apikey", "rotate"},
		{"bogus"},
	}
	for _, c := range cases {
		if err := run(c); err == nil {
			t.Errorf("run(%v) must fail without network/args", c)
		}
	}
}

func TestImagesPrewarmSendsIdempotencyKey(t *testing.T) {
	var gotKey, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotPath = r.Header.Get("Idempotency-Key"), r.URL.Path
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)
	digest := "sha256:" + strings.Repeat("b", 64)
	if err := runImagesPrewarm([]string{
		"--image", "registry.example/app@" + digest,
		"--node-pool", "pool-a", "--idempotency-key", "k-123",
	}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/images/prewarm" || gotKey != "k-123" {
		t.Fatalf("prewarm request = %s key=%q, want /v1/images/prewarm key=k-123", gotPath, gotKey)
	}
}

func TestImagesUnpinSendsIdempotencyKey(t *testing.T) {
	var gotKey, gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotPath, gotMethod = r.Header.Get("Idempotency-Key"), r.URL.Path, r.Method
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)
	if err := runImagesUnpin([]string{"pin-1", "--idempotency-key", "k-unpin"}); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/v1/images/pins/pin-1" || gotKey != "k-unpin" {
		t.Fatalf("unpin request = %s %s key=%q", gotMethod, gotPath, gotKey)
	}
}

func TestApikeyRotateDispatch(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		_, _ = w.Write([]byte(`{"id":"apik_new","key":"fp_new","scopes":["read"],"project_id":"p-a"}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)
	if err := runAPIKey([]string{"rotate", "apik_old"}); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/apikeys/apik_old/rotate" {
		t.Fatalf("rotate request = %s %s", gotMethod, gotPath)
	}
}

func TestVolumeAttachParsesFlagsAfterPositional(t *testing.T) {
	var gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath() + "?" + r.URL.RawQuery
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)
	if err := runVolume([]string{"attach", "m-1", "--volume", "vol-1", "--mount-path", "/data"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotPath, "/v1/machines/m-1/volume-attach") || !strings.Contains(gotPath, "volume_id=vol-1") {
		t.Fatalf("attach path = %q", gotPath)
	}
	if !strings.Contains(gotBody, "/data") {
		t.Fatalf("attach body = %q", gotBody)
	}
}

func TestWaitMachineParsesFlagsAfterPositional(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)
	if err := runWait([]string{"machine", "m-9", "--execution", "ex-1"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotQuery, "execution_id=ex-1") {
		t.Fatalf("wait query = %q", gotQuery)
	}
}

func TestResolveIdemKey(t *testing.T) {
	if got := resolveIdemKey("explicit"); got != "explicit" {
		t.Fatalf("explicit key = %q", got)
	}
	t.Setenv("FP_IDEMPOTENCY_KEY", "from-env")
	if got := resolveIdemKey(""); got != "from-env" {
		t.Fatalf("env key = %q", got)
	}
}
