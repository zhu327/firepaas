// cli_flags_test.go：全局 flags/version/help、--json、路径转义、HTTP 超时
// 与 app create 新 flags 的无网络或测试服务器单测。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestMain 支持把测试二进制自身当 CLI 重新执行（验证真实退出码与 usage）。
func TestMain(m *testing.M) {
	if os.Getenv("FPCTL_TEST_MAIN") == "1" {
		for i, a := range os.Args {
			if a == "--" {
				os.Args = append([]string{"fpctl"}, os.Args[i+1:]...)
				break
			}
		}
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runCLI(t *testing.T, args ...string) (string, int) {
	t.Helper()
	cmdArgs := append([]string{"-test.run=^$", "--"}, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), "FPCTL_TEST_MAIN=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	t.Fatalf("run %v: %v", args, err)
	return "", -1
}

func TestHelpExitsZeroWithUsage(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {}} {
		out, code := runCLI(t, args...)
		if code != 0 {
			t.Fatalf("runCLI(%v) exit = %d, want 0\n%s", args, code, out)
		}
		if !strings.Contains(out, "usage") && !strings.Contains(out, "用法") {
			t.Fatalf("runCLI(%v) missing usage:\n%s", args, out)
		}
	}
}

func TestVersionCommandAndFlag(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}} {
		out, code := runCLI(t, args...)
		if code != 0 {
			t.Fatalf("runCLI(%v) exit = %d, want 0\n%s", args, code, out)
		}
		if !strings.Contains(out, version) {
			t.Fatalf("runCLI(%v) output %q missing version %q", args, out, version)
		}
	}
}

func TestUnknownGlobalFlagNonZeroExit(t *testing.T) {
	_, code := runCLI(t, "--definitely-not-a-flag")
	if code == 0 {
		t.Fatal("unknown global flag must exit non-zero")
	}
}

func TestUnknownSubcommandFlagNonZeroExitWithUsage(t *testing.T) {
	out, code := runCLI(t, "app", "create", "--definitely-not-a-flag")
	if code == 0 {
		t.Fatalf("unknown subcommand flag must exit non-zero, got 0\n%s", out)
	}
	if !strings.Contains(out, "flag provided but not defined") {
		t.Fatalf("missing flag error in output:\n%s", out)
	}
	if !strings.Contains(out, "Usage of create") && !strings.Contains(out, "--hostname") {
		t.Fatalf("missing usage in output:\n%s", out)
	}
}

func TestJSONOutputParses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(
			[]byte(
				`{"keys":[{"id":"apik_1","name":"n","scopes":["read"],"project_id":"","created_at":"2026-01-01T00:00:00Z","last_used_at":null,"revoked":false}]}`,
			),
		)
	}))
	defer srv.Close()

	var buf bytes.Buffer
	old := stdout
	stdout = &buf
	defer func() {
		stdout = old
		global = globalOptions{}
	}()

	t.Setenv("FP_API_ADDR", srv.URL)
	t.Setenv("FP_PROJECT", "p-json")
	if err := run([]string{"--json", "apikey", "ls"}); err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("--json output is not valid JSON: %v\n%s", err, buf.String())
	}
	if _, ok := parsed["keys"]; !ok {
		t.Fatalf("--json output missing keys: %s", buf.String())
	}
}

func TestPathSegmentsAreEscaped(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"ops show", []string{"ops", "show", "op/1 2"}, "/v1/operations/op%2F1%202"},
		{"app status", []string{"app", "status", "app/1 2"}, "/v1/apps/app%2F1%202"},
		{"machines show", []string{"machines", "show", "m/1 2"}, "/v1/machines/m%2F1%202"},
		{"volume show", []string{"volume", "show", "vol/1 2"}, "/v1/volumes/vol%2F1%202"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.EscapedPath()
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()
			t.Setenv("FP_API_ADDR", srv.URL)
			if err := run(tc.args); err != nil {
				t.Fatal(err)
			}
			if gotPath != tc.want {
				t.Fatalf("path = %q, want %q", gotPath, tc.want)
			}
		})
	}
}

func TestSharedClientsUseResponseHeaderTimeout(t *testing.T) {
	assert := func(name string, c *http.Client, want time.Duration) {
		tr, ok := c.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("%s transport = %T, want *http.Transport", name, c.Transport)
		}
		if tr.ResponseHeaderTimeout != want {
			t.Fatalf("%s ResponseHeaderTimeout = %v, want %v", name, tr.ResponseHeaderTimeout, want)
		}
		if tr.DialContext == nil {
			t.Fatalf("%s missing DialContext timeout", name)
		}
	}
	assert("apiClient", apiClient, apiResponseHeaderTimeout)
	assert("longClient", longClient, longRequestHeaderTimeout)
	if apiResponseHeaderTimeout >= longRequestHeaderTimeout {
		t.Fatalf("stream/long-poll timeout must exceed normal timeout")
	}
}

func TestDoEnforcesResponseHeaderTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)

	old := apiClient
	apiClient = newHTTPClient(50 * time.Millisecond)
	defer func() { apiClient = old }()

	start := time.Now()
	err := do("GET", "/v1/apps", nil, nil)
	if err == nil {
		t.Fatal("expected response-header timeout error")
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("timeout not enforced, request took %v", elapsed)
	}
}

func TestErrorBodyBoundedAndSuppressedOn5xx(t *testing.T) {
	long4xx := strings.Repeat("x", errBodySnippetMax+100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/apps" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, long4xx)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "internal stack/secret material")
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)

	err := do("GET", "/v1/apps", nil, nil)
	if err == nil {
		t.Fatal("expected 400 error")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("error missing status: %v", err)
	}
	if len(err.Error()) > errBodySnippetMax+64 {
		t.Fatalf("4xx error body not bounded (%d chars): %s", len(err.Error()), err.Error())
	}

	err = do("GET", "/v1/apps/secret", nil, nil)
	if err == nil {
		t.Fatal("expected 500 error")
	}
	if strings.Contains(err.Error(), "secret material") {
		t.Fatalf("5xx error leaked body: %v", err)
	}
}

func TestAppCreateSendsNewFields(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"app_id":"a"}`))
	}))
	defer srv.Close()
	t.Setenv("FP_API_ADDR", srv.URL)

	err := runApp([]string{
		"create",
		"--hostname", "h.example", "--image", "registry.example/app@sha256:" + strings.Repeat("a", 64),
		"--project", "p-a",
		"--env", "A=1", "--env", "B=2",
		"--label", "tier=web",
		"--node-pool", "pool-a",
		"--anti-affinity", "deployment",
		"--health-check-http", "http://127.0.0.1:8080/healthz",
		"--health-check-interval", "5",
		"--health-check-timeout", "2",
		"--health-check-unhealthy-threshold", "3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotBody["project_id"] != "p-a" {
		t.Fatalf("project_id = %v", gotBody["project_id"])
	}
	env, _ := gotBody["env"].(map[string]any)
	if env["A"] != "1" || env["B"] != "2" {
		t.Fatalf("env = %v", gotBody["env"])
	}
	labels, _ := gotBody["labels"].(map[string]any)
	if labels["tier"] != "web" {
		t.Fatalf("labels = %v", gotBody["labels"])
	}
	if gotBody["node_pool"] != "pool-a" {
		t.Fatalf("node_pool = %v", gotBody["node_pool"])
	}
	if gotBody["anti_affinity"] != "DEPLOYMENT" {
		t.Fatalf("anti_affinity = %v", gotBody["anti_affinity"])
	}
	hc, _ := gotBody["health_check"].(map[string]any)
	if hc["type"] != "http" || hc["target"] != "http://127.0.0.1:8080/healthz" {
		t.Fatalf("health_check = %v", gotBody["health_check"])
	}
	if hc["interval_seconds"] != float64(5) || hc["timeout_seconds"] != float64(2) ||
		hc["unhealthy_threshold"] != float64(3) {
		t.Fatalf("health_check tuning = %v", hc)
	}
}

func TestAppCreateRejectsUnknownAntiAffinityAndBadHealthCheck(t *testing.T) {
	t.Setenv("FP_API_ADDR", "http://127.0.0.1:1") // 不应被访问
	base := []string{"create", "--hostname", "h", "--image", "img", "--anti-affinity", "same-node"}
	if err := runApp(base); err == nil || !strings.Contains(err.Error(), "anti-affinity") {
		t.Fatalf("unknown anti-affinity err = %v", err)
	}
	if err := runApp([]string{
		"create", "--hostname", "h", "--image", "img",
		"--health-check-http", "http://a", "--health-check-tcp", "127.0.0.1:80",
	}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("health check exclusivity err = %v", err)
	}
	if err := runApp([]string{
		"create", "--hostname", "h", "--image", "img", "--health-check-tcp", "not-a-hostport",
	}); err == nil || !strings.Contains(err.Error(), "health-check-tcp") {
		t.Fatalf("bad tcp target err = %v", err)
	}
}

func TestGlobalFlagsOverrideEnv(t *testing.T) {
	var gotAuth, gotQuery, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotQuery, gotPath = r.Header.Get("Authorization"), r.URL.RawQuery, r.URL.Path
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	defer func() { global = globalOptions{} }()
	t.Setenv("FP_API_ADDR", "http://127.0.0.1:1") // 不可达，若被使用则失败
	t.Setenv("FP_API_TOKEN", "env-token")
	t.Setenv("FP_PROJECT", "env-project")

	if err := run([]string{"--addr", srv.URL, "--token", "flag-token", "--project", "flag-project", "volume", "ls"}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/volumes" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotAuth != "Bearer flag-token" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if !strings.Contains(gotQuery, "project_id=flag-project") {
		t.Fatalf("query = %q", gotQuery)
	}
}
