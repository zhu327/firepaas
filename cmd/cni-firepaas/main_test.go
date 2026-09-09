package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runShim 以给定 env/stdin 执行 run()（进程内），返回 stdout 内容与错误。
func runShim(t *testing.T, env map[string]string, stdin string) (string, error) {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	old := os.Stdin
	if stdin != "" {
		f, err := os.CreateTemp(t.TempDir(), "stdin")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(stdin); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := os.Open(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		os.Stdin = r
		t.Cleanup(func() { _ = r.Close() })
	}
	t.Cleanup(func() { os.Stdin = old })
	var buf bytes.Buffer
	// run() 输出到 os.Stdout；测试里替换。
	oldOut := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	err := run()
	_ = w.Close()
	os.Stdout = oldOut
	_, _ = buf.ReadFrom(r)
	return buf.String(), err
}

func writeFence(t *testing.T, opID string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fence")
	if err := os.WriteFile(path, []byte(opID), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestVersionCommand：VERSION 无 fence 要求，输出 supportedVersions。
func TestVersionCommand(t *testing.T) {
	out, err := runShim(t, map[string]string{"CNI_COMMAND": "VERSION"}, "")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		CniVersion        string   `json:"cniVersion"`
		SupportedVersions []string `json:"supportedVersions"`
	}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("bad version output: %s", out)
	}
	if v.CniVersion != "1.0.0" || len(v.SupportedVersions) < 2 {
		t.Fatalf("version = %+v", v)
	}
}

// TestFenceGuard（§20）：无 fence / 错权限 / 空 fence 一律拒绝且不触碰状态。
func TestFenceGuard(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"no fence":      {"CNI_COMMAND": "ADD"},
		"missing file":  {"CNI_COMMAND": "ADD", "FIREPAAS_CNI_FENCE_FILE": "/nonexistent/fence"},
		"empty env var": {"CNI_COMMAND": "ADD", "FIREPAAS_CNI_FENCE_FILE": ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := runShim(t, env, `{"machine_id":"m1","state_path":"/tmp/x.json"}`)
			if err == nil {
				t.Fatal("expected fence rejection")
			}
			if !strings.Contains(err.Error(), "fence") && !strings.Contains(err.Error(), "FIREPAAS_CNI_FENCE_FILE") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
	// 权限过宽（0644）→ 拒绝。
	path := filepath.Join(t.TempDir(), "fence")
	if err := os.WriteFile(path, []byte("op-1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runShim(t, map[string]string{
		"CNI_COMMAND": "ADD", "FIREPAAS_CNI_FENCE_FILE": path,
	}, `{"machine_id":"m1","state_path":"/tmp/x.json"}`); err == nil {
		t.Fatal("expected permission rejection")
	}
}

// TestProtocolConformance：bundle 校验与不支持命令。
func TestProtocolConformance(t *testing.T) {
	fence := writeFence(t, "op-cni-1")
	// 缺 state_path → 拒绝。
	if _, err := runShim(t, map[string]string{
		"CNI_COMMAND": "ADD", "CNI_CONTAINERID": "m1", "FIREPAAS_CNI_FENCE_FILE": fence,
	}, `{"machine_id":"m1"}`); err == nil {
		t.Fatal("expected bundle rejection")
	}
	// 不支持命令。
	if _, err := runShim(t, map[string]string{
		"CNI_COMMAND": "GC", "FIREPAAS_CNI_FENCE_FILE": fence,
	}, `{"machine_id":"m1","state_path":"/tmp/x.json"}`); err == nil {
		t.Fatal("expected command rejection")
	}
	// 非 JSON bundle。
	if _, err := runShim(t, map[string]string{
		"CNI_COMMAND": "ADD", "FIREPAAS_CNI_FENCE_FILE": fence,
	}, `not-json`); err == nil {
		t.Fatal("expected bundle parse rejection")
	}
}

// TestFenceMachineBinding（W5）：fence 第二行绑定 machine 时必须与 bundle
// 一致；单行旧格式仍接受。
func TestFenceMachineBinding(t *testing.T) {
	bound := filepath.Join(t.TempDir(), "fence")
	if err := os.WriteFile(bound, []byte("op-2\nm1"), 0o600); err != nil {
		t.Fatal(err)
	}
	// bundle machine 不一致 → 拒绝。
	if _, err := runShim(t, map[string]string{
		"CNI_COMMAND": "CHECK", "FIREPAAS_CNI_FENCE_FILE": bound,
	}, `{"machine_id":"m2","state_path":"/tmp/x.json"}`); err == nil ||
		!strings.Contains(err.Error(), "bound to machine") {
		t.Fatalf("expected fence binding rejection, got %v", err)
	}
}

// TestBundleBackendRejectsUnknown（W5）：未知后端名 fail closed。
func TestBundleBackendRejectsUnknown(t *testing.T) {
	fence := writeFence(t, "op-cni-9")
	if _, err := runShim(t, map[string]string{
		"CNI_COMMAND": "ADD", "FIREPAAS_CNI_FENCE_FILE": fence,
	}, `{"machine_id":"m1","state_path":"/tmp/x.json","subnet_cidr":"10.100.0.0/16","backend":"vxlan"}`); err == nil ||
		!strings.Contains(err.Error(), "unknown bundle backend") {
		t.Fatalf("expected backend rejection, got %v", err)
	}
}
