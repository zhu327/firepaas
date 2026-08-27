package state

import (
	"os"
	"path/filepath"
	"testing"
)

// M4：验证材料只存摘要；校验含 execution 绑定与恒时比较。
func TestCredsLifecycle(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCreds(filepath.Join(dir, "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest("raw-token")
	if err := c.Set("m1", "exec-1", digest); err != nil {
		t.Fatal(err)
	}
	if !c.Verify("m1", "exec-1", "raw-token") {
		t.Fatal("valid credential must verify")
	}
	if c.Verify("m1", "exec-2", "raw-token") {
		t.Fatal("wrong execution must not verify (replaced execution revoked)")
	}
	if c.Verify("m1", "exec-1", "wrong") {
		t.Fatal("wrong credential must not verify")
	}
	if c.Verify("ghost", "exec-1", "raw-token") {
		t.Fatal("unknown machine must not verify")
	}
	// 落盘重载后仍可校验（agent 重启场景）。
	c2, err := OpenCreds(filepath.Join(dir, "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !c2.Verify("m1", "exec-1", "raw-token") {
		t.Fatal("digest must survive restart")
	}
	// 删除即撤销。
	if err := c2.Drop("m1"); err != nil {
		t.Fatal(err)
	}
	if c2.Verify("m1", "exec-1", "raw-token") {
		t.Fatal("dropped credential must not verify")
	}
	// 摘要文件中不得出现原文。
	b := readFile(t, filepath.Join(dir, "credentials.json"))
	if contains(b, "raw-token") {
		t.Fatal("raw credential persisted to disk!")
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
