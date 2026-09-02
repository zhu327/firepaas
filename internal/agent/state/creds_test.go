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

// M4/P0#1：Ensure 供 create 重放补写 credential——缺材料才写、同值不重写、
// 换代覆盖；落盘遵守 0600 + temp/fsync/rename/fsync(dir) 纪律（可观测面：
// 权限、无 .tmp 残留、重启后内容一致）。
func TestCredsEnsureAndDurablePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.json")
	c, err := OpenCreds(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest("token-a")
	if err := c.Ensure("m1", "exec-1", digest); err != nil {
		t.Fatal(err)
	}
	if !c.Verify("m1", "exec-1", "token-a") {
		t.Fatal("ensure on empty store must install the digest")
	}
	first := readFile(t, path)
	// 同值 Ensure 不得重写（否则重放每次都产生一次无意义落盘）。
	if err := c.Ensure("m1", "exec-1", digest); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != first {
		t.Fatal("ensure with identical entry must not rewrite the file")
	}
	// 换代（execution/digest 变化）必须覆盖。
	if err := c.Ensure("m1", "exec-2", Digest("token-b")); err != nil {
		t.Fatal(err)
	}
	if c.Verify("m1", "exec-1", "token-a") || !c.Verify("m1", "exec-2", "token-b") {
		t.Fatal("ensure on a replaced execution must overwrite the entry")
	}
	// 0600 权限 + 无 .tmp 残留。
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("creds file perm = %o, want 600", perm)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file must not survive rename: %v", err)
	}
	// 重载后内容一致（rename 语义）。
	c2, err := OpenCreds(path)
	if err != nil || !c2.Verify("m1", "exec-2", "token-b") {
		t.Fatalf("reloaded creds: %v", err)
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
	return len(s) >= len(sub) && func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}
