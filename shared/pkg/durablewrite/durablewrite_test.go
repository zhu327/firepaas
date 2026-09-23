package durablewrite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomicRoundTripAndModes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "ledger.json")
	if err := WriteFileAtomic(path, "test", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != `{"n":1}` {
		t.Fatalf("round trip = %q err=%v", data, err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("tmp residue left after success")
	}
	if got := statPerm(t, filepath.Dir(path)); got != 0o700 {
		t.Fatalf("dir mode = %o, want 700", got)
	}
	if got := statPerm(t, path); got != 0o600 {
		t.Fatalf("file mode = %o, want 600", got)
	}
	// 覆盖写：旧内容被原子替换。
	if err := WriteFileAtomic(path, "test", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	if string(data) != `{"n":2}` {
		t.Fatalf("overwrite = %q", data)
	}
}

// rename 前掉电（rename 失败）：原文件 intact，tmp 已清理，调用方重试可恢复。
func TestWriteFileAtomicRenameFailureKeepsOriginal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.json")
	if err := WriteFileAtomic(path, "test", []byte(`v1`)); err != nil {
		t.Fatal(err)
	}
	deps := Deps{Rename: func(_, _ string) error { return errors.New("power loss before rename") }}
	if err := WriteFileAtomicWithDeps(path, "test", []byte(`v2`), deps); err == nil {
		t.Fatal("want rename error")
	}
	if data, _ := os.ReadFile(path); string(data) != `v1` {
		t.Fatalf("original clobbered: %q", data)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("tmp not cleaned after rename failure")
	}
	// 重试成功。
	if err := WriteFileAtomic(path, "test", []byte(`v2`)); err != nil {
		t.Fatal(err)
	}
}

// fsync 失败（file）：数据未落盘即报错，原文件 intact，无 tmp 残留。
func TestWriteFileAtomicFileSyncFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.json")
	if err := WriteFileAtomic(path, "test", []byte(`v1`)); err != nil {
		t.Fatal(err)
	}
	deps := Deps{SyncFile: func(_ *os.File) error { return errors.New("fsync failed") }}
	if err := WriteFileAtomicWithDeps(path, "test", []byte(`v2`), deps); err == nil {
		t.Fatal("want fsync error")
	}
	if data, _ := os.ReadFile(path); string(data) != `v1` {
		t.Fatalf("original clobbered: %q", data)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("tmp not cleaned after fsync failure")
	}
}

// fsync 失败（dir）：rename 已发生但目录项未落盘——掉电可能回滚到旧目录项；
// 本函数必须报错，让调用方（ledger/fences）不推进内存态（fail-closed）。
func TestWriteFileAtomicDirSyncFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.json")
	if err := WriteFileAtomic(path, "test", []byte(`v1`)); err != nil {
		t.Fatal(err)
	}
	deps := Deps{SyncDir: func(_ *os.File) error { return errors.New("dir fsync failed") }}
	if err := WriteFileAtomicWithDeps(path, "test", []byte(`v2`), deps); err == nil {
		t.Fatal("want dir fsync error")
	}
}

// tmp 残留：上次崩溃留下的 stale tmp 不影响下次成功，且成功后无残留。
func TestWriteFileAtomicStaleTmpOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ledger.json")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", []byte("stale-partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(path, "test", []byte(`fresh`)); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(path); string(data) != `fresh` {
		t.Fatalf("stale tmp poisoned write: %q", data)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("tmp residue left after success over stale tmp")
	}
}

func statPerm(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
