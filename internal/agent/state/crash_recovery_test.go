package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// corrupt JSON 必须 fail-closed：Open 报错，调用方不以空状态继续
// （空状态会放行旧 generation，fence 形同虚设）。
func TestFencesCorruptFileFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fences.json")
	if err := os.WriteFile(path, []byte(`{corrupt`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFences(path); err == nil {
		t.Fatal("corrupt fences must fail Open, not start empty")
	}
}

func TestLedgerCorruptFileFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	if err := os.WriteFile(path, []byte(`[1,2`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("corrupt ledger must fail Open, not start empty")
	}
}

// ledger persist 失败（path 被目录占住）时 Begin 必须回滚内存 claim，
// Get 不得看到半写记录。
func TestLedgerBeginPersistFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(filepath.Join(dir, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	// path 指到文件之下的不可建目录：Open 已成功，persist 必败。
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	l.path = filepath.Join(blocker, "ledger.json")
	rec := Record{OperationID: "op-1", MachineID: "m-1", RequestHash: "h"}
	if _, _, err := l.Begin(rec); err == nil {
		t.Fatal("Begin with uncreatable dir must fail persist")
	}
	if _, ok, _ := l.Get("op-1", "h"); ok {
		t.Fatal("failed Begin must not leave memory claim")
	}
}

// fences persist 失败时 Advance 必须回滚内存高水位：旧代仍按旧水位判定，
// 不出现“内存已推进、磁盘未落盘”的分叉。
func TestFencesAdvancePersistFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	f, err := OpenFences(filepath.Join(dir, "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.path = filepath.Join(blocker, "fences.json")
	if err := f.Advance("m", 5, "e1"); err == nil {
		t.Fatal("Advance with uncreatable dir must fail persist")
	}
	// 回滚后未知 machine 语义：任何 generation 通过（未推进）。
	if err := f.Check("m", 1); err != nil {
		t.Fatalf("failed Advance must not advance water mark: %v", err)
	}
	// 同代刷新路径同样回滚。
	path := filepath.Join(t.TempDir(), "fences.json")
	f2, err := OpenFences(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f2.Advance("m", 5, "e1"); err != nil {
		t.Fatal(err)
	}
	f2.path = filepath.Join(blocker, "fences.json") // 后续 persist 必败
	if err := f2.Advance("m", 5, "e2"); err == nil {
		t.Fatal("same-generation Advance persist failure must surface")
	}
	if !errors.Is(f2.Check("m", 4), ErrStaleGeneration) {
		t.Fatal("water mark must survive failed same-generation refresh")
	}
}
