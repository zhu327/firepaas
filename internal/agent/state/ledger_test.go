package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLedgerPutCheckReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "m-1", "hash-1", []byte(`{"machine_id":"m"}`)); err != nil {
		t.Fatal(err)
	}
	result, ok, err := l.Check("op-1", "hash-1")
	if err != nil || !ok {
		t.Fatalf("check after put: ok=%v err=%v", ok, err)
	}
	if !jsonEqual(t, result, []byte(`{"machine_id":"m"}`)) {
		t.Fatalf("unexpected result: %s", result)
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(av, bv)
}

func TestLedgerRestartReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "m-1", "hash-1", []byte(`{"machine_id":"m"}`)); err != nil {
		t.Fatal(err)
	}

	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	result, ok, err := l2.Check("op-1", "hash-1")
	if err != nil || !ok || !jsonEqual(t, result, []byte(`{"machine_id":"m"}`)) {
		t.Fatalf("replay failed: ok=%v err=%v result=%s", ok, err, result)
	}
}

func TestLedgerInProgressSurvivesCrashWindowAndCompletesAfterRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	claim := Record{
		OperationID: "op-crash", MachineID: "m-1", ExecutionID: "e-1", Generation: 7,
		Kind: "snapshot.create", Identity: []byte(`{"snapshot_id":"s-1"}`), RequestHash: "hash-1",
	}
	if _, existing, err := l.Begin(claim); err != nil || existing {
		t.Fatalf("begin: existing=%v err=%v", existing, err)
	}

	// Simulate process death after the external side effect but before Complete.
	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rec, existing, err := restarted.Begin(claim)
	if err != nil || !existing || rec.Status != StatusInProgress || rec.Kind != claim.Kind ||
		!jsonEqual(t, rec.Identity, claim.Identity) {
		t.Fatalf("recovered claim = %#v existing=%v err=%v", rec, existing, err)
	}
	if _, completed, err := restarted.Check(claim.OperationID, claim.RequestHash); err != nil || completed {
		t.Fatalf("in-progress claim must not replay as completed: completed=%v err=%v", completed, err)
	}
	if err := restarted.Complete(claim.OperationID, claim.RequestHash, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	result, completed, err := restarted.Check(claim.OperationID, claim.RequestHash)
	if err != nil || !completed || !jsonEqual(t, result, []byte(`{"ok":true}`)) {
		t.Fatalf("completed recovery: completed=%v err=%v result=%s", completed, err, result)
	}
}

func TestLedgerLoadsLegacyRecordAsCompleted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	legacy := `{"op-old":{"operation_id":"op-old","machine_id":"m-1","request_hash":"hash-1","result":{"legacy":true},"created_at":"2026-08-01T00:00:00Z"}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	result, ok, err := l.Check("op-old", "hash-1")
	if err != nil || !ok || !jsonEqual(t, result, []byte(`{"legacy":true}`)) {
		t.Fatalf("legacy replay: ok=%v err=%v result=%s", ok, err, result)
	}
}

func TestLedgerConflictingHashRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "m-1", "hash-1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "m-1", "hash-2", []byte(`{}`)); !errors.Is(err, ErrRequestHashConflict) {
		t.Fatalf("expected ErrRequestHashConflict, got %v", err)
	}
	if _, _, err := l.Check("op-1", "hash-2"); !errors.Is(err, ErrRequestHashConflict) {
		t.Fatalf("expected conflict on check, got %v", err)
	}
}

func TestLedgerIdempotentPut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "m-1", "hash-1", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	// 第二次相同 hash 不覆盖首次结果。
	if err := l.Put("op-1", "m-1", "hash-1", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	result, ok, err := l.Check("op-1", "hash-1")
	if err != nil || !ok || !jsonEqual(t, result, []byte(`{"n":1}`)) {
		t.Fatalf("idempotent put failed: ok=%v err=%v result=%s", ok, err, result)
	}
}

func TestLedgerMissingOperation(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := l.Check("nope", "hash"); err != nil || ok {
		t.Fatalf("missing op: ok=%v err=%v", ok, err)
	}
}

// Put 记录 machine_id；machine 删除后 PruneMachineExcept 清掉该 machine 的
// 历史记录但保留 delete 自身的去重记录（M1 评审 P2-5）。
func TestPruneMachineExcept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-create", "m-1", "hash-1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-other", "m-2", "hash-2", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-del", "m-1", "hash-3", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	// 旧格式记录（machine_id 为空）不受影响。
	if err := l.Put("op-legacy", "", "hash-4", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	removed, err := l.PruneMachineExcept("m-1", "op-del")
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, ok, _ := l.Check("op-create", "hash-1"); ok {
		t.Fatal("create record for deleted machine should be pruned")
	}
	if _, ok, _ := l.Check("op-del", "hash-3"); !ok {
		t.Fatal("delete record must be kept for its own dedup window")
	}
	if _, ok, _ := l.Check("op-other", "hash-2"); !ok {
		t.Fatal("records of other machines must be kept")
	}
	if _, ok, _ := l.Check("op-legacy", "hash-4"); !ok {
		t.Fatal("legacy records without machine_id must be kept")
	}

	// 重启后仍然生效。
	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l2.Check("op-create", "hash-1"); ok {
		t.Fatal("pruned record resurfaced after restart")
	}
}

// 年龄 GC：超过保留窗口的记录被清理，窗口内的保留（M1 评审 P2-5）。
func TestPruneBefore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := Record{
		OperationID: "op-old",
		MachineID:   "m-old",
		RequestHash: "hash-old",
		Result:      []byte(`{}`),
		CreatedAt:   time.Now().Add(-48 * time.Hour).UTC(),
	}
	l.records["op-old"] = old
	if err := l.Put("op-new", "m-new", "hash-new", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	removed, err := l.PruneBefore(time.Now().Add(-24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, ok, _ := l.Check("op-old", "hash-old"); ok {
		t.Fatal("expired record should be pruned")
	}
	if _, ok, _ := l.Check("op-new", "hash-new"); !ok {
		t.Fatal("record within retention window must be kept")
	}
}
