package state

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLedgerPutCheckReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "hash-1", []byte(`{"machine_id":"m"}`)); err != nil {
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
	if err := l.Put("op-1", "hash-1", []byte(`{"machine_id":"m"}`)); err != nil {
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

func TestLedgerConflictingHashRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "hash-1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Put("op-1", "hash-2", []byte(`{}`)); !errors.Is(err, ErrRequestHashConflict) {
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
	if err := l.Put("op-1", "hash-1", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	// 第二次相同 hash 不覆盖首次结果。
	if err := l.Put("op-1", "hash-1", []byte(`{"n":2}`)); err != nil {
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
