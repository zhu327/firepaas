package mutation

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/state"
)

func openProtocol(t *testing.T) (*Protocol, string, string) {
	t.Helper()
	dir := t.TempDir()
	lp, fp := filepath.Join(dir, "ledger.json"), filepath.Join(dir, "fences.json")
	l, err := state.Open(lp)
	if err != nil {
		t.Fatal(err)
	}
	f, err := state.OpenFences(fp)
	if err != nil {
		t.Fatal(err)
	}
	return New(l, f), lp, fp
}

func bytesCodec() Codec[json.RawMessage] {
	return Codec[json.RawMessage]{
		Encode: func(v json.RawMessage) (json.RawMessage, error) { return v, nil },
		Decode: func(v json.RawMessage) (json.RawMessage, error) { return v, nil },
	}
}

func TestCreateDeleteCopyAndRecoveryFamilies(t *testing.T) {
	t.Run("create advances before credential and completion", func(t *testing.T) {
		p, _, _ := openProtocol(t)
		id := Identity{OperationID: "create", MachineID: "m", ExecutionID: "e", Generation: 3, RequestHash: "h"}
		credential := false
		out, err := RunCreateMachine(p, CreateMachine[json.RawMessage]{
			Identity: id,
			Effect:   func() (json.RawMessage, error) { return []byte(`{"created":true}`), nil },
			PersistCredential: func() error {
				credential = true
				return p.fences.Check("m", 2)
			}, Codec: bytesCodec(),
		})
		if !errors.Is(err, ErrStale) || !credential || out != nil {
			t.Fatalf("create ordering: out=%s credential=%v err=%v", out, credential, err)
		}
		if _, complete, _ := p.ledger.Check("create", "h"); complete {
			t.Fatal("credential failure completed create")
		}
	})

	t.Run("delete and recovered delete", func(t *testing.T) {
		p, _, _ := openProtocol(t)
		id := Identity{OperationID: "delete", MachineID: "m", ExecutionID: "e", Generation: 2, RequestHash: "h"}
		calls := 0
		if _, err := RunDeleteMachine(p, DeleteMachine{Identity: id, Effect: func() error { calls++; return nil }}); err != nil ||
			calls != 1 {
			t.Fatalf("delete calls=%d err=%v", calls, err)
		}
		if _, err := RunDeleteMachine(p, DeleteMachine{Identity: id, Effect: func() error { calls++; return nil }}); err != nil ||
			calls != 1 {
			t.Fatalf("delete replay calls=%d err=%v", calls, err)
		}
		resourceID := Identity{OperationID: "resource-delete", MachineID: "v", RequestHash: "vh"}
		if _, _, err := p.ledger.Begin(state.Record{OperationID: resourceID.OperationID, MachineID: resourceID.MachineID, RequestHash: resourceID.RequestHash}); err != nil {
			t.Fatal(err)
		}
		if err := RunResourceDelete(p, DeleteMutation{Identity: resourceID, AlreadyDeleted: func() (bool, error) { return true, nil }, Effect: func() error { t.Fatal("reran delete"); return nil }}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("copy advances and replays", func(t *testing.T) {
		p, _, _ := openProtocol(t)
		id := Identity{OperationID: "copy", MachineID: "m", ExecutionID: "e", Generation: 4, RequestHash: "h"}
		calls := 0
		op := CopyTo[json.RawMessage]{
			Lifecycle: Lifecycle[json.RawMessage]{
				Identity: id,
				Effect:   func() (json.RawMessage, error) { calls++; return []byte(`{"copied":true}`), nil },
				Codec:    bytesCodec(),
			},
		}
		if _, err := RunCopyTo(p, op); err != nil {
			t.Fatal(err)
		}
		if _, err := RunCopyTo(p, op); err != nil || calls != 1 {
			t.Fatalf("copy calls=%d err=%v", calls, err)
		}
		if err := p.fences.Check("m", 3); !errors.Is(err, ErrStale) {
			t.Fatalf("copy fence=%v", err)
		}
	})
}

func TestReplacementCredentialFailureLeavesClaimRecoverable(t *testing.T) {
	p, lp, fp := openProtocol(t)
	id := Identity{OperationID: "replace-fail", MachineID: "m", ExecutionID: "e", Generation: 5, RequestHash: "h"}
	credentialErr := errors.New("credential persistence failed")
	_, err := RunReplacement(p, Replacement[json.RawMessage]{
		Identity:          id,
		Effect:            func() (json.RawMessage, error) { return []byte(`{"ok":true}`), nil },
		PersistCredential: func() error { return credentialErr }, Codec: bytesCodec(),
	})
	if !errors.Is(err, credentialErr) {
		t.Fatalf("credential error=%v", err)
	}
	ledger, _ := state.Open(lp)
	fences, _ := state.OpenFences(fp)
	if _, complete, _ := ledger.Check(id.OperationID, id.RequestHash); complete {
		t.Fatal("credential failure completed ledger")
	}
	if err := fences.Check("m", 4); !errors.Is(err, ErrStale) {
		t.Fatalf("fence was not durable: %v", err)
	}
	_, err = RunReplacement(New(ledger, fences), Replacement[json.RawMessage]{
		Identity: id,
		Recover: func() (Recovery[json.RawMessage], error) {
			return Recovery[json.RawMessage]{Value: []byte(`{"ok":true}`), Found: true}, nil
		},
		PersistCredential: func() error { return nil }, Codec: bytesCodec(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClaimedMutationCompletionFailureIsReturned(t *testing.T) {
	p, lp, _ := openProtocol(t)
	id := Identity{OperationID: "complete-fail", MachineID: "resource", RequestHash: "h"}
	_, err := RunResourceMutation(p, ClaimedMutation[json.RawMessage]{
		Identity: id,
		Effect: func() (json.RawMessage, error) {
			if err := os.Remove(lp); err != nil {
				return nil, err
			}
			if err := os.Mkdir(lp, 0o700); err != nil {
				return nil, err
			}
			return []byte(`{"ok":true}`), nil
		}, Codec: bytesCodec(),
	})
	if err == nil {
		t.Fatal("completion persistence failure was ignored")
	}
}

func TestLifecycleReplayConflictAndFence(t *testing.T) {
	p, _, _ := openProtocol(t)
	calls := 0
	id := Identity{OperationID: "op", MachineID: "m", ExecutionID: "e", Generation: 2, RequestHash: "hash"}
	out, err := RunLifecycle(
		p,
		Lifecycle[json.RawMessage]{
			Identity: id,
			Effect:   func() (json.RawMessage, error) { calls++; return []byte(`{"ok":true}`), nil },
			Codec:    bytesCodec(),
		},
	)
	if err != nil || calls != 1 || !json.Valid(out) {
		t.Fatalf("first: %s %d %v", out, calls, err)
	}
	_, err = RunLifecycle(
		p,
		Lifecycle[json.RawMessage]{
			Identity: id,
			Effect:   func() (json.RawMessage, error) { calls++; return nil, nil },
			Codec:    bytesCodec(),
		},
	)
	if err != nil || calls != 1 {
		t.Fatalf("replay reran effect: %d %v", calls, err)
	}
	id.RequestHash = "different"
	_, err = RunLifecycle(p, Lifecycle[json.RawMessage]{Identity: id, Codec: bytesCodec()})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
}

func TestClaimedMutationPersistsCompatibleClaimAndRecovers(t *testing.T) {
	p, lp, fp := openProtocol(t)
	id := Identity{
		OperationID: "op",
		MachineID:   "m",
		ExecutionID: "e",
		Generation:  7,
		Kind:        "snapshot.create",
		Coordinates: []byte(`{"snapshot_id":"s"}`),
		RequestHash: "hash",
	}
	effectErr := errors.New("crash window")
	_, err := RunResourceMutation(
		p,
		ClaimedMutation[json.RawMessage]{
			Identity: id,
			Effect:   func() (json.RawMessage, error) { return nil, effectErr },
			Codec:    bytesCodec(),
		},
	)
	if !errors.Is(err, effectErr) {
		t.Fatalf("effect=%v", err)
	}
	l, err := state.Open(lp)
	if err != nil {
		t.Fatal(err)
	}
	f, err := state.OpenFences(fp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RunResourceMutation(
		New(l, f),
		ClaimedMutation[json.RawMessage]{Identity: id, Recover: func() (Recovery[json.RawMessage], error) {
			return Recovery[json.RawMessage]{Value: []byte(`{"done":true}`), Found: true}, nil
		}, Codec: bytesCodec()},
	)
	if err != nil || string(got) != `{"done":true}` {
		t.Fatalf("recover=%s %v", got, err)
	}
	rec, ok, err := l.Get("op", "hash")
	if err != nil || !ok || !rec.Completed() || rec.Kind != id.Kind {
		t.Fatalf("record=%#v %v %v", rec, ok, err)
	}
	var a, b any
	_ = json.Unmarshal(rec.Identity, &a)
	_ = json.Unmarshal(id.Coordinates, &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("identity changed")
	}
}

func TestReplacementRecoveryAfterReopenAdvancesFenceBeforeCredentialAndCompletion(t *testing.T) {
	p, lp, fp := openProtocol(t)
	id := Identity{
		OperationID: "replace",
		MachineID:   "m",
		ExecutionID: "new-exec",
		Generation:  9,
		Kind:        "snapshot.restore",
		RequestHash: "hash",
	}
	crash := errors.New("crash after replacement")
	_, err := RunReplacement(p, Replacement[json.RawMessage]{
		Identity: id,
		Effect:   func() (json.RawMessage, error) { return nil, crash }, Codec: bytesCodec(),
	})
	if !errors.Is(err, crash) {
		t.Fatalf("first effect: %v", err)
	}

	ledger, err := state.Open(lp)
	if err != nil {
		t.Fatal(err)
	}
	fences, err := state.OpenFences(fp)
	if err != nil {
		t.Fatal(err)
	}
	credentialCalled := false
	got, err := RunReplacement(New(ledger, fences), Replacement[json.RawMessage]{
		Identity: id,
		Recover: func() (Recovery[json.RawMessage], error) {
			return Recovery[json.RawMessage]{Value: []byte(`{"recovered":true}`), Found: true}, nil
		},
		Effect: func() (json.RawMessage, error) { t.Fatal("recovered replacement reran effect"); return nil, nil },
		PersistCredential: func() error {
			credentialCalled = true
			if err := fences.Check(id.MachineID, id.Generation-1); !errors.Is(err, ErrStale) {
				t.Fatalf("credential persisted before durable fence advance: %v", err)
			}
			reopened, err := state.OpenFences(fp)
			if err != nil {
				return err
			}
			if err := reopened.Check(id.MachineID, id.Generation-1); !errors.Is(err, ErrStale) {
				t.Fatalf("reopened fence did not reject old generation: %v", err)
			}
			if _, complete, err := ledger.Check(id.OperationID, id.RequestHash); err != nil || complete {
				t.Fatalf("ledger completed before credential: complete=%v err=%v", complete, err)
			}
			return nil
		}, Codec: bytesCodec(),
	})
	if err != nil || !credentialCalled || string(got) != `{"recovered":true}` {
		t.Fatalf("recovery=%s credential=%v err=%v", got, credentialCalled, err)
	}
	if _, complete, err := ledger.Check(id.OperationID, id.RequestHash); err != nil || !complete {
		t.Fatalf("ledger not completed last: complete=%v err=%v", complete, err)
	}
}

func TestLegacyCompletedLedgerReplaysWithoutMigration(t *testing.T) {
	d := t.TempDir()
	lp := filepath.Join(d, "ledger.json")
	legacy := `{"old":{"operation_id":"old","machine_id":"m","request_hash":"hash","result":{"legacy":true},"created_at":"2026-08-01T00:00:00Z"}}`
	if err := os.WriteFile(lp, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	l, _ := state.Open(lp)
	f, _ := state.OpenFences(filepath.Join(d, "fences.json"))
	calls := 0
	out, err := RunLifecycle(
		New(l, f),
		Lifecycle[json.RawMessage]{
			Identity: Identity{OperationID: "old", MachineID: "m", RequestHash: "hash"},
			Effect:   func() (json.RawMessage, error) { calls++; return nil, nil },
			Codec:    bytesCodec(),
		},
	)
	if err != nil || calls != 0 || string(out) != `{"legacy":true}` {
		t.Fatalf("legacy=%s calls=%d err=%v", out, calls, err)
	}
}

func TestExecTombstoneNonReattachable(t *testing.T) {
	p, _, _ := openProtocol(t)
	id := Identity{OperationID: "exec", MachineID: "m", RequestHash: "hash"}
	if ok, err := ClaimExec(p, id, []byte(`{"status":"created"}`)); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := ClaimExec(p, id, []byte(`{}`)); err != nil || ok {
		t.Fatal(ok, err)
	}
}
