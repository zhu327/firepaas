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

// P0#1：create 崩溃窗口（Effect 已生效、Complete 未落盘）的重试恢复——
// 不透明地重跑 Effect 会撞 hypeman 同名拒绝而永久失败；durable-claim 模型
// 必须改为 inventory 认领或干净重跑。
func TestCreateCrashWindowRecovery(t *testing.T) {
	t.Run("claim with existing instance is adopted", func(t *testing.T) {
		p, lp, fp := openProtocol(t)
		id := Identity{OperationID: "op", MachineID: "m", ExecutionID: "e", Generation: 3, RequestHash: "h"}
		credentialErr := errors.New("credential persistence failed")
		effectCalls := 0
		_, err := RunCreateMachine(p, CreateMachine[json.RawMessage]{
			Identity:          id,
			Effect:            func() (json.RawMessage, error) { effectCalls++; return []byte(`{"created":true}`), nil },
			PersistCredential: func() error { return credentialErr },
			Codec:             bytesCodec(),
		})
		if !errors.Is(err, credentialErr) || effectCalls != 1 {
			t.Fatalf("first create: calls=%d err=%v", effectCalls, err)
		}
		// 崩溃点：claim 存活（未完成）；fence 已随 Advance 落盘到 3（顺序与
		// legacy 一致：advance 先于 credential——见 create advances before
		// credential and completion），credential 失败留下可恢复窗口。
		l1, err := state.Open(lp)
		if err != nil {
			t.Fatal(err)
		}
		f1, err := state.OpenFences(fp)
		if err != nil {
			t.Fatal(err)
		}
		rec, ok, err := l1.Get("op", "h")
		if err != nil || !ok || rec.Completed() {
			t.Fatalf("claim=%#v ok=%v err=%v", rec, ok, err)
		}
		if err := f1.Check("m", 2); !errors.Is(err, ErrStale) {
			t.Fatalf("fence advance must be durable before credential: %v", err)
		}
		// 重启后同 operation_id 重试：inventory 里找到实例 → 认领完成，
		// Effect 绝不重跑；fence 必须已先于 credential 推进到 3。
		credential := false
		out, err := RunCreateMachine(New(l1, f1), CreateMachine[json.RawMessage]{
			Identity: id,
			Recover: func() (Recovery[json.RawMessage], error) {
				return Recovery[json.RawMessage]{Value: []byte(`{"adopted":true}`), Found: true}, nil
			},
			Effect: func() (json.RawMessage, error) {
				t.Fatal("adopted claim reran effect")
				return nil, nil
			},
			PersistCredential: func() error {
				credential = true
				if err := f1.Check("m", 2); !errors.Is(err, ErrStale) {
					t.Fatalf("fence not advanced before credential on adopt: %v", err)
				}
				return nil
			},
			Codec: bytesCodec(),
		})
		if err != nil || !credential || string(out) != `{"adopted":true}` {
			t.Fatalf("adopt: out=%s credential=%v err=%v", out, credential, err)
		}
		if _, complete, _ := l1.Check("op", "h"); !complete {
			t.Fatal("adopted create not completed in ledger")
		}
		// 认领后的重放返回记录结果（不再触碰 Recover/Effect）。
		replayed, err := RunCreateMachine(New(l1, f1), CreateMachine[json.RawMessage]{
			Identity: id,
			Recover: func() (Recovery[json.RawMessage], error) {
				t.Fatal("replay reran recover")
				return Recovery[json.RawMessage]{}, nil
			},
			Effect:            func() (json.RawMessage, error) { t.Fatal("replay reran effect"); return nil, nil },
			PersistCredential: func() error { return nil },
			Codec:             bytesCodec(),
		})
		if err != nil || string(replayed) != `{"adopted":true}` {
			t.Fatalf("replay after adopt: %s %v", replayed, err)
		}
	})

	t.Run("claim without instance reruns effect", func(t *testing.T) {
		p, lp, fp := openProtocol(t)
		id := Identity{OperationID: "op", MachineID: "m", ExecutionID: "e", Generation: 1, RequestHash: "h"}
		effectErr := errors.New("hypeman create failed")
		_, err := RunCreateMachine(p, CreateMachine[json.RawMessage]{
			Identity: id,
			Effect:   func() (json.RawMessage, error) { return nil, effectErr },
			Codec:    bytesCodec(),
		})
		if !errors.Is(err, effectErr) {
			t.Fatalf("first effect: %v", err)
		}
		l1, _ := state.Open(lp)
		f1, _ := state.OpenFences(fp)
		if rec, ok, err := l1.Get("op", "h"); err != nil || !ok || rec.Completed() {
			t.Fatalf("failed effect must leave an unfinished claim: rec=%#v ok=%v err=%v", rec, ok, err)
		}
		// 未完成的 claim 不得越过 fence（高水位只在成功完成序列中推进）。
		if err := f1.Check("m", 0); err != nil {
			t.Fatalf("unfinished claim advanced the fence: %v", err)
		}
		effectCalls := 0
		out, err := RunCreateMachine(New(l1, f1), CreateMachine[json.RawMessage]{
			Identity: id,
			Recover:  func() (Recovery[json.RawMessage], error) { return Recovery[json.RawMessage]{Found: false}, nil },
			Effect: func() (json.RawMessage, error) {
				effectCalls++
				return []byte(`{"created":true}`), nil
			},
			Codec: bytesCodec(),
		})
		if err != nil || effectCalls != 1 || string(out) != `{"created":true}` {
			t.Fatalf("re-effect: out=%s calls=%d err=%v", out, effectCalls, err)
		}
		if _, complete, _ := l1.Check("op", "h"); !complete {
			t.Fatal("re-ran create not completed")
		}
	})

	t.Run("completed claim replays result and re-persists credential", func(t *testing.T) {
		p, _, _ := openProtocol(t)
		id := Identity{OperationID: "op", MachineID: "m", ExecutionID: "e", Generation: 1, RequestHash: "h"}
		credentialCalls := 0
		op := CreateMachine[json.RawMessage]{
			Identity:          id,
			Effect:            func() (json.RawMessage, error) { return []byte(`{"ok":true}`), nil },
			PersistCredential: func() error { credentialCalls++; return nil },
			Codec:             bytesCodec(),
		}
		if _, err := RunCreateMachine(p, op); err != nil || credentialCalls != 1 {
			t.Fatalf("first create: credentials=%d err=%v", credentialCalls, err)
		}
		op.Recover = func() (Recovery[json.RawMessage], error) {
			t.Fatal("replay reran recover")
			return Recovery[json.RawMessage]{}, nil
		}
		op.Effect = func() (json.RawMessage, error) { t.Fatal("replay reran effect"); return nil, nil }
		out, err := RunCreateMachine(p, op)
		if err != nil || string(out) != `{"ok":true}` || credentialCalls != 2 {
			t.Fatalf("replay: out=%s credentials=%d err=%v", out, credentialCalls, err)
		}
	})

	t.Run("stale generation is rejected before any claim", func(t *testing.T) {
		p, _, _ := openProtocol(t)
		if _, err := RunCreateMachine(p, CreateMachine[json.RawMessage]{
			Identity: Identity{OperationID: "op-new", MachineID: "m", ExecutionID: "e", Generation: 5, RequestHash: "h5"},
			Effect:   func() (json.RawMessage, error) { return []byte(`{"ok":true}`), nil },
			Codec:    bytesCodec(),
		}); err != nil {
			t.Fatal(err)
		}
		_, err := RunCreateMachine(p, CreateMachine[json.RawMessage]{
			Identity: Identity{
				OperationID: "op-stale",
				MachineID:   "m",
				ExecutionID: "e",
				Generation:  3,
				RequestHash: "h3",
			},
			Effect: func() (json.RawMessage, error) { t.Fatal("stale create ran effect"); return nil, nil },
			Codec:  bytesCodec(),
		})
		if !errors.Is(err, ErrStale) {
			t.Fatalf("stale create: %v", err)
		}
		if _, ok, err := p.ledger.Get("op-stale", "h3"); err != nil || ok {
			t.Fatalf("rejected stale create left a claim: ok=%v err=%v", ok, err)
		}
	})

	t.Run("legacy completed record replays and re-persists credential", func(t *testing.T) {
		d := t.TempDir()
		lp := filepath.Join(d, "ledger.json")
		legacy := `{"op":{"operation_id":"op","machine_id":"m","request_hash":"h","result":{"legacy":true},"created_at":"2026-08-01T00:00:00Z"}}`
		if err := os.WriteFile(lp, []byte(legacy), 0o600); err != nil {
			t.Fatal(err)
		}
		l, _ := state.Open(lp)
		f, _ := state.OpenFences(filepath.Join(d, "fences.json"))
		credential := false
		out, err := RunCreateMachine(New(l, f), CreateMachine[json.RawMessage]{
			Identity:          Identity{OperationID: "op", MachineID: "m", RequestHash: "h"},
			Effect:            func() (json.RawMessage, error) { t.Fatal("legacy replay reran effect"); return nil, nil },
			PersistCredential: func() error { credential = true; return nil },
			Codec:             bytesCodec(),
		})
		if err != nil || !credential || string(out) != `{"legacy":true}` {
			t.Fatalf("legacy replay: out=%s credential=%v err=%v", out, credential, err)
		}
	})
}

// TestLifecycleClaimedCrashWindowConverges：R2 崩溃窗口——pause claim 在途
// （Effect 完成后崩溃）而实例已达目标态；重试必须经 Recover 收敛为成功，
// 不重跑 Effect，且 fence 推进先于 durable 完成。同 op 重放幂等。
func TestLifecycleClaimedCrashWindowConverges(t *testing.T) {
	p, lp, fp := openProtocol(t)
	id := Identity{OperationID: "pause", MachineID: "m", ExecutionID: "e", Generation: 2, RequestHash: "h"}
	crash := errors.New("crash after effect, before completion")
	effectCalls := 0
	_, err := RunLifecycle(p, Lifecycle[json.RawMessage]{
		Identity: id,
		Effect:   func() (json.RawMessage, error) { effectCalls++; return nil, crash },
		Codec:    bytesCodec(),
	})
	if !errors.Is(err, crash) || effectCalls != 1 {
		t.Fatalf("first effect: calls=%d err=%v", effectCalls, err)
	}
	// 模拟 agent 重启：ledger/fences 从盘重开。
	l, err := state.Open(lp)
	if err != nil {
		t.Fatal(err)
	}
	f, err := state.OpenFences(fp)
	if err != nil {
		t.Fatal(err)
	}
	// claim 在途 + 实例已 PAUSED → Recover 收敛成功，Effect 不重跑。
	out, err := RunLifecycle(New(l, f), Lifecycle[json.RawMessage]{
		Identity: id,
		Recover: func() (Recovery[json.RawMessage], error) {
			return Recovery[json.RawMessage]{Value: []byte(`{"state":"PAUSED"}`), Found: true}, nil
		},
		Effect: func() (json.RawMessage, error) {
			t.Fatal("converged claim reran effect")
			return nil, nil
		},
		Codec: bytesCodec(),
	})
	if err != nil || string(out) != `{"state":"PAUSED"}` {
		t.Fatalf("converge: out=%s err=%v", out, err)
	}
	if _, complete, _ := l.Check("pause", "h"); !complete {
		t.Fatal("converged lifecycle not durably completed")
	}
	// fence 已推进：更旧的 generation 被拒；同代仍放行。
	if err := f.Check("m", 1); !errors.Is(err, ErrStale) {
		t.Fatalf("lifecycle fence not advanced: %v", err)
	}
	// 同 op 重放：返回记录结果，不再触碰 Recover/Effect。
	replay, err := RunLifecycle(New(l, f), Lifecycle[json.RawMessage]{
		Identity: id,
		Recover: func() (Recovery[json.RawMessage], error) {
			t.Fatal("replay reran recover")
			return Recovery[json.RawMessage]{}, nil
		},
		Effect: func() (json.RawMessage, error) { t.Fatal("replay reran effect"); return nil, nil },
		Codec:  bytesCodec(),
	})
	if err != nil || string(replay) != `{"state":"PAUSED"}` {
		t.Fatalf("replay: out=%s err=%v", replay, err)
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
