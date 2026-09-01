package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// v1.2-B（ADR-0024）：secret delivery lease 状态机。
// ISSUED → CLAIMED → DELIVERED → ACKED | EXPIRED | REVOKED；
// 全部转换 CAS；同 machine/execution 单历史 lease；完整绑定冲突拒绝。

// cleanLeaseRows 清理共享测试库中这些 execution 的残留 lease 行
// （ID 含随机段，按 execution 删）。
func cleanLeaseRows(t *testing.T, s *Store, execs ...string) {
	t.Helper()
	for _, e := range execs {
		_, _ = s.pool.Exec(context.Background(),
			`DELETE FROM secret_delivery_leases WHERE execution_id=$1`, e)
	}
}

func issueTestLease(t *testing.T, s *Store, project, machine, exec, opID, hash string) *SecretLease {
	t.Helper()
	l, err := s.EnsureSecretLease(context.Background(), project, machine, exec, 2, opID, hash, time.Minute)
	if err != nil {
		t.Fatalf("issue lease: %v", err)
	}
	if l.State != SecretLeaseIssued {
		t.Fatalf("fresh lease state: %s", l.State)
	}
	return l
}

// 短 execution ID 也必须安全生成 lease ID，不能切片越界 panic。
func TestEnsureSecretLeaseShortExecutionID(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl-short-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	cleanLeaseRows(t, s, "x")

	lease, err := s.EnsureSecretLease(ctx, project, "m-short", "x", 1, "op-short", "hash-short", time.Minute)
	if err != nil {
		t.Fatalf("short execution ID must not panic or fail: %v", err)
	}
	if lease.ExecutionID != "x" {
		t.Fatalf("execution ID = %q, want x", lease.ExecutionID)
	}
}

// 签发幂等只接受完整绑定一致；同 execution 的其它请求一律冲突。
func TestEnsureSecretLeaseReuseAndConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-1", "exec-sdl-1"
	cleanLeaseRows(t, s, exec)

	l1 := issueTestLease(t, s, project, machine, exec, "op-a", "hash-1")
	// 完整绑定相同 → 幂等复用。
	l2, err := s.EnsureSecretLease(ctx, project, machine, exec, 2, "op-a", "hash-1", time.Minute)
	if err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if l1.ID != l2.ID {
		t.Fatalf("expected reuse, got %s vs %s", l1.ID, l2.ID)
	}
	for _, tc := range []struct{ op, hash string }{{"op-b", "hash-1"}, {"op-a", "hash-2"}} {
		if _, err := s.EnsureSecretLease(ctx, project, machine, exec, 2, tc.op, tc.hash, time.Minute); !errors.Is(
			err,
			ErrSecretLeaseConflict,
		) {
			t.Fatalf("binding mismatch must conflict, got %v", err)
		}
	}
	if _, err := s.pool.Exec(ctx, `UPDATE secret_delivery_leases SET state='EXPIRED' WHERE id=$1`, l1.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureSecretLease(ctx, project, machine, exec, 2, "op-new", "hash-new", time.Minute); !errors.Is(
		err,
		ErrSecretLeaseConflict,
	) {
		t.Fatalf("terminal history must prohibit reissue, got %v", err)
	}
}

// 不确定结果必须与 create 终态化、fenced reap 原子持久化。
func TestSecretCreateUncertainAtomicallyEnqueuesCleanup(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-uncertain-%d", time.Now().UnixNano())
	machine, exec, opID := "m-uncertain", "exec-uncertain", "op-uncertain"
	cleanLeaseRows(t, s, exec)
	_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE id IN ($1,$2)`, opID, "op-secret-cleanup-"+opID)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	seedMachine(t, s, project, "app-uncertain", machine, "dep-uncertain", exec)
	raw := ensureCreateRequest(t, machine, "dep-uncertain", exec, opID)
	if _, err := s.pool.Exec(ctx, `INSERT INTO operations(id,project_id,machine_id,execution_id,generation,kind,idempotency_key,status,request,dispatch_node_id)
		VALUES($1,$2,$3,$4,2,'create',$1,'CLAIMED',$5::jsonb,'node-1')`, opID, project, machine, exec, string(raw)); err != nil {
		t.Fatal(err)
	}
	lease := issueTestLease(t, s, project, machine, exec, opID, hashBytes(raw))
	if err := s.ClaimSecretLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	cleanupID := "op-secret-cleanup-" + opID
	cleanup := []byte(
		`{"machine_id":"m-uncertain","execution_id":"exec-uncertain","generation":"2","operation_id":"` + cleanupID + `"}`,
	)
	op := Operation{
		ID: opID, ProjectID: project, MachineID: machine, ExecutionID: exec,
		Generation: 2, DispatchNodeID: "node-1",
	}
	if err := s.MarkSecretCreateUncertainAndEnqueueCleanup(ctx, op, lease, cleanupID, cleanup, "ambiguous"); err != nil {
		t.Fatal(err)
	}
	// Leader recovery/re-entry is idempotent and does not duplicate cleanup.
	if err := s.MarkSecretCreateUncertainAndEnqueueCleanup(ctx, op, lease, cleanupID, cleanup, "ambiguous"); err != nil {
		t.Fatal(err)
	}
	gotLease, err := s.SecretLeaseForExecution(ctx, machine, exec)
	if err != nil || gotLease.State != SecretLeaseUncertain {
		t.Fatalf("lease state = %v, err=%v; want UNCERTAIN", gotLease, err)
	}
	var createStatus, cleanupStatus, kind, node string
	if err := s.pool.QueryRow(ctx, `SELECT status FROM operations WHERE id=$1`, opID).Scan(&createStatus); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT status,kind,coalesce(dispatch_node_id,'') FROM operations WHERE id=$1`, cleanupID).
		Scan(&cleanupStatus, &kind, &node); err != nil {
		t.Fatal(err)
	}
	if createStatus != "FAILED" || cleanupStatus != "PENDING" || kind != "reap" || node != "node-1" {
		t.Fatalf("create=%s cleanup=%s kind=%s node=%s", createStatus, cleanupStatus, kind, node)
	}
	if err := s.ClaimSecretLease(ctx, gotLease); !errors.Is(err, ErrSecretLeaseTerminal) {
		t.Fatalf("UNCERTAIN lease became dispatchable: %v", err)
	}
}

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestSecretLeaseLifecycleTransitions(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl2-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-2", "exec-sdl-2"
	cleanLeaseRows(t, s, exec)

	l := issueTestLease(t, s, project, machine, exec, "op-x", "hash-x")
	if err := s.ClaimSecretLease(ctx, l); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// CLAIMED 是 RPC 前持久 fence，不允许同 execution 再派。
	claimed, err := s.SecretLeaseForExecution(ctx, machine, exec)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimSecretLease(ctx, claimed); !errors.Is(err, ErrSecretLeaseTerminal) {
		t.Fatalf("re-claim must be terminal: %v", err)
	}
	if err := s.MarkSecretLeaseDelivered(ctx, l); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if err := s.MarkSecretLeaseAcked(ctx, l); err != nil {
		t.Fatalf("ack: %v", err)
	}
	got, err := s.SecretLeaseForExecution(ctx, machine, exec)
	if err != nil || got.State != SecretLeaseAcked {
		t.Fatalf("final state: %v %+v", err, got)
	}
	// ACK 幂等。
	if err := s.MarkSecretLeaseAcked(ctx, l); err != nil {
		t.Fatalf("re-ack: %v", err)
	}
	// 绑定校验：任一字段错误都返回 typed transition error。
	wrong := *l
	wrong.MachineID = "other-machine"
	if err := s.MarkSecretLeaseAcked(ctx, &wrong); !errors.Is(err, ErrSecretLeaseTransition) {
		t.Fatalf("ack with wrong binding must return typed error, got %v", err)
	}
}

// CAS 零行（不存在）必须返回 typed error。
func TestSecretLeaseMissingTransitionRejected(t *testing.T) {
	s := testStore(t)
	missing := &SecretLease{
		ID: "sdl-missing", ProjectID: "p", MachineID: "m", ExecutionID: "e",
		Generation: 1, OperationID: "op", RequestHash: "hash",
	}
	if err := s.ClaimSecretLease(context.Background(), missing); !errors.Is(err, ErrSecretLeaseTransition) {
		t.Fatalf("missing lease must return typed error, got %v", err)
	}
}

func TestSecretLeaseExpiredTransitionRejected(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl-exp-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-exp", "exec-sdl-exp"
	cleanLeaseRows(t, s, exec)

	l := issueTestLease(t, s, project, machine, exec, "op-exp", "hash-exp")
	if _, err := s.pool.Exec(ctx, `UPDATE secret_delivery_leases SET expires_at=now()-interval '1 second' WHERE id=$1`, l.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimSecretLease(ctx, l); !errors.Is(err, ErrSecretLeaseTransition) {
		t.Fatalf("expired claim must return typed error, got %v", err)
	}
	if err := s.MarkSecretLeaseDelivered(ctx, l); !errors.Is(err, ErrSecretLeaseTransition) {
		t.Fatalf("expired delivery must return typed error, got %v", err)
	}
	if err := s.MarkSecretLeaseAcked(ctx, l); !errors.Is(err, ErrSecretLeaseTransition) {
		t.Fatalf("expired ack must return typed error, got %v", err)
	}
}

// ACK 直达（R8 补账）：Create 成功但控制器未及标 DELIVERED，observed ACKED
// 必须能把 ISSUED lease 推到 ACKED。
func TestSecretLeaseAckSkipsIntermediateStates(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl3-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-3", "exec-sdl-3"
	cleanLeaseRows(t, s, exec)

	l := issueTestLease(t, s, project, machine, exec, "op-y", "hash-y")
	if err := s.MarkSecretLeaseAcked(ctx, l); err != nil {
		t.Fatalf("direct ack from ISSUED: %v", err)
	}
}

// 单历史 lease：同 machine/execution 插入第二条终态或活跃行都被拒绝。
func TestSecretLeaseSingleActivePerExecution(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl4-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-4", "exec-sdl-4"
	cleanLeaseRows(t, s, exec)

	l := issueTestLease(t, s, project, machine, exec, "op-z", "hash-z")
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO secret_delivery_leases(id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at)
		 VALUES('sdl-dup', $1, $2, $3, 1, 'op-dup', 'hash-dup', 'EXPIRED', now())`,
		project, machine, exec); err == nil {
		_ = l
		t.Fatal("second historical lease for same machine/execution must violate unique index")
	}
}

// 回收：超时 → EXPIRED；execution 换代 → REVOKED。
func TestReapSecretLeases(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl5-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	// 共享测试库防残留：按 execution/id 清掉旧运行留下的行。
	cleanLeaseRows(t, s, "exec-stale", "exec-old")
	for _, id := range []string{"sdl-stale", "sdl-old"} {
		_, _ = s.pool.Exec(ctx, `DELETE FROM secret_delivery_leases WHERE id=$1`, id)
	}
	for _, m := range []string{"m-live", "m-old"} {
		_, _ = s.pool.Exec(ctx, `DELETE FROM machines WHERE id=$1`, m)
	}

	// 机器当前 execution = exec-live；lease 挂在 exec-stale 上 → REVOKED。
	seedMachine(t, s, project, "app-sdl5", "m-live", "dep-1", "exec-live")
	if _, err := s.pool.Exec(ctx, `INSERT INTO secret_delivery_leases
		(id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at)
		VALUES('sdl-stale',$1,'m-live','exec-stale',1,'op-1','h1','ISSUED',now()+interval '5 minutes')`,
		project); err != nil {
		t.Fatal(err)
	}
	// 已过期 lease：machine 仍持有该 execution（不触发 revoke），但
	// expires_at 已过 → EXPIRED。
	seedMachine(t, s, project, "app-sdl5b", "m-old", "dep-1", "exec-old")
	if _, err := s.pool.Exec(ctx, `INSERT INTO secret_delivery_leases
		(id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at)
		VALUES('sdl-old',$1,'m-old','exec-old',1,'op-2','h2','CLAIMED',now()-interval '1 minute')`,
		project); err != nil {
		t.Fatal(err)
	}

	// 共享库可能有其它残留 lease（会被一并 revoke），只断言本用例的两行。
	_, _, err := s.ReapSecretLeases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sdl-stale", "sdl-old"} {
		l, err := s.SecretLeaseByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		want := SecretLeaseRevoked
		if id == "sdl-old" {
			want = SecretLeaseExpired
		}
		if l.State != want {
			t.Fatalf("%s state: %s, want %s", id, l.State, want)
		}
	}
	// 幂等：目标行已终态，不再变化。
	if _, _, err := s.ReapSecretLeases(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sdl-stale", "sdl-old"} {
		l, err := s.SecretLeaseByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if l.State == SecretLeaseIssued || l.State == SecretLeaseClaimed || l.State == SecretLeaseDelivered {
			t.Fatalf("%s re-reaped from terminal: %s", id, l.State)
		}
	}
}

// MachineHasActiveSecretDelivery：pause 防护的查询面。
func TestMachineHasActiveSecretDelivery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl6-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	cleanLeaseRows(t, s, "exec-guard")
	_, _ = s.pool.Exec(ctx, `DELETE FROM secret_delivery_leases WHERE id=$1`, "sdl-guard")

	if _, err := s.pool.Exec(ctx, `INSERT INTO secret_delivery_leases
		(id, project_id, machine_id, execution_id, generation, operation_id, request_hash, state, expires_at)
		VALUES('sdl-guard',$1,'m-guard','exec-guard',1,'op-g','hg','ACKED',now()+interval '5 minutes')`,
		project); err != nil {
		t.Fatal(err)
	}
	has, err := s.MachineHasActiveSecretDelivery(ctx, "m-guard", "exec-guard")
	if err != nil || !has {
		t.Fatalf("ack lease must count as active delivery: %v %v", has, err)
	}
	has, err = s.MachineHasActiveSecretDelivery(ctx, "m-guard", "exec-other")
	if err != nil || has {
		t.Fatalf("other execution must not match: %v %v", has, err)
	}
}

// v1.2-B review（P1）：终态/过期 lease 的 Claim 必须返回
// ErrSecretLeaseTerminal——上层据此终态化并换 execution，不得 requeue 死循环。
func TestClaimSecretLeaseTerminalStates(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl7-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-7", "exec-sdl-7"
	cleanLeaseRows(t, s, exec)

	l := issueTestLease(t, s, project, machine, exec, "op-t1", "hash-t1")
	// 直接置终态后 claim → terminal 错误。
	for _, st := range []string{"CLAIMED", "DELIVERED", "UNCERTAIN", "EXPIRED", "REVOKED", "ACKED"} {
		if _, err := s.pool.Exec(ctx,
			`UPDATE secret_delivery_leases SET state=$1 WHERE id=$2`, st, l.ID); err != nil {
			t.Fatal(err)
		}
		got, _ := s.SecretLeaseForExecution(ctx, machine, exec)
		if err := s.ClaimSecretLease(ctx, got); !errors.Is(err, ErrSecretLeaseTerminal) {
			t.Fatalf("claim after %s: want ErrSecretLeaseTerminal, got %v", st, err)
		}
	}
	// 活跃但已过期 → terminal。
	if _, err := s.pool.Exec(ctx,
		`UPDATE secret_delivery_leases SET state='ISSUED', expires_at=now()-interval '1 minute' WHERE id=$1`, l.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := s.SecretLeaseForExecution(ctx, machine, exec)
	if err := s.ClaimSecretLease(ctx, got); !errors.Is(err, ErrSecretLeaseTerminal) {
		t.Fatalf("claim expired: want ErrSecretLeaseTerminal, got %v", err)
	}
	// 活跃未过期 → 正常 CLAIMED。
	if _, err := s.pool.Exec(ctx,
		`UPDATE secret_delivery_leases SET expires_at=now()+interval '5 minutes' WHERE id=$1`, l.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = s.SecretLeaseForExecution(ctx, machine, exec)
	if err := s.ClaimSecretLease(ctx, got); err != nil {
		t.Fatalf("claim active: %v", err)
	}
}

// v1.2-B review（P2）：pause 防护含终态 lease（EXPIRED 也可能已写入 guest）。
func TestMachineHasActiveSecretDeliveryIncludesTerminal(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-sdl8-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	machine, exec := "m-sdl-8", "exec-sdl-8"
	cleanLeaseRows(t, s, exec)

	l := issueTestLease(t, s, project, machine, exec, "op-t2", "hash-t2")
	if _, err := s.pool.Exec(ctx,
		`UPDATE secret_delivery_leases SET state='EXPIRED' WHERE id=$1`, l.ID); err != nil {
		t.Fatal(err)
	}
	has, err := s.MachineHasActiveSecretDelivery(ctx, machine, exec)
	if err != nil || !has {
		t.Fatalf("expired lease must still guard memory snapshot: %v %v", has, err)
	}
}
