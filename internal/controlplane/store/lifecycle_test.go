package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// seedMachine 为 store 测试创建最小 app+machine 行。
func seedMachine(t *testing.T, s *Store, project, appID, machineID, deploymentID, executionID string) {
	t.Helper()
	ctx := context.Background()
	if err := s.EnsureProject(ctx, project, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
		VALUES($1,$2,$3,'img',1,512,1,1) ON CONFLICT (id) DO NOTHING`,
		appID, project, appID+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
			desired_state, generation, current_execution_id, requested_vcpu,
			requested_mem_mib, image_ref, node_id, ingress_port)
		VALUES($1,$2,$3,0,$4,'CREATED',1,$5,1,512,'img','',8080)
		ON CONFLICT (id) DO NOTHING`,
		machineID, appID, deploymentID, machineID+".test", executionID); err != nil {
		t.Fatal(err)
	}
}

// ensureCreateRequest 构造一个最小 create 请求 JSON。
func ensureCreateRequest(t *testing.T, machineID, deploymentID, executionID, opID string) []byte {
	t.Helper()
	req := &pb.CreateMachineRequest{
		MachineId: machineID, Generation: 2, OperationId: opID,
		Spec: &pb.MachineSpec{
			ProjectId: "dev", AppId: "app-" + machineID, DeploymentId: deploymentID,
			ExecutionId: executionID, ImageRef: "img", Vcpu: 1, MemMib: 512,
		},
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestResetRestartAttemptsNotFound(t *testing.T) {
	s := testStore(t)
	if err := s.ResetRestartAttempts(context.Background(), "missing-restart-reset"); !errors.Is(err, ErrMachineNotFound) {
		t.Fatalf("want ErrMachineNotFound, got %v", err)
	}
}

// P1（v1.2-D review）：stable window 必须随 restart attempt 重新锚定。
// 旧行为：RecordRestartAttempt 不清 restart_stable_since，旧 execution 的
// READY 锚点跨 restart 存活 → attempts 被提前清零 → restart storm。
func TestRecordRestartAttemptResetsStableWindow(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-stable-%d", os.Getpid())
	t.Cleanup(func() { cleanupProject(t, s, project) })
	cleanupProject(t, s, project)

	machineID := "m-stable-window"
	seedMachine(t, s, project, "app-stable", machineID, "dep-stable", "exec-old")
	// 模拟旧 execution 已 READY：锚点在很久以前。
	oldAnchor := time.Now().Add(-2 * time.Hour)
	if err := s.SetRestartStableSince(ctx, machineID, &oldAnchor); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordRestartAttempt(ctx, machineID, 1, time.Now().Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMachine(ctx, machineID)
	if err != nil || m == nil {
		t.Fatal("get machine after restart attempt")
	}
	if m.RestartStableSince != nil {
		t.Fatalf("restart_stable_since must be NULL after RecordRestartAttempt (new execution not READY yet), got %v", *m.RestartStableSince)
	}
	if m.RestartAttempts != 1 {
		t.Fatalf("restart_attempts = %d, want 1", m.RestartAttempts)
	}
}

// P2（v1.2-D review）：并发 delete 与 restart/recreate 的复活竞态。
// EnsureAppAndEnqueueCreate 对 DELETED 行必须拒绝（返回 ErrMachineLifecycleClosed）
// 而不是无条件复活为 CREATED + 新 execution。
func TestEnsureCreateRejectsDeletedMachine(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-delrace-%d", os.Getpid())
	t.Cleanup(func() { cleanupProject(t, s, project) })
	cleanupProject(t, s, project)

	machineID := "m-del-race"
	seedMachine(t, s, project, "app-delrace", machineID, "dep-delrace", "exec-old")
	if err := s.MarkMachineDeleted(ctx, machineID); err != nil {
		t.Fatal(err)
	}

	raw := ensureCreateRequest(t, machineID, "dep-delrace", "exec-new", "op-del-race-1")
	_, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-delrace", machineID+".test", "img",
		1, 512, 0, 8080, machineID, "dep-delrace", "exec-new", "op-del-race-1", 2, 0, raw, nil)
	if err == nil {
		t.Fatal("EnsureAppAndEnqueueCreate must reject DELETED machine (revive guard)")
	}
	m, gerr := s.GetMachine(ctx, machineID)
	if gerr != nil || m == nil {
		t.Fatal("get machine after rejected create")
	}
	if m.DesiredState != "DELETED" {
		t.Fatalf("desired_state = %q, want DELETED (guard must not flip it)", m.DesiredState)
	}
	if m.CurrentExecutionID != "exec-old" {
		t.Fatalf("current_execution_id = %q, want exec-old (guard must not advance execution)", m.CurrentExecutionID)
	}
}

// P2（v1.2-D review）：RecordRestartAttempt 对 DELETED 行也必须拒绝。
func TestCreateLifecycleIsAtomicAndReplayDoesNotExtendTTL(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-create-lifecycle-%d", os.Getpid())
	t.Cleanup(func() { cleanupProject(t, s, project) })
	cleanupProject(t, s, project)

	machineID, appID, opID := "m-create-lifecycle", "app-create-lifecycle", "op-create-lifecycle"
	if err := s.EnsureProject(ctx, project, "create-lifecycle-test"); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	raw := ensureCreateRequest(t, machineID, "dep-create-lifecycle", "exec-create-lifecycle", opID)
	if _, err := s.EnsureAppAndEnqueueCreateWithLifecycle(ctx, project, appID,
		machineID+".test", "img", 1, 512, 0, 8080, machineID,
		"dep-create-lifecycle", "exec-create-lifecycle", opID, 2, 0, raw, nil,
		&expires, "ON_FAILURE", 7, 13, 29); err != nil {
		t.Fatal(err)
	}
	later := expires.Add(time.Hour)
	if _, err := s.EnsureAppAndEnqueueCreateWithLifecycle(ctx, project, appID,
		machineID+".test", "img", 1, 512, 0, 8080, machineID,
		"dep-create-lifecycle", "exec-create-lifecycle", opID, 2, 0, raw, nil,
		&later, "ALWAYS", 99, 99, 99); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMachine(ctx, machineID)
	if err != nil || m == nil {
		t.Fatal("get machine")
	}
	if m.ExpiresAt == nil || !m.ExpiresAt.Equal(expires) {
		t.Fatalf("idempotent replay changed expires_at: got %v want %v", m.ExpiresAt, expires)
	}
	if m.RestartMode != "ON_FAILURE" || m.RestartMaxAttempts != 7 ||
		m.RestartBackoffSeconds != 13 || m.RestartStableWindowSeconds != 29 {
		t.Fatalf("lifecycle policy not atomically preserved: %+v", m)
	}
}

func TestRecordRestartAttemptRejectsDeleted(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-rrdel-%d", os.Getpid())
	t.Cleanup(func() { cleanupProject(t, s, project) })
	cleanupProject(t, s, project)

	machineID := "m-rr-del"
	seedMachine(t, s, project, "app-rrdel", machineID, "dep-rrdel", "exec-old")
	if err := s.MarkMachineDeleted(ctx, machineID); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordRestartAttempt(ctx, machineID, 1, time.Now().Add(time.Second)); err == nil {
		t.Fatal("RecordRestartAttempt must reject DELETED machine")
	}
}
