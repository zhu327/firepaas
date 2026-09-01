package store

import (
	"context"
	"errors"
	"testing"
)

// M3.3：app/deployment/rollout 基础 CRUD + 单 rollout 互斥（ADR-0015）。
func TestCreateAppAndDeploymentAtomic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-m3-app-atomic"
	cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	// Invalid deployment FK forces the transaction to fail; the app must not remain.
	err := s.CreateAppAndDeployment(ctx, project,
		App{ID: "app-atomic", Hostname: "atomic.local", ImageRef: "img:v1", VCPU: 1, MemMIB: 512, DesiredReplicas: 1},
		Deployment{ID: "dep-atomic", AppID: "wrong-app", Generation: 1, ImageRef: "img:v1", VCPU: 1, MemMIB: 512, Port: 80, Status: "ACTIVE"})
	if err == nil {
		t.Fatal("expected transaction failure")
	}
	if app, err := s.GetApp(ctx, "app-atomic"); err != nil || app != nil {
		t.Fatalf("app persisted after failed transaction: app=%+v err=%v", app, err)
	}
}

func TestAppDeploymentRolloutLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-m3-apps"
	cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	appID := "app-test-m3"
	if err := s.EnsureApp(ctx, project, appID, "app.test.local", "img:v1", 1, 512, 80, 2); err != nil {
		t.Fatal(err)
	}
	app, err := s.GetApp(ctx, appID)
	if err != nil || app == nil || app.DesiredReplicas != 2 || app.Generation != 1 {
		t.Fatalf("app = %+v err %v", app, err)
	}

	dep1 := "dep-1"
	if err := s.CreateDeployment(ctx, Deployment{
		ID: dep1, AppID: appID, Generation: 1, ImageRef: "img:v1",
		VCPU: 1, MemMIB: 512, Port: 80, Status: "ACTIVE",
	}); err != nil {
		t.Fatal(err)
	}
	dep2 := "dep-2"
	if err := s.CreateDeployment(ctx, Deployment{
		ID: dep2, AppID: appID, Generation: 2, ImageRef: "img:v2",
		VCPU: 1, MemMIB: 512, Port: 80, Status: "PREPARING",
	}); err != nil {
		t.Fatal(err)
	}

	// 单 rollout 互斥：第一个活跃后第二个必须 ErrRolloutBusy。
	if err := s.CreateRollout(ctx, Rollout{
		ID: "rl-1", AppID: appID, FromGeneration: 1, ToGeneration: 2,
	}); err != nil {
		t.Fatal(err)
	}
	err = s.CreateRollout(ctx, Rollout{
		ID: "rl-2", AppID: appID, FromGeneration: 1, ToGeneration: 3,
	})
	if !errors.Is(err, ErrRolloutBusy) {
		t.Fatalf("second rollout err = %v, want ErrRolloutBusy", err)
	}

	// 状态机推进：PREPARING → CUTOVER（幂等）→ COMPLETE。
	if err := s.RolloutToCutover(ctx, appID, "2099-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	rl, err := s.ActiveRolloutForApp(ctx, appID)
	if err != nil || rl == nil || rl.Status != "CUTOVER" {
		t.Fatalf("rollout = %+v err %v", rl, err)
	}
	if err := s.RolloutToCutover(ctx, appID, "2099-01-01T00:00:00Z"); err != nil {
		t.Fatalf("idempotent cutover: %v", err)
	}
	if err := s.CompleteRollout(ctx, appID, false); err != nil {
		t.Fatal(err)
	}
	rl, err = s.ActiveRolloutForApp(ctx, appID)
	if err != nil || rl != nil {
		t.Fatalf("rollout after complete = %+v err %v", rl, err)
	}

	// 互斥解除后可以再发。
	if err := s.CreateRollout(ctx, Rollout{
		ID: "rl-3", AppID: appID, FromGeneration: 2, ToGeneration: 3,
	}); err != nil {
		t.Fatalf("rollout after complete: %v", err)
	}
	if err := s.RolloutToRollback(ctx, appID); err != nil {
		t.Fatal(err)
	}
	rl, _ = s.ActiveRolloutForApp(ctx, appID)
	if rl == nil || rl.Status != "ROLLING_BACK" {
		t.Fatalf("rollback rollout = %+v", rl)
	}
	if err := s.CompleteRollout(ctx, appID, true); err != nil {
		t.Fatal(err)
	}
}

// M3.3：机器唯一键放宽后同 ordinal 跨 generation 共存（发布窗口需要）。
func TestMachinesCrossGenerationCoexist(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-m3-coexist"
	cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	appID := "app-coexist"
	if err := s.EnsureApp(ctx, project, appID, "co.local", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	gen1 := &Deployment{ID: "d1", AppID: appID, Generation: 1, ImageRef: "img:v1", VCPU: 1, MemMIB: 512, Port: 80, Status: "ACTIVE"}
	gen2 := &Deployment{ID: "d2", AppID: appID, Generation: 2, ImageRef: "img:v2", VCPU: 1, MemMIB: 512, Port: 80, Status: "PREPARING"}
	if err := s.CreateDeployment(ctx, *gen1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDeployment(ctx, *gen2); err != nil {
		t.Fatal(err)
	}

	// 同 ordinal、不同 generation 的两台机器必须都能存在。
	for _, m := range []struct {
		machineID, depID, execID string
		gen                      int64
	}{
		{"app-coexist-r0-g1", "d1", "exec-1", 1},
		{"app-coexist-r0-g2", "d2", "exec-2", 2},
	} {
		if _, err := s.EnsureAppAndEnqueueCreate(ctx, project, appID, "co.local",
			"img:v1", 1, 512, 0, 80, m.machineID, m.depID, m.execID,
			"op-"+m.machineID, m.gen, 0, []byte(`{"machine_id":"`+m.machineID+`"}`), nil); err != nil {
			t.Fatalf("enqueue %s: %v", m.machineID, err)
		}
	}
	ms, err := s.ListMachinesForApp(ctx, appID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("machines = %d, want 2", len(ms))
	}
}

// TestSoftDeleteAppTombstone（P0-1）：墓碑化后 ListApps 不再返回、
// reconcile 输入被过滤；重复删除幂等；scale 被拒绝。
func TestSoftDeleteAppTombstone(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-m3-del"
	cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	appID := "app-del-me"
	if err := s.EnsureApp(ctx, project, appID, "del.test.local", "img:v1", 1, 512, 80, 3); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDeployment(ctx, Deployment{
		ID: "dep-del-1", AppID: appID, Generation: 1, ImageRef: "img:v1", VCPU: 1, MemMIB: 512, Port: 80, Status: "ACTIVE",
	}); err != nil {
		t.Fatal(err)
	}
	// 活跃 rollout 存在时删除（S9 组合）。
	if err := s.CreateRollout(ctx, Rollout{
		ID: "rl-del", AppID: appID, FromGeneration: 1, ToGeneration: 2,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.SoftDeleteApp(ctx, appID); err != nil {
		t.Fatal(err)
	}
	// 幂等：再删一次不报错。
	if err := s.SoftDeleteApp(ctx, appID); err != nil {
		t.Fatalf("second delete should be idempotent: %v", err)
	}

	// ListApps 过滤墓碑。
	apps, err := s.ListApps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range apps {
		if a.ID == appID {
			t.Fatal("deleted app must not appear in ListApps")
		}
	}
	// GetApp 仍可读（含 Deleted 标记）。
	app, err := s.GetApp(ctx, appID)
	if err != nil || app == nil {
		t.Fatalf("get deleted app: %v %v", app, err)
	}
	if !app.Deleted {
		t.Fatal("app.Deleted must be true after SoftDeleteApp")
	}
	if app.DesiredReplicas != 0 {
		t.Fatalf("desired_replicas = %d, want 0", app.DesiredReplicas)
	}
	// 活跃 rollout 已终结。
	if rl, _ := s.ActiveRolloutForApp(ctx, appID); rl != nil {
		t.Fatalf("active rollout should be completed, got %+v", rl)
	}
	// ACTIVE deployment 已 SUPERSEDED。
	if dep, _ := s.ActiveDeploymentForApp(ctx, appID); dep != nil {
		t.Fatalf("deployment should be SUPERSEDED, got %+v", dep)
	}
	// 墓碑不可 scale。
	if err := s.SetAppReplicas(ctx, appID, 5); !errors.Is(err, ErrAppDeleted) {
		t.Fatalf("scale deleted app: err = %v, want ErrAppDeleted", err)
	}
}

// TestDeployAppTx（P2-3）：事务建 deployment+rollout+generation；
// 互斥由事务保证。
func TestDeployAppTx(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-m3-deploytx"
	cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	appID := "app-deploytx"
	if err := s.EnsureApp(ctx, project, appID, "tx.test.local", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDeployment(ctx, Deployment{
		ID: "dep-tx-1", AppID: appID, Generation: 1, ImageRef: "img:v1", VCPU: 1, MemMIB: 512, Port: 80, Status: "ACTIVE",
	}); err != nil {
		t.Fatal(err)
	}

	dep2 := Deployment{ID: "dep-tx-2", AppID: appID, Generation: 2, ImageRef: "img:v2", VCPU: 1, MemMIB: 512, Port: 80, Status: "PREPARING"}
	if err := s.DeployApp(ctx, dep2, Rollout{ID: "rl-tx-1", AppID: appID, FromGeneration: 1, ToGeneration: 2}, 2); err != nil {
		t.Fatal(err)
	}
	// app.generation 已推进。
	app, _ := s.GetApp(ctx, appID)
	if app.Generation != 2 {
		t.Fatalf("app.generation = %d, want 2", app.Generation)
	}
	// 活跃 rollout 存在。
	if rl, _ := s.ActiveRolloutForApp(ctx, appID); rl == nil || rl.ToGeneration != 2 {
		t.Fatalf("rollout = %+v", rl)
	}
	// 再次 deploy → ErrRolloutBusy（事务内互斥）。
	dep3 := Deployment{ID: "dep-tx-3", AppID: appID, Generation: 3, ImageRef: "img:v3", VCPU: 1, MemMIB: 512, Port: 80, Status: "PREPARING"}
	if err := s.DeployApp(ctx, dep3, Rollout{ID: "rl-tx-2", AppID: appID, FromGeneration: 2, ToGeneration: 3}, 3); !errors.Is(err, ErrRolloutBusy) {
		t.Fatalf("second deploy err = %v, want ErrRolloutBusy", err)
	}
	// 事务失败不留部分状态：gen=3 的 deployment 因 rollout 互斥不应存在。
	if dep, _ := s.GetDeployment(ctx, "dep-tx-3"); dep != nil {
		t.Fatal("deployment from rejected deploy must not exist (transaction rolled back)")
	}
}

// TestUserDeleteOpID（P0-2）：opID 嵌 execution，不同 execution 不撞键。
func TestUserDeleteOpID(t *testing.T) {
	a := UserDeleteOpID("m-1", "exec-11111111-2222")
	b := UserDeleteOpID("m-1", "exec-33333333-4444")
	if a == b {
		t.Fatalf("different executions must produce different opIDs: %s", a)
	}
	// 尾部 8 字符后缀（exec-11111111-2222 → 111-2222）。
	if a != "op-del-m-1-111-2222" {
		t.Fatalf("opID = %s, want op-del-m-1-111-2222", a)
	}
	// 空 execution 也要有后缀（uuid 防撞键）。
	c := UserDeleteOpID("m-2", "")
	d := UserDeleteOpID("m-2", "")
	if c == "" || c == d {
		t.Fatalf("empty execution must still produce unique suffix: %q %q", c, d)
	}
}
