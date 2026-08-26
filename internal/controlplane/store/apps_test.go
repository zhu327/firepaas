package store

import (
	"context"
	"errors"
	"testing"
)

// M3.3：app/deployment/rollout 基础 CRUD + 单 rollout 互斥（ADR-0015）。
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
			"img:v1", 1, 512, 80, m.machineID, m.depID, m.execID,
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
