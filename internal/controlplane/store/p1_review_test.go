package store

// P1 review 回归（需真实 PG；FIREPAAS_TEST_POSTGRES 未设时跳过）：
// canary 权重持久化、TakeoverScaleCAS、ListMachinesPaged。
//
//go:generate 不需要；testStore 已跑全量迁移（含 0040 canary_weight）。
import (
	"context"
	"testing"
)

func TestRolloutCanaryWeightRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-p1-canary"
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-canary", "canary.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-canary-2", "canary2.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	// 显式权重落库。
	if err := s.CreateRollout(ctx, Rollout{ID: "rl-1", AppID: "app-canary", FromGeneration: 1, ToGeneration: 2, CanaryWeight: 25}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRollout(ctx, "rl-1")
	if err != nil || got == nil || got.CanaryWeight != 25 {
		t.Fatalf("rollout = %+v %v, want canary 25", got, err)
	}
	// 未指定（0）→ 不启用（列默认 0，不加权）。
	if err := s.CreateRollout(ctx, Rollout{ID: "rl-2", AppID: "app-canary-2", FromGeneration: 1, ToGeneration: 2}); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetRollout(ctx, "rl-2")
	if err != nil || got == nil || got.CanaryWeight != 0 {
		t.Fatalf("rollout = %+v %v, want canary disabled (0)", got, err)
	}
}

func TestTakeoverScaleCAS(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-p1-scalecas"
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-cas", "cas.test", "img:v1", 1, 512, 80, 3); err != nil {
		t.Fatal(err)
	}
	app, err := s.GetApp(ctx, "app-cas")
	if err != nil || app == nil {
		t.Fatalf("get app: %+v %v", app, err)
	}
	// 前提成立（desired + 时间戳）→ 接管成功。
	ok, err := s.TakeoverScaleCAS(ctx, "app-cas", 5, 3, app.UpdatedAt)
	if err != nil || !ok {
		t.Fatalf("cas = %v %v, want true nil", ok, err)
	}
	// 陈旧时间戳（ABA：值碰巧回到 3 也救不了）→ false，不覆盖。
	if err := s.TakeoverScale(ctx, "app-cas", 3); err != nil {
		t.Fatal(err)
	}
	ok, err = s.TakeoverScaleCAS(ctx, "app-cas", 7, 3, app.UpdatedAt)
	if err != nil || ok {
		t.Fatalf("stale-timestamp cas = %v %v, want false nil", ok, err)
	}
	cur, err := s.GetApp(ctx, "app-cas")
	if err != nil || cur == nil || cur.DesiredReplicas != 3 {
		t.Fatalf("app = %+v %v, want replicas 3", cur, err)
	}
}

func TestListMachinesPaged(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-p1-page"
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-page", "page.test", "img:v1", 1, 512, 80, 3); err != nil {
		t.Fatal(err)
	}
	for i, m := range []string{"app-page-r0", "app-page-r1", "app-page-r2"} {
		if _, err := s.EnsureAppAndEnqueueCreate(ctx, CreateMachineParams{
			ProjectID: project, AppID: "app-page", Hostname: "page.test", ImageRef: "img:v1",
			VCPU: 1, MemMIB: 512, IngressPort: 80, MachineID: m, DeploymentID: "dep-page",
			ExecutionID: "exec-1", OperationID: "op-" + m, Generation: 1, ReplicaOrdinal: i,
			RequestJSON: []byte(`{}`),
		}); err != nil {
			t.Fatalf("enqueue %s: %v", m, err)
		}
	}
	// limit=2 → 首页 2 条 + cursor；次页 1 条 + 空 cursor。
	first, err := s.ListMachinesPaged(ctx, project, 2, "")
	if err != nil || len(first) != 2 {
		t.Fatalf("first = %d %v", len(first), err)
	}
	second, err := s.ListMachinesPaged(ctx, project, 2, first[1].ID)
	if err != nil || len(second) != 1 || second[0].ID == first[0].ID || second[0].ID == first[1].ID {
		t.Fatalf("second = %+v %v", second, err)
	}
	// 未知 cursor → ErrNotFound（调用方 400）。
	if _, err := s.ListMachinesPaged(ctx, project, 2, "no-such-machine"); err == nil {
		t.Fatal("unknown cursor must error")
	}
	// limit 归一：<=0 → 200（全量 3 条一次返回）。
	all, err := s.ListMachinesPaged(ctx, project, 0, "")
	if err != nil || len(all) != 3 {
		t.Fatalf("default limit = %d %v", len(all), err)
	}
}
