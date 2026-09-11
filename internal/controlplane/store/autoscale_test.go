package store

import (
	"context"
	"testing"
)

// 纯函数校验（无需 PG）：ADR-0041 §1 约束矩阵 + 验收线 §9。
func TestValidateAutoscalePolicy(t *testing.T) {
	valid := DefaultAutoscalePolicy()
	if err := ValidateAutoscalePolicy(valid); err != nil {
		t.Fatalf("default policy must validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*AutoscalePolicy)
	}{
		{"min>max", func(p *AutoscalePolicy) { p.MinReplicas = 5; p.MaxReplicas = 3 }},
		{"max=0", func(p *AutoscalePolicy) { p.MinReplicas = 0; p.MaxReplicas = 0 }},
		{"max>100", func(p *AutoscalePolicy) { p.MaxReplicas = 101 }},
		{"negative min", func(p *AutoscalePolicy) { p.MinReplicas = -1 }},
		{"target=0", func(p *AutoscalePolicy) { p.TargetConcurrency = 0 }},
		{"target>256", func(p *AutoscalePolicy) { p.TargetConcurrency = 257 }},
		{"delay low", func(p *AutoscalePolicy) { p.ScaleDownDelaySec = 29 }},
		{"delay high", func(p *AutoscalePolicy) { p.ScaleDownDelaySec = 601 }},
		{"panic low", func(p *AutoscalePolicy) { p.PanicThreshold = 1.4 }},
		{"panic high", func(p *AutoscalePolicy) { p.PanicThreshold = 5.1 }},
	}
	for _, tc := range cases {
		p := DefaultAutoscalePolicy()
		tc.mutate(&p)
		if err := ValidateAutoscalePolicy(p); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
	// 边界合法：min=0（显式 scale-to-zero）、delay/panic 上下限。
	edge := DefaultAutoscalePolicy()
	edge.MinReplicas = 0
	edge.ScaleDownDelaySec = 30
	edge.PanicThreshold = 1.5
	edge.TargetConcurrency = 1
	if err := ValidateAutoscalePolicy(edge); err != nil {
		t.Errorf("lower edge must validate: %v", err)
	}
	edge = DefaultAutoscalePolicy()
	edge.MaxReplicas = 100
	edge.ScaleDownDelaySec = 600
	edge.PanicThreshold = 5.0
	edge.TargetConcurrency = 256
	if err := ValidateAutoscalePolicy(edge); err != nil {
		t.Errorf("upper edge must validate: %v", err)
	}
}

// clamp 方向：脏行收敛到合法域，从不放大。
func TestClampAutoscalePolicy(t *testing.T) {
	got := clampAutoscalePolicy(AutoscalePolicy{})
	if got.MaxReplicas != 1 || got.TargetConcurrency != 1 ||
		got.ScaleDownDelaySec != 30 || got.PanicThreshold != 1.5 {
		t.Fatalf("zero row clamps to minimums, got %+v", got)
	}
	if err := ValidateAutoscalePolicy(got); err != nil {
		t.Fatalf("clamped policy must validate: %v", err)
	}
	inverted := AutoscalePolicy{MinReplicas: 8, MaxReplicas: 3, TargetConcurrency: 999}
	got = clampAutoscalePolicy(inverted)
	if got.MinReplicas != 3 || got.MaxReplicas != 3 || got.TargetConcurrency != 256 {
		t.Fatalf("inverted row clamps inward, got %+v", got)
	}
}

// App.Autoscale 把裸零值行归一为合法策略（min=max=0 不得被误读为缩零）。
func TestAppAutoscaleDefaults(t *testing.T) {
	var a App
	p := a.Autoscale()
	if err := ValidateAutoscalePolicy(p); err != nil {
		t.Fatalf("zero App must yield a valid policy: %v (%+v)", err, p)
	}
	// 精确锁定归一值：min=0（未启用，min 仅启用时参与 clamp 下限）、
	// max/target/delay/panic 收敛到最小合法值。
	if p.Enabled || p.MinReplicas != 0 || p.MaxReplicas != 1 ||
		p.TargetConcurrency != 1 || p.ScaleDownDelaySec != 30 || p.PanicThreshold != 1.5 {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

// TestAutoscalePolicyCRUD 跑 PG（migration 0039 + 策略 CRUD + CAS + 接管）。
func TestAutoscalePolicyCRUD(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-autoscale-policy"
	cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	appID := "app-autoscale-1"
	if err := s.EnsureApp(ctx, project, appID, "autoscale.local", "img:v1", 1, 512, 80, 2); err != nil {
		t.Fatal(err)
	}
	// 默认：关闭（migration 默认值经新列生效）。
	pol, err := s.GetAutoscalePolicy(ctx, appID)
	if err != nil {
		t.Fatal(err)
	}
	if pol.Enabled {
		t.Fatal("new app must default to autoscale disabled")
	}
	// 非法策略拒绝。
	bad := DefaultAutoscalePolicy()
	bad.Enabled = true
	bad.MinReplicas = 9
	bad.MaxReplicas = 2
	if err := s.SetAutoscalePolicy(ctx, appID, bad); err == nil {
		t.Fatal("expected validation error for min>max")
	}
	// 合法全量替换。
	want := AutoscalePolicy{
		Enabled: true, MinReplicas: 0, MaxReplicas: 5,
		TargetConcurrency: 20, ScaleDownDelaySec: 60, PanicThreshold: 2.0,
	}
	if err := s.SetAutoscalePolicy(ctx, appID, want); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetAutoscalePolicy(ctx, appID)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("policy = %+v, want %+v", got, want)
	}
	// CAS：expect 命中 → true；失配 → false 且不覆盖。
	ok, err := s.SetAppReplicasCAS(ctx, appID, 4, 2)
	if err != nil || !ok {
		t.Fatalf("cas match: ok=%v err=%v", ok, err)
	}
	ok, err = s.SetAppReplicasCAS(ctx, appID, 1, 2)
	if err != nil || ok {
		t.Fatalf("cas mismatch must not write: ok=%v err=%v", ok, err)
	}
	app, _ := s.GetApp(ctx, appID)
	if app.DesiredReplicas != 4 {
		t.Fatalf("desired = %d, want 4", app.DesiredReplicas)
	}
	// 关闭后 CAS 永不命中（接管/策略变更兜底）。
	off := want
	off.Enabled = false
	if err := s.SetAutoscalePolicy(ctx, appID, off); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.SetAppReplicasCAS(ctx, appID, 5, 4); ok {
		t.Fatal("cas on disabled policy must miss")
	}
	// 接管：单条写 desired + 关 enabled。
	if err := s.TakeoverScale(ctx, appID, 6); err != nil {
		t.Fatal(err)
	}
	app, _ = s.GetApp(ctx, appID)
	if app.DesiredReplicas != 6 {
		t.Fatalf("takeover desired = %d, want 6", app.DesiredReplicas)
	}
	got, _ = s.GetAutoscalePolicy(ctx, appID)
	if got.Enabled {
		t.Fatal("takeover must disable autoscale")
	}
	if _, err := s.GetAutoscalePolicy(ctx, "no-such-app"); err == nil {
		t.Fatal("expected ErrNotFound for missing app")
	}
}
