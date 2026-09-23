package store

// Wave3 Stage B 回归（需真实 PG；FIREPAAS_TEST_POSTGRES 未设时跳过）：
// PAUSED_FOR_APPROVAL 暂停 → 人工放行 → CUTOVER；非 PAUSED 放行 409 语义
//（approved=false）；暂停态计入活跃互斥。
import (
	"context"
	"testing"
	"time"
)

func TestRolloutApprovalPauseAndApprove(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-approval-gate"
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-approval", "approval.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRollout(ctx, Rollout{
		ID: "rl-approval", AppID: "app-approval",
		FromGeneration: 1, ToGeneration: 2, ApprovalRequired: true,
	}); err != nil {
		t.Fatal(err)
	}
	// PAUSED 前放行 → false（不在等待态）。
	if ok, err := s.ApproveRollout(ctx, "app-approval", "tester", time.Now().Add(time.Minute)); err != nil || ok {
		t.Fatalf("approve before pause: ok=%v err=%v", ok, err)
	}
	// PREPARING → PAUSED。
	if err := s.RolloutToPaused(ctx, "app-approval"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRollout(ctx, "rl-approval")
	if err != nil || got == nil || got.Status != "PAUSED_FOR_APPROVAL" || !got.ApprovalRequired {
		t.Fatalf("paused rollout = %+v err=%v", got, err)
	}
	// 暂停态仍是活跃互斥：再建 rollout → ErrRolloutBusy。
	if err := s.CreateRollout(ctx, Rollout{
		ID: "rl-approval-2", AppID: "app-approval", FromGeneration: 2, ToGeneration: 3,
	}); err != ErrRolloutBusy {
		t.Fatalf("second rollout during pause err=%v, want ErrRolloutBusy", err)
	}
	// 放行 → CUTOVER + 审批人落库。
	deadline := time.Now().Add(time.Minute)
	ok, err := s.ApproveRollout(ctx, "app-approval", "tester", deadline)
	if err != nil || !ok {
		t.Fatalf("approve: ok=%v err=%v", ok, err)
	}
	got, _ = s.GetRollout(ctx, "rl-approval")
	if got.Status != "CUTOVER" || got.ApprovedBy != "tester" ||
		got.ApprovedAt == nil || got.CutoverAt == nil || got.DrainDeadline == nil {
		t.Fatalf("approved rollout = %+v", got)
	}
	// 重复放行 → false（已是 CUTOVER）。
	if ok, err := s.ApproveRollout(ctx, "app-approval", "tester", deadline); err != nil || ok {
		t.Fatalf("re-approve: ok=%v err=%v", ok, err)
	}
	// PAUSED 态可手动回滚（审批等待中发现坏代）：另起一 app 验证。
	if err := s.EnsureApp(ctx, project, "app-approval-rb", "approval-rb.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRollout(ctx, Rollout{
		ID: "rl-approval-rb", AppID: "app-approval-rb", FromGeneration: 1, ToGeneration: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RolloutToPaused(ctx, "app-approval-rb"); err != nil {
		t.Fatal(err)
	}
	if err := s.RolloutToRollback(ctx, "app-approval-rb"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetRollout(ctx, "rl-approval-rb")
	if got.Status != "ROLLING_BACK" {
		t.Fatalf("rollback from pause = %s, want ROLLING_BACK", got.Status)
	}
}
