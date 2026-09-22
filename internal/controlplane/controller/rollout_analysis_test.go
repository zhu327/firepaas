package controller

import (
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// cutover 分析是纯函数（Argo analysis 的判定表），不依赖 DB。
func TestServingRatioAnalysis(t *testing.T) {
	base := time.Now().Add(-10 * time.Minute)
	cutoverAt := base
	app := &store.App{ID: "app", DesiredReplicas: 3}
	rollout := &store.Rollout{
		AppID:          "app",
		FromGeneration: 1,
		ToGeneration:   2,
		Status:         "CUTOVER",
		CutoverAt:      &cutoverAt,
	}
	serving := func(ordinals ...int) []store.Machine {
		var out []store.Machine
		for _, o := range ordinals {
			out = append(out, store.Machine{
				ReplicaOrdinal: o, ObservedState: "RUNNING", ObservedReadiness: "READY",
			})
		}
		return out
	}
	now := time.Now()
	a := NewServingRatioAnalysis(CutoverAnalysisConfig{})

	// 窗内部分就绪 → Wait（不提前回退，避免抖动误杀）。
	if v, _ := a.AssessCutover(app, rollout, serving(0, 1), false, now); v != VerdictWait {
		t.Fatalf("mid-window partial = %v, want Wait", v)
	}
	// 窗到期全量 → Proceed。
	if v, _ := a.AssessCutover(app, rollout, serving(0, 1, 2), true, now); v != VerdictProceed {
		t.Fatalf("at-end full = %v, want Proceed", v)
	}
	// 窗到期 2/3（默认 min=1.0）→ Rollback（替代无限等待）。
	if v, reason := a.AssessCutover(app, rollout, serving(0, 1), true, now); v != VerdictRollback {
		t.Fatalf("at-end partial = %v (%s), want Rollback", v, reason)
	}
	// 自定义 min=0.5：窗到期 2/3 → Proceed。
	b := NewServingRatioAnalysis(CutoverAnalysisConfig{MinServingRatio: 0.5})
	if v, _ := b.AssessCutover(app, rollout, serving(0, 1), true, now); v != VerdictProceed {
		t.Fatalf("at-end 2/3 with min=0.5 = %v, want Proceed", v)
	}
	// 灾难快线：超宽限零可服务 → Rollback（窗内也一样）。
	zero := []store.Machine{{ReplicaOrdinal: 0, ObservedState: "UNKNOWN", ObservedReadiness: "UNKNOWN"}}
	if v, reason := a.AssessCutover(app, rollout, zero, false, now); v != VerdictRollback {
		t.Fatalf("zero serving = %v (%s), want Rollback", v, reason)
	}
	// 宽限内零可服务 → Wait（切流传播抖动）。
	if v, _ := a.AssessCutover(app, rollout, zero, false, cutoverAt.Add(5*time.Second)); v != VerdictWait {
		t.Fatalf("zero serving within grace = %v, want Wait", v)
	}
	// desired=0 → Proceed（无可观察量）。
	empty := &store.App{ID: "app"}
	if v, _ := a.AssessCutover(empty, rollout, nil, true, now); v != VerdictProceed {
		t.Fatalf("zero desired = %v, want Proceed", v)
	}
}

func TestCutoverAnalysisEnd(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	r := &store.Rollout{CutoverAt: &base, StartedAt: base.Add(-time.Hour)}
	deadline := base.Add(30 * time.Second)
	// Window=0 → drain deadline（历史行为）。
	if got := cutoverAnalysisEnd(r, 0, deadline); !got.Equal(deadline) {
		t.Fatalf("end=%v want drain deadline %v", got, deadline)
	}
	// Window>0 → cutover+window。
	if got := cutoverAnalysisEnd(r, 5*time.Minute, deadline); !got.Equal(base.Add(5 * time.Minute)) {
		t.Fatalf("end=%v want cutover+5m", got)
	}
	// cutover_at 缺失 → started_at 回退（不无限等待）。
	bare := &store.Rollout{StartedAt: base}
	if got := cutoverBase(bare); !got.Equal(base) {
		t.Fatalf("base=%v want started_at", got)
	}
	if got := cutoverBase(nil); !got.IsZero() {
		t.Fatalf("nil rollout base=%v want zero", got)
	}
}

func TestCutoverAnalysisConfigNormalized(t *testing.T) {
	c := CutoverAnalysisConfig{}.normalized()
	if c.MinServingRatio != 1.0 || c.ZeroServingGrace != 30*time.Second {
		t.Fatalf("default = %+v", c)
	}
	c = CutoverAnalysisConfig{MinServingRatio: 2}.normalized()
	if c.MinServingRatio != 1 {
		t.Fatalf("clamped = %+v", c)
	}
}
