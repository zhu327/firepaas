package controller

import (
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// approvalGateApplies 是纯函数：显式 opt-in 或 rolling canary 启用时暂停等人看。
func TestApprovalGateApplies(t *testing.T) {
	rolling := &store.Deployment{Strategy: "rolling"}
	bluegreen := &store.Deployment{Strategy: "bluegreen"}
	cases := []struct {
		name string
		r    *store.Rollout
		dep  *store.Deployment
		want bool
	}{
		{"nil rollout", nil, rolling, false},
		{"explicit opt-in bluegreen", &store.Rollout{ApprovalRequired: true}, bluegreen, true},
		{"explicit opt-in rolling", &store.Rollout{ApprovalRequired: true}, rolling, true},
		{"canary rolling implies gate", &store.Rollout{CanaryWeight: 20}, rolling, true},
		{"canary bluegreen rejected upstream, no gate", &store.Rollout{CanaryWeight: 20}, bluegreen, false},
		{"plain rolling no gate", &store.Rollout{}, rolling, false},
		{"plain bluegreen no gate", &store.Rollout{}, bluegreen, false},
		{"nil dep no panic", &store.Rollout{CanaryWeight: 20}, nil, false},
	}
	for _, tc := range cases {
		if got := approvalGateApplies(tc.r, tc.dep); got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, got, tc.want)
		}
	}
}
