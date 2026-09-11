package controller

import (
	"context"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// rolloutHoldsRecreate 判断该机器是否处于“发布持有”状态（S4/S6）：
// CUTOVER 中的旧代机器、ROLLING_BACK 中的新代机器死亡时不重建。
// v1.2-D（ADR-0026 §6 owner 决策表）：active rollout 负责 target replica
// 的生命周期——PREPARING/CUTOVER/ROLLING_BACK 期间，target deployment
// 的机器死亡由 rollout create retry 收敛，不消耗 restart attempts。
func (c *Controller) rolloutHoldsRecreate(ctx context.Context, m store.Machine) bool {
	if m.DeploymentID == "" {
		return false
	}
	rl, err := c.store.ActiveRolloutForApp(ctx, m.AppID)
	if err != nil {
		return true // fail closed: an unknown rollout owner must suppress restart
	}
	if rl == nil {
		return false
	}
	dep, err := c.store.GetDeployment(ctx, m.DeploymentID)
	if err != nil || dep == nil {
		return true
	}
	return rolloutHoldDecision(rl.Status, dep.Generation, rl.FromGeneration, rl.ToGeneration)
}

// rolloutHoldDecision 是 owner 决策表的纯函数（v1.2-D，ADR-0026 §6）：
// active rollout 负责 target replica 的生命周期；target 的死亡走 rollout
// create retry（S2/S3 超时、回滚），不消耗 restart attempts。
func rolloutHoldDecision(status string, depGen, fromGen, toGen int64) bool {
	switch status {
	case "PREPARING":
		return depGen == toGen
	case "CUTOVER":
		return depGen != toGen
	case "ROLLING_BACK":
		return depGen != fromGen
	default:
		return false
	}
}

// rolloutOwnsReplacement identifies the deployment that an active rollout must
// actively keep at desired replica count. This is intentionally distinct from
// rolloutHoldDecision, which also covers generations being drained/removed.
func rolloutOwnsReplacement(status string, depGen, fromGen, toGen int64) bool {
	switch status {
	case "PREPARING", "CUTOVER":
		return depGen == toGen
	case "ROLLING_BACK":
		return depGen == fromGen
	default:
		return false
	}
}

func (c *Controller) rolloutRepairsMachine(ctx context.Context, m store.Machine) bool {
	rl, err := c.store.ActiveRolloutForApp(ctx, m.AppID)
	if err != nil || rl == nil {
		return false
	}
	dep, err := c.store.GetDeployment(ctx, m.DeploymentID)
	return err == nil && dep != nil && rolloutOwnsReplacement(rl.Status, dep.Generation,
		rl.FromGeneration, rl.ToGeneration)
}

// reconcileApps 是 M3 的 app 层对账（scale + rollout），错误只记日志不中断
// 主循环（单个 app 的脏状态不能拖垮 machine reconcile）。
func (c *Controller) reconcileApps(ctx context.Context) error {
	if err := c.reconcileRollouts(ctx); err != nil {
		return err
	}
	return c.reconcileAppScale(ctx)
}
