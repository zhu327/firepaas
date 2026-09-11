package controller

import (
	"context"
	"log/slog"
	"time"
)

// KickRouteRebuild 立即执行一次路由重建（P3-13，M5 评审）：显式重投影端点
// 调用它抢占 5s syncTicker，而不只是 wipe 后等待。用后台 context（不占
// leader 循环的取消窗口），重建完成即返回耗时。
// ---------------------------------------------------------------------------
// route 投影（R7：全量重建 + prune）
// ---------------------------------------------------------------------------

func (c *Controller) KickRouteRebuild() (time.Duration, error) {
	start := time.Now()
	ctx := context.Background()
	if err := c.buildRoutes(ctx); err != nil {
		return time.Since(start), err
	}
	return time.Since(start), nil
}

// rebuildLeases：预约全量重建（P2-2）+ 节点投影 stale 标记（P3-6c）+ route
// 投影全量重建。流程：
//
//  1. ListInFlightOperations 取活跃 create 集合；
//  2. PruneStaleOps 删除非活跃 resv:op 键；
//  3. Reset 原子重建（清零全部 node/project hash + 从存活 op 键重放在途
//     承诺），修复 op 键 TTL 先过期、hash 增量永久残留的节点假满；
//     重建不依赖重新 Acquire——Acquire 的幂等早退会跳过 hash 入账。
//  4. MarkStaleNodes 把 last_seen 超时节点置 UNKNOWN；
//  5. buildRoutes。
//
// 只在单写者（M2a leader）周期内执行。
func (c *Controller) rebuildLeases(ctx context.Context) error {
	inFlight, err := c.store.ListInFlightOperations(ctx)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, op := range inFlight {
		if op.Kind != "create" {
			continue
		}
		active[op.ID] = true
	}
	pruned, err := c.resv.PruneStaleOps(ctx, active)
	if err != nil {
		return err
	}
	cleared, err := c.resv.Reset(ctx)
	if err != nil {
		return err
	}
	if pruned > 0 || cleared > 0 {
		slog.Info("reservation rebuild", "pruned_stale_ops", pruned, "cleared_hashes", cleared)
		c.metrics.Inc("firepaas_reservation_rebuilds_total", nil, 1)
	}

	// P3-6c：节点从 Nomad 消失后 PG 投影永远保留旧状态。阈值必须覆盖
	// nodemanager 的 ServiceInfo 心跳周期；否则心跳间隔大于 controller sync
	// 间隔时，健康节点会在两次心跳之间被周期性误标 UNKNOWN。
	if n, err := c.store.MarkStaleNodes(ctx, c.cfg.NodeStaleAfter); err != nil {
		slog.Warn("mark stale nodes", "error", err)
	} else if n > 0 {
		slog.Warn("marked stale nodes UNKNOWN", "count", n)
	}
	return c.buildRoutes(ctx)
}

func (c *Controller) buildRoutes(ctx context.Context) error {
	proxyByNode := make(map[string]string)
	for _, view := range c.nodeViews() {
		proxyByNode[view.agentID] = view.proxy
	}
	return c.routes.Rebuild(ctx, proxyByNode)
}
