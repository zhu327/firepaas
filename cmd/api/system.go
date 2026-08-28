// system.go：M5.4 系统级运维端点（admin scope，routeScope 表已收口）。
//
//	POST /v1/system/reprojections  清 Redis 投影 → 立即从 PG 全量重建
//	                                （P3-13：抢占 5s syncTicker，不等周期），
//	                                返回删除键数与真实重建耗时。
package main

import (
	"log/slog"
	"net/http"
	"time"
)

func (a *API) reproject(w http.ResponseWriter, r *http.Request) {
	if a.cat == nil {
		writeErr(w, 503, "catalog not configured")
		return
	}
	start := time.Now()
	deleted, err := a.cat.WipeProjections(r.Context())
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	wipeDur := time.Since(start)
	// P3-13（M5 评审）：直接调 leader controller 的 KickRouteRebuild 抢占
	// 5s ticker；leader 未就绪（刚启动/丢锁）时降级为“等下一 sync 周期”。
	rebuilt, rebuildDur := false, time.Duration(0)
	if a.kicker != nil {
		if d, kerr, ok := a.kicker.Kick(); ok {
			rebuilt, rebuildDur = true, d
			if kerr != nil {
				slog.Error("explicit reproject kick failed", "error", kerr)
			}
		}
	}
	total := time.Since(start)
	slog.Info("projections wiped for explicit rebuild",
		"deleted", deleted, "wipe_ms", wipeDur.Milliseconds(),
		"rebuilt_now", rebuilt, "rebuild_ms", rebuildDur.Milliseconds(),
		"total_ms", total.Milliseconds())
	if a.metrics != nil {
		a.metrics.Inc("firepaas_projection_rebuilds_total", nil, 1)
	}
	writeJSON(w, 200, map[string]any{
		"deleted_keys": deleted,
		"duration_ms":  total.Milliseconds(),
		"rebuilt_now":  rebuilt,
		"rebuild": func() string {
			if rebuilt {
				return "immediate (kicked)"
			}
			return "controller sync cycle (routes ≤5s, leases ≤30s)"
		}(),
	})
}
