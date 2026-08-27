// system.go：M5.4 系统级运维端点（admin scope，routeScope 表已收口）。
//
//	POST /v1/system/reprojections  清 Redis 投影 → 下一 controller sync
//	                                周期（routes 5s / leases 30s）从 PG
//	                                全量重建；返回删除键数与耗时。
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
	dur := time.Since(start)
	slog.Info("projections wiped for explicit rebuild",
		"deleted", deleted, "duration_ms", dur.Milliseconds())
	if a.metrics != nil {
		a.metrics.Inc("firepaas_projection_rebuilds_total", nil, 1)
	}
	writeJSON(w, 200, map[string]any{
		"deleted_keys": deleted,
		"duration_ms":  dur.Milliseconds(),
		"rebuild":      "controller sync cycle (routes ≤5s, leases ≤30s)",
	})
}
