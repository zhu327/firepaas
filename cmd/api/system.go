// system.go：M5.4 系统级运维端点（admin scope，routeScope 表已收口）。
//
//	GET  /readyz                   依赖就绪探活（PG SELECT 1 + Redis PING）
//	POST /v1/system/reprojections  清 Redis 投影 → 立即从 PG 全量重建
//	                                （P3-13：抢占 5s syncTicker，不等周期），
//	                                返回删除键数与真实重建耗时。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// readyz（R2 评审）：与 /v1/health（进程存活）区分的「依赖就绪」探针。
// PG 与 Redis 都是本进程服务核心请求面的硬依赖，任一不可用即 503，便于
// 负载/编排摘除实例；每次探测 ≤1s 超时，避免探针本身被挂住的依赖拖死。
func (a *API) readyz(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{}
	probe := func(name string, fn func(ctx context.Context) error) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			slog.Warn("readyz dependency probe failed", "dependency", name, "error", err)
			checks[name] = "unavailable"
			return
		}
		checks[name] = "ok"
	}
	probe("postgres", func(ctx context.Context) error {
		if a.pool == nil {
			return errors.New("postgres pool not configured") // 未装配，fail closed
		}
		var one int
		return a.pool.QueryRow(ctx, `SELECT 1`).Scan(&one)
	})
	probe("redis", func(ctx context.Context) error {
		if a.rdb == nil {
			return errors.New("redis client not configured")
		}
		return a.rdb.Ping(ctx).Err()
	})
	for _, v := range checks {
		if v != "ok" {
			writeJSON(w, 503, map[string]any{"status": "not_ready", "checks": checks})
			return
		}
	}
	writeJSON(w, 200, map[string]any{"status": "ready", "checks": checks})
}

func (a *API) reproject(w http.ResponseWriter, r *http.Request) {
	if a.cat == nil {
		writeErr(w, 503, "catalog not configured")
		return
	}
	start := time.Now()
	deleted, err := a.cat.WipeProjections(r.Context())
	if err != nil {
		writeInternalErr(w, r, err)
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
