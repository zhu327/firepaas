package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/observability/metrics"
)

// sampleJSON 构造信号 field（与 edge.HostSample 同形状）。
func sampleJSON(ewma, rps float64, hard, unserved int64, ts time.Time) string {
	raw, _ := json.Marshal(autoscaleSample{
		EWMA: ewma, RPS: rps, HardRejected: hard, Unserved: unserved,
		TsMs: ts.UnixMilli(), WinMs: 10000,
	})
	return string(raw)
}

func testPolicy() store.AutoscalePolicy {
	return store.AutoscalePolicy{
		Enabled: true, MinReplicas: 1, MaxReplicas: 10,
		TargetConcurrency: 20, ScaleDownDelaySec: 120, PanicThreshold: 2.0,
	}
}

// 聚合：fresh 求和、unserved 折算、多 hostname 不翻倍、脏 field 跳过。
func TestAggregateAutoscaleSignal(t *testing.T) {
	now := time.Now()
	fields := map[string]string{
		"edge-1": sampleJSON(15, 30, 0, 25, now),
		"edge-2": sampleJSON(5, 10, 0, 0, now),
		"junk":   "not-json",
	}
	sig := aggregateAutoscaleSignal([]map[string]string{fields}, 20, now)
	if !sig.HasFresh || sig.PanicReject || sig.HasStaleNonzero {
		t.Fatalf("flags: %+v", sig)
	}
	if sig.Served != 20 || sig.TotalRPS != 40 {
		t.Fatalf("sum: %+v", sig)
	}
	if sig.Unserved != 2 { // ceil(25/20)
		t.Fatalf("unserved=%v, want 2", sig.Unserved)
	}
	// 多 hostname 求和 = 逐个之和（单归属，不翻倍）。
	a := aggregateAutoscaleSignal([]map[string]string{{"e1": sampleJSON(10, 5, 0, 0, now)}}, 20, now)
	b := aggregateAutoscaleSignal([]map[string]string{{"e1": sampleJSON(10, 5, 0, 0, now)}}, 20, now)
	ab := aggregateAutoscaleSignal([]map[string]string{
		{"e1": sampleJSON(10, 5, 0, 0, now)},
		{"e2": sampleJSON(10, 5, 0, 0, now)},
	}, 20, now)
	if ab.Served != a.Served+b.Served || ab.TotalRPS != a.TotalRPS+b.TotalRPS {
		t.Fatalf("multi-host double count: %+v vs %+v+%+v", ab, a, b)
	}
}

// 新鲜度：无 fresh→hold；过期全零忽略；过期非零禁缩容；轻微未来接受。
func TestAggregateFreshness(t *testing.T) {
	now := time.Now()
	// 全过期 → 无 fresh。
	sig := aggregateAutoscaleSignal([]map[string]string{{
		"dead": sampleJSON(9, 9, 0, 0, now.Add(-60*time.Second)),
	}}, 20, now)
	if sig.HasFresh || !sig.HasStaleNonzero || sig.Served != 0 {
		t.Fatalf("stale: %+v", sig)
	}
	// 过期全零 → 缺席（不禁缩容）。
	sig = aggregateAutoscaleSignal([]map[string]string{{
		"dead": sampleJSON(0, 0, 0, 0, now.Add(-60*time.Second)),
	}}, 20, now)
	if sig.HasFresh || sig.HasStaleNonzero {
		t.Fatalf("expired-zero: %+v", sig)
	}
	// 轻微未来（+3s）在容忍内。
	sig = aggregateAutoscaleSignal([]map[string]string{{
		"e": sampleJSON(7, 0, 0, 0, now.Add(3*time.Second)),
	}}, 20, now)
	if !sig.HasFresh || sig.Served != 7 {
		t.Fatalf("future: %+v", sig)
	}
	// hard_rejected 任一 fresh>0 即 panic。
	sig = aggregateAutoscaleSignal([]map[string]string{{
		"e": sampleJSON(0, 0, 3, 0, now),
	}}, 20, now)
	if !sig.PanicReject || !sig.HasFresh {
		t.Fatalf("panic: %+v", sig)
	}
}

// 决策核：ceil/clamp/panic 双路/max 封顶/零容量无 panic。
func TestComputeAutoscaleWant(t *testing.T) {
	p := testPolicy()
	sig := func(served, unserved float64, panic bool) AutoscaleSignal {
		return AutoscaleSignal{Served: served, Unserved: unserved, PanicReject: panic, HasFresh: true}
	}
	// 45 并发 / target 20 → 3。
	if w, pb := computeAutoscaleWant(p, sig(45, 0, false), 2, 0); w != 3 || pb {
		t.Fatalf("want=%d panic=%v", w, pb)
	}
	// idle → min。
	if w, _ := computeAutoscaleWant(p, sig(0, 0, false), 3, 0); w != 1 {
		t.Fatalf("idle want=%d, want 1", w)
	}
	// 零容量冷起：unserved 即负载（min=0 时 want>=1）。
	p0 := p
	p0.MinReplicas = 0
	if w, _ := computeAutoscaleWant(p0, sig(0, 21, false), 0, 0); w != 2 {
		t.Fatalf("cold start want=%d, want 2", w)
	}
	// panic 阈值路：load=50 > 2*20*1=40 → max(3,ceil(2*1)=2,1+2=3)=3。
	if w, pb := computeAutoscaleWant(p, sig(50, 0, false), 1, 0); w != 3 || !pb {
		t.Fatalf("panic want=%d boost=%v", w, pb)
	}
	// hard_rejected 路：load 低也放大到 max(ceil(panic*eff), eff+2)=8。
	if w, pb := computeAutoscaleWant(p, sig(5, 0, true), 4, 0); w != 8 || !pb {
		t.Fatalf("reject want=%d boost=%v", w, pb)
	}
	// eff=0 时 panic 关闭（零容量不走快扩）。
	if w, pb := computeAutoscaleWant(p, sig(0, 0, true), 0, 0); pb || w != 1 {
		t.Fatalf("zero-eff want=%d boost=%v", w, pb)
	}
	// warming 计入 eff：load=50，ready=1+warming=1 → eff=2，50<80 不放大。
	if w, pb := computeAutoscaleWant(p, sig(50, 0, false), 1, 1); w != 3 || pb {
		t.Fatalf("warming want=%d boost=%v", w, pb)
	}
	// max 封顶（含 panic）。
	if w, _ := computeAutoscaleWant(p, sig(10000, 0, true), 50, 0); w != 10 {
		t.Fatalf("capped want=%d, want 10", w)
	}
}

// 稳定窗：首次 false、同 want 未到期 false、到期 true、want 变化重置。
func TestScaleDownElapsed(t *testing.T) {
	st := newAutoscaleLoopState()
	now := time.Now()
	if st.scaleDownElapsed("a", 1, now, 120) {
		t.Fatal("first sight must hold")
	}
	if st.scaleDownElapsed("a", 1, now.Add(119*time.Second), 120) {
		t.Fatal("must hold before delay")
	}
	if !st.scaleDownElapsed("a", 1, now.Add(120*time.Second), 120) {
		t.Fatal("must release after delay")
	}
	if st.scaleDownElapsed("a", 0, now.Add(121*time.Second), 120) {
		t.Fatal("want change must restart window")
	}
}

// 并发探针（P0-1 回归）：dispatch worker 写入与主循环读写并发必须无 race。
// 跑法：go test -race -run TestAutoscaleLoopStateConcurrent ./internal/controlplane/controller/
func TestAutoscaleLoopStateConcurrent(t *testing.T) {
	c := &Controller{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			app := fmt.Sprintf("race-%d", i%2)
			for j := 0; j < 50; j++ {
				c.noteQuotaRejection(app)
				c.notePlacementSuccess(app)
				st := c.autoscaleLoop()
				st.checkPolicy(app, testPolicy())
				st.scaleDownElapsed(app, j%3, time.Now(), 30)
				st.frozen(app, time.Now())
				st.markFreezeNoted(app)
				st.isFreezeNoted(app)
				st.clearFreeze(app)
				st.deleteScaleDown(app)
				st.clearStreak(app)
				st.bumpStreak(app, time.Second)
				st.prefetchAllowed("dep-race", time.Now())
				st.collectExpiredFreeze(app, time.Now())
				st.clearApp(app)
				st.purgeUnknown(map[string]bool{app: true})
			}
		}(i)
	}
	wg.Wait()
}

// setScaleDown/hasScaleDown 是环路测试的锁内状态预置/断言（生产代码禁直接碰 map）。
func setScaleDown(c *Controller, appID string, want int, since time.Time) {
	st := c.autoscaleLoop()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.scaleDown[appID] = scaleDownEntry{want: want, since: since}
}

func hasScaleDown(c *Controller, appID string) bool {
	st := c.autoscaleLoop()
	st.mu.RLock()
	defer st.mu.RUnlock()
	_, ok := st.scaleDown[appID]
	return ok
}

// ---- 环路测试（PG + Redis；缺一跳过）----

func autoscaleLoopFixtures(t *testing.T) (*Controller, *store.Store, *redis.Client, string) {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run autoscale loop tests")
	}
	raddr := os.Getenv("FIREPAAS_TEST_REDIS")
	if raddr == "" {
		raddr = "127.0.0.1:6379"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rdb := redis.NewClient(&redis.Options{Addr: raddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	st := store.New(pool)
	project := fmt.Sprintf("test-autoscale-loop-%d", time.Now().UnixNano())
	if err := st.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = pool.Exec(cctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = pool.Exec(
			cctx,
			`DELETE FROM machines WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
			project,
		)
		_, _ = pool.Exec(
			cctx,
			`DELETE FROM rollouts WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
			project,
		)
		_, _ = pool.Exec(
			cctx,
			`DELETE FROM deployments WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
			project,
		)
		_, _ = pool.Exec(cctx, `DELETE FROM user_events WHERE project_id=$1`, project)
		_, _ = pool.Exec(cctx, `DELETE FROM apps WHERE project_id=$1`, project)
		_, _ = pool.Exec(cctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	c := &Controller{store: st, cat: catalog.New(rdb), metrics: metrics.New(), cfg: Config{}}
	return c, st, rdb, project
}

func seedAutoscaleApp(t *testing.T, st *store.Store, project, appID, hostname string, desired int) {
	t.Helper()
	ctx := context.Background()
	if err := st.EnsureApp(ctx, project, appID, hostname, "img:v1", 1, 512, 80, desired); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateDeployment(ctx, store.Deployment{
		ID: "dep-" + appID + "-1", AppID: appID, Generation: 1,
		ImageRef: "img:v1", VCPU: 1, MemMIB: 512, Port: 80, Status: "ACTIVE",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetAutoscalePolicy(ctx, appID, store.AutoscalePolicy{
		Enabled: true, MinReplicas: 1, MaxReplicas: 5,
		TargetConcurrency: 20, ScaleDownDelaySec: 120, PanicThreshold: 2.0,
	}); err != nil {
		t.Fatal(err)
	}
}

func writeSample(t *testing.T, rdb *redis.Client, host, field string, s autoscaleSample) {
	t.Helper()
	raw, _ := json.Marshal(s)
	if err := rdb.HSet(context.Background(), catalog.AutoscaleKey(host), field, string(raw)).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), catalog.AutoscaleKey(host)).Err() })
}

func fresh(ewma, rps float64, hard, unserved int64) autoscaleSample {
	return autoscaleSample{
		EWMA: ewma, RPS: rps, HardRejected: hard,
		Unserved: unserved, TsMs: time.Now().UnixMilli(), WinMs: 10000,
	}
}

func appDesired(t *testing.T, st *store.Store, appID string) int {
	t.Helper()
	app, err := st.GetApp(context.Background(), appID)
	if err != nil || app == nil {
		t.Fatalf("get app: %+v %v", app, err)
	}
	return app.DesiredReplicas
}

func countDecisionEvents(t *testing.T, st *store.Store, project, appID, action string) int {
	t.Helper()
	evs, err := st.ListUserEvents(context.Background(), store.UserEventFilter{
		ProjectID: project, AppID: appID, Type: store.UserEventAutoscaleDecision, Limit: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for i := range evs {
		var d struct {
			Action string `json:"action"`
		}
		_ = json.Unmarshal(evs[i].Details, &d)
		if action == "" || d.Action == action {
			n++
		}
	}
	return n
}

// 策略变更重置稳定窗 + 过期冻结惰性清理（审查 P2-2/P2-3 回归）。
func TestAutoscaleLoopPolicyChangeResetsWindow(t *testing.T) {
	c, st, rdb, project := autoscaleLoopFixtures(t)
	ctx := context.Background()
	appID, host := "app-as-loop-4", "as-loop-4.local"
	seedAutoscaleApp(t, st, project, appID, host, 3)
	writeSample(t, rdb, host, "edge-1", fresh(0, 0, 0, 0))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if !hasScaleDown(c, appID) {
		t.Fatal("stable window timer must start")
	}
	// 只改 delay（仍 enabled，want 不变）→ 计时清零。
	pol, _ := st.GetAutoscalePolicy(ctx, appID)
	pol.ScaleDownDelaySec = 30
	if err := st.SetAutoscalePolicy(ctx, appID, pol); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("policy change must restart window, desired=%d", got)
	}
	// 过期且未记 freeze 的冻结条目被惰性清理。
	c.autoscaleLoop().freezeApp(appID, time.Now().Add(-time.Second))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.autoscaleLoop().frozen(appID, time.Now()); ok {
		t.Fatal("expired unfrozen entry must be collected")
	}
}

// 阶跃扩容：负载 2x 后一拍跟上；信号丢失 hold；发布冻结。
func TestAutoscaleLoopScaleUpHoldFreeze(t *testing.T) {
	c, st, rdb, project := autoscaleLoopFixtures(t)
	ctx := context.Background()
	appID, host := "app-as-loop-1", "as-loop-1.local"
	seedAutoscaleApp(t, st, project, appID, host, 2)

	// 阶跃：served=45 → want=3，立即扩。
	writeSample(t, rdb, host, "edge-1", fresh(45, 90, 0, 0))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("desired=%d, want 3", got)
	}
	if n := countDecisionEvents(t, st, project, appID, "scale_up"); n != 1 {
		t.Fatalf("scale_up events=%d, want 1", n)
	}

	// 信号丢失（删 key）→ hold，不缩。
	if err := rdb.Del(ctx, catalog.AutoscaleKey(host)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("signal loss must hold at 3, got %d", got)
	}

	// 发布冻结：PREPARING 中高负载也不动。
	if err := st.CreateDeployment(ctx, store.Deployment{
		ID: "dep-" + appID + "-2", AppID: appID, Generation: 2,
		ImageRef: "img:v2", VCPU: 1, MemMIB: 512, Port: 80, Status: "PREPARING",
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRollout(ctx, store.Rollout{
		ID: "rl-" + appID, AppID: appID, FromGeneration: 1, ToGeneration: 2,
	}); err != nil {
		t.Fatal(err)
	}
	writeSample(t, rdb, host, "edge-1", fresh(80, 160, 0, 0))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("rollout must freeze at 3, got %d", got)
	}
	if err := st.CompleteRollout(ctx, appID, false); err != nil {
		t.Fatal(err)
	}
	// rollout 结束 → 下一拍恢复（want=ceil(80/20)=4）。
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 4 {
		t.Fatalf("desired=%d, want 4", got)
	}
}

// 慢缩与冷起：稳定窗到期才缩；min=0 缩零需 rps==0；unserved 冷起。
func TestAutoscaleLoopScaleDownAndColdStart(t *testing.T) {
	c, st, rdb, project := autoscaleLoopFixtures(t)
	ctx := context.Background()
	appID, host := "app-as-loop-2", "as-loop-2.local"
	seedAutoscaleApp(t, st, project, appID, host, 3)
	writeSample(t, rdb, host, "edge-1", fresh(0, 0, 0, 0))

	// 首拍只记时，不缩。
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("stable window must hold at 3, got %d", got)
	}
	// 时间快进（in-package 状态操作，模拟 120s 稳定窗走完）。
	setScaleDown(c, appID, 1, time.Now().Add(-121*time.Second))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 1 {
		t.Fatalf("desired=%d, want 1", got)
	}
	if n := countDecisionEvents(t, st, project, appID, "scale_down"); n != 1 {
		t.Fatalf("scale_down events=%d, want 1", n)
	}

	// min=0 + 在飞 rps → 不缩零。
	pol, _ := st.GetAutoscalePolicy(ctx, appID)
	pol.MinReplicas = 0
	if err := st.SetAutoscalePolicy(ctx, appID, pol); err != nil {
		t.Fatal(err)
	}
	if err := st.TakeoverScale(ctx, appID, 1); err != nil { // desired=1 且关 enabled
		t.Fatal(err)
	}
	if err := st.SetAutoscalePolicy(ctx, appID, pol); err != nil { // 重开
		t.Fatal(err)
	}
	writeSample(t, rdb, host, "edge-1", fresh(0, 5, 0, 0)) // rps>0
	setScaleDown(c, appID, 0, time.Now().Add(-200*time.Second))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 1 {
		t.Fatalf("inflight rps must block scale-to-zero, got %d", got)
	}
	// rps=0 → 缩零；再来 unserved → 30s 内冷起（本测即时）。
	writeSample(t, rdb, host, "edge-1", fresh(0, 0, 0, 0))
	setScaleDown(c, appID, 0, time.Now().Add(-200*time.Second))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 0 {
		t.Fatalf("desired=%d, want 0", got)
	}
	writeSample(t, rdb, host, "edge-1", fresh(0, 3, 0, 401)) // unserved 401 → 21 单位 → want=2
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 2 {
		t.Fatalf("cold start desired=%d, want 2", got)
	}
}

// 过期非零禁缩容；配额冻结只冻扩不冻缩 + 超时自解冻。
func TestAutoscaleLoopStaleAndQuota(t *testing.T) {
	c, st, rdb, project := autoscaleLoopFixtures(t)
	ctx := context.Background()
	appID, host := "app-as-loop-3", "as-loop-3.local"
	seedAutoscaleApp(t, st, project, appID, host, 3)

	// fresh 全零 + 过期非零 field → 禁缩。
	raw, _ := json.Marshal(fresh(0, 0, 0, 0))
	old, _ := json.Marshal(autoscaleSample{EWMA: 9, TsMs: time.Now().Add(-60 * time.Second).UnixMilli(), WinMs: 10000})
	if err := rdb.HSet(ctx, catalog.AutoscaleKey(host), "edge-live", string(raw), "edge-dead", string(old)).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), catalog.AutoscaleKey(host)).Err() })
	setScaleDown(c, appID, 1, time.Now().Add(-500*time.Second))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("stale nonzero must block shrink, got %d", got)
	}

	// 配额冻结：高负载不扩。
	writeSample(t, rdb, host, "edge-1", fresh(90, 180, 0, 0))
	c.noteQuotaRejection(appID)
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 3 {
		t.Fatalf("quota freeze must hold at 3, got %d", got)
	}
	if n := countDecisionEvents(t, st, project, appID, "freeze"); n != 1 {
		t.Fatalf("freeze events=%d, want 1", n)
	}
	// 缩容通道开放：零信号 + 窗口走完 → 照缩（先清过期 field）。
	if err := rdb.Del(ctx, catalog.AutoscaleKey(host)).Err(); err != nil {
		t.Fatal(err)
	}
	writeSample(t, rdb, host, "edge-1", fresh(0, 0, 0, 0))
	setScaleDown(c, appID, 1, time.Now().Add(-500*time.Second))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 1 {
		t.Fatalf("freeze must not block shrink, got %d", got)
	}
	// 超时自解冻 + 重探扩容。
	if err := rdb.Del(ctx, catalog.AutoscaleKey(host)).Err(); err != nil {
		t.Fatal(err)
	}
	c.autoscaleLoop().freezeApp(appID, time.Now().Add(-time.Second))
	writeSample(t, rdb, host, "edge-1", fresh(90, 180, 0, 0))
	if err := c.reconcileAutoscale(ctx); err != nil {
		t.Fatal(err)
	}
	if got := appDesired(t, st, appID); got != 5 {
		t.Fatalf("after unfreeze desired=%d, want 5", got)
	}
	if n := countDecisionEvents(t, st, project, appID, "unfreeze"); n != 1 {
		t.Fatalf("unfreeze events=%d, want 1", n)
	}
}

// P2-2：resources 连续拒绝冻结路径（notePlacementFailure 经本 op 的
// filter_rejection:resources:* 事件计数，3 次达阈值冻结扩容）。
func TestNotePlacementFailureFreezesAfterStreak(t *testing.T) {
	c, st, _, project := autoscaleLoopFixtures(t)
	ctx := context.Background()
	appID := "app-as-streak-1"
	op := store.Operation{ID: "op-streak-1", ProjectID: project, MachineID: "m-streak-1"}
	if err := st.RecordSchedulerEvent(ctx, store.SchedulerEvent{
		ProjectID: project, MachineID: "m-streak-1", OperationID: op.ID,
		Kind: "filter_rejection", Reason: "resources: need vcpu=2 mem=1024MiB",
	}); err != nil {
		t.Fatal(err)
	}
	c.notePlacementFailure(ctx, op, appID)
	c.notePlacementFailure(ctx, op, appID)
	if _, ok := c.autoscaleLoop().frozen(appID, time.Now()); ok {
		t.Fatal("2 consecutive resources rejections must not freeze yet")
	}
	c.notePlacementFailure(ctx, op, appID)
	if _, ok := c.autoscaleLoop().frozen(appID, time.Now()); !ok {
		t.Fatal("3rd consecutive resources rejection must freeze scale-up")
	}
	// 非 resources 原因不计数。
	op2 := store.Operation{ID: "op-streak-2", ProjectID: project, MachineID: "m-streak-2"}
	if err := st.RecordSchedulerEvent(ctx, store.SchedulerEvent{
		ProjectID: project, MachineID: "m-streak-2", OperationID: op2.ID,
		Kind: "filter_rejection", Reason: "pool: node pool mismatch",
	}); err != nil {
		t.Fatal(err)
	}
	c.notePlacementFailure(ctx, op2, "app-as-streak-2")
	if _, ok := c.autoscaleLoop().frozen("app-as-streak-2", time.Now()); ok {
		t.Fatal("non-resources rejection must not freeze")
	}
	// 成功派发清计数。
	c.notePlacementSuccess(appID)
	c.autoscaleLoop().clearFreeze(appID)
	if _, ok := c.autoscaleLoop().frozen(appID, time.Now()); ok {
		t.Fatal("freeze must clear")
	}
}
