package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// autoscale.go：ADR-0041 并发自动弹性——leader 内决策循环（KPA 式）。
//
// 决策只写既有 desired_replicas（条件 CAS），复用 reconcileAppScale /
// rollout / placement / quota / fencing 全链路。决策状态不持久化（leader
// 进程内有界 map；切换/重启后重置 = 推迟缩容，保守方向）。

// autoscaleSample 是信号线格式（catalog.SignalSample 别名：
// 与 edge reporter 同一定义，P2-5）。
type autoscaleSample = catalog.SignalSample

// AutoscaleSignal 是单 app 聚合后的负载信号（controller 侧只读）。
type AutoscaleSignal struct {
	Served      float64 // fresh ewma 之和
	Unserved    float64 // ceil(fresh unserved 滚动和 / target)
	TotalRPS    float64 // fresh rps 之和
	PanicReject bool    // 任一 fresh field hard_rejected>0
	HasFresh    bool    // 存在至少一个 fresh field（含全零——合法 idle 信号）
	// HasStaleNonzero：存在过期且非零 field → 禁止缩容（fail-closed：
	// 分区 edge 仍在 serve-stale 服务，其负载不可见，缩容会删掉它正在
	// 服务的副本；key TTL 被 live edge 刷新时该冻结是故意的）。
	HasStaleNonzero bool
}

// 信号新鲜度窗口（§2）：容忍小时钟偏差与轻微未来时间。
const (
	autoscaleFreshPast   = 20 * time.Second
	autoscaleFreshFuture = 5 * time.Second
)

// autoscaleResourceStreakThreshold 是连续 resources 拒绝触发扩容冻结的次数。
const autoscaleResourceStreakThreshold = 3

// autoscalePrefetchCooldown 是同 deployment 自动预取的去重冷却
// （独立于 rollout 级 prefetchedRollouts，不得复用——键空间不同）。
const autoscalePrefetchCooldown = 5 * time.Minute

func autoscaleSampleFresh(tsMs int64, now time.Time) bool {
	ts := time.UnixMilli(tsMs)
	return now.Add(-autoscaleFreshPast).Compare(ts) <= 0 && ts.Compare(now.Add(autoscaleFreshFuture)) <= 0
}

// aggregateAutoscaleSignal 把一 app 全部已知 hostname 的 field 集聚合成
// 单一信号（每请求按 Host 单归属记账，每个 hostname 只被求和一次，
// 不随 hostname 数放大——§4.7）。
func aggregateAutoscaleSignal(fieldSets []map[string]string, target int, now time.Time) AutoscaleSignal {
	var sig AutoscaleSignal
	if target < 1 {
		target = 1
	}
	var unservedSum int64
	for _, fields := range fieldSets {
		for _, raw := range fields {
			var s autoscaleSample
			if err := json.Unmarshal([]byte(raw), &s); err != nil {
				continue // 脏 field 跳过（不污染聚合）
			}
			if !autoscaleSampleFresh(s.TsMs, now) {
				if s.EWMA != 0 || s.RPS != 0 || s.HardRejected != 0 || s.Unserved != 0 {
					sig.HasStaleNonzero = true
				}
				continue // 过期全零 field 视为缺席
			}
			sig.HasFresh = true
			sig.Served += s.EWMA
			sig.TotalRPS += s.RPS
			unservedSum += s.Unserved
			if s.HardRejected > 0 {
				sig.PanicReject = true
			}
		}
	}
	sig.Unserved = math.Ceil(float64(unservedSum) / float64(target))
	return sig
}

// ceilDiv 是整数上除（load=0 → want=0，冷起/缩零与同一公式共用）。
func ceilDiv(a, b int) int {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

// computeAutoscaleWant 是决策核（纯函数）：load=served+unserved，无容量
// 流量也是负载；panic 双路只放大。eff<=0 时 panic 路关闭（零容量冷起走
// 正常 want，不走快扩——到零后 unserved 即负载，自然 want>=1）。
// panicTriggered 表示 panic 条件已触发（事件 reason 用），即使放大值与 want
// 持平（阈值恰好落在 ceil 边界上）也为 true。
func computeAutoscaleWant(policy store.AutoscalePolicy,
	sig AutoscaleSignal, ready, warming int,
) (want int, panicTriggered bool) {
	eff := ready + warming
	load := sig.Served + sig.Unserved
	want = ceilDiv(int(math.Ceil(load)), policy.TargetConcurrency)
	if want < policy.MinReplicas {
		want = policy.MinReplicas
	}
	if want > policy.MaxReplicas {
		want = policy.MaxReplicas
	}
	if eff > 0 && (sig.PanicReject ||
		load > policy.PanicThreshold*float64(policy.TargetConcurrency*eff)) {
		boosted := max(want, int(math.Ceil(policy.PanicThreshold*float64(eff))), eff+2)
		if boosted > policy.MaxReplicas {
			boosted = policy.MaxReplicas
		}
		if boosted > want {
			want = boosted
		}
		return want, true
	}
	return want, false
}

// scaleDownEntry 是 KPA 稳定窗计时（同一 want 持续 scale_down_delay 才放行，
// 直接缩到 want；任何 want>=desired/hold/策略变/接管/rollout 出現结束清零）。
type scaleDownEntry struct {
	want  int
	since time.Time
}

// autoscaleLoopState 是 leader 进程内有界决策状态（不持久化）。
//
// 并发纪律（P0-1）：dispatch worker（processCreate 经 noteQuotaRejection /
// notePlacementSuccess / notePlacementFailure 写入）与 leader 主循环
// （reconcileAutoscale 读写）并发访问本结构——全部经方法 + 内嵌 RWMutex，
// 禁止外部直接碰 map（Go map 并发写是 fatal error，不可 recover）。
// 决策 IO（PG/Redis）一律在锁外，方法只做短临界区，比对-设置语义内聚。
type autoscaleLoopState struct {
	mu             sync.RWMutex
	scaleDown      map[string]scaleDownEntry
	frozenUntil    map[string]time.Time
	frozenNoted    map[string]bool
	resourceStreak map[string]int
	prefetched     map[string]time.Time
	// policyHash 感知策略变更（PUT 与 CAS 跨进程，决策侧只能拍间比对；
	// 变化即重置稳定窗——ADR §3“策略变更清零计时”。）
	policyHash map[string]string
}

// clearApp 清除单 app 全部决策状态（策略关闭/删除/回收）。
func (s *autoscaleLoopState) clearApp(appID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.scaleDown, appID)
	delete(s.frozenNoted, appID)
	delete(s.frozenUntil, appID)
	delete(s.resourceStreak, appID)
	delete(s.policyHash, appID)
}

// checkPolicy 比对策略指纹：变化即重置稳定窗，返回是否变化。
func (s *autoscaleLoopState) checkPolicy(appID string, policy store.AutoscalePolicy) bool {
	h := autoscalePolicyHash(policy)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.policyHash[appID] == h {
		return false
	}
	s.policyHash[appID] = h
	delete(s.scaleDown, appID)
	return true
}

// collectExpiredFreeze 惰性清理过期且未记事件的冻结条目。
func (s *autoscaleLoopState) collectExpiredFreeze(appID string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if until, ok := s.frozenUntil[appID]; ok && !now.Before(until) && !s.frozenNoted[appID] {
		delete(s.frozenUntil, appID)
	}
}

// freezeApp 冻结扩容至 until。
func (s *autoscaleLoopState) freezeApp(appID string, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frozenUntil[appID] = until
}

// frozenUntil 报告扩容冻结到期时间（ok=false = 未冻结或已过期）。
func (s *autoscaleLoopState) frozen(appID string, now time.Time) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	until, ok := s.frozenUntil[appID]
	if !ok || !now.Before(until) {
		return time.Time{}, false
	}
	return until, true
}

// markFreezeNoted 原子标记已发 freeze 事件（返回此前是否已标记）。
func (s *autoscaleLoopState) markFreezeNoted(appID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frozenNoted[appID] {
		return true
	}
	s.frozenNoted[appID] = true
	return false
}

// isFreezeNoted 报告是否已发 freeze 事件。
func (s *autoscaleLoopState) isFreezeNoted(appID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.frozenNoted[appID]
}

// clearFreeze 清除冻结及其事件标记（解冻）。
func (s *autoscaleLoopState) clearFreeze(appID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.frozenUntil, appID)
	delete(s.frozenNoted, appID)
}

// deleteScaleDown 清除稳定窗计时。
func (s *autoscaleLoopState) deleteScaleDown(appID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.scaleDown, appID)
}

// scaleDownElapsed：want<desired 首次出现记时；同一 want 持续 delay 后放行。
func (s *autoscaleLoopState) scaleDownElapsed(appID string,
	want int, now time.Time, delaySec int,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.scaleDown[appID]; ok && e.want == want {
		return now.Sub(e.since) >= time.Duration(delaySec)*time.Second
	}
	s.scaleDown[appID] = scaleDownEntry{want: want, since: now}
	return false
}

// bumpStreak 累计 resources 连续拒绝；达阈值即冻结扩容，返回累计值。
func (s *autoscaleLoopState) bumpStreak(appID string, freezeFor time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resourceStreak[appID]++
	n := s.resourceStreak[appID]
	if n >= autoscaleResourceStreakThreshold {
		s.frozenUntil[appID] = time.Now().Add(freezeFor)
	}
	return n
}

// clearStreak 清除 resources 连续拒绝计数。
func (s *autoscaleLoopState) clearStreak(appID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.resourceStreak, appID)
}

// prefetchAllowed 按 deployment 限频预取（命中冷却返回 false）。
func (s *autoscaleLoopState) prefetchAllowed(depID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if last, ok := s.prefetched[depID]; ok && now.Sub(last) < autoscalePrefetchCooldown {
		return false
	}
	s.prefetched[depID] = now
	if len(s.prefetched) > 1024 {
		s.prefetched = map[string]time.Time{depID: now}
	}
	return true
}

// purgeUnknown 回收已不在存量 app 列表中的条目（删除 app 的内存兜底；
// ListApps 只返回未删除行，删除后不再经过 clearApp 分支）。
func (s *autoscaleLoopState) purgeUnknown(known map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.scaleDown {
		if !known[id] {
			delete(s.scaleDown, id)
		}
	}
	for id := range s.frozenUntil {
		if !known[id] {
			delete(s.frozenUntil, id)
		}
	}
	for id := range s.frozenNoted {
		if !known[id] {
			delete(s.frozenNoted, id)
		}
	}
	for id := range s.resourceStreak {
		if !known[id] {
			delete(s.resourceStreak, id)
		}
	}
	for id := range s.policyHash {
		if !known[id] {
			delete(s.policyHash, id)
		}
	}
}

func newAutoscaleLoopState() *autoscaleLoopState {
	return &autoscaleLoopState{
		scaleDown:      map[string]scaleDownEntry{},
		frozenUntil:    map[string]time.Time{},
		frozenNoted:    map[string]bool{},
		resourceStreak: map[string]int{},
		prefetched:     map[string]time.Time{},
		policyHash:     map[string]string{},
	}
}

// autoscaleResult 是 firepaas_autoscale_decisions_total 的 result 取值
// （ADR-0027 §8 收敛：不设带 app label 的 desired gauge）。
const (
	autoscaleResultScaleUp     = "scale_up"
	autoscaleResultScaleDown   = "scale_down"
	autoscaleResultHoldSignal  = "hold_signal"
	autoscaleResultHoldRollout = "hold_rollout"
	autoscaleResultHoldQuota   = "hold_quota"
	autoscaleResultConflict    = "conflict"
	autoscaleResultError       = "error"
)

// autoscaleLoop 返回 leader 进程内决策状态。指针懒初始化受 autoscaleMu
// 保护（dispatch worker 与主循环并发首触安全）；返回后全部状态访问经
// autoscaleLoopState 内嵌 RWMutex 的方法。
func (c *Controller) autoscaleLoop() *autoscaleLoopState {
	c.autoscaleMu.Lock()
	defer c.autoscaleMu.Unlock()
	if c.autoscale == nil {
		c.autoscale = newAutoscaleLoopState()
	}
	return c.autoscale
}

func (c *Controller) autoscaleInterval() time.Duration {
	if c.cfg.AutoscaleInterval > 0 {
		return c.cfg.AutoscaleInterval
	}
	return 10 * time.Second
}

func (c *Controller) autoscaleQuotaFreeze() time.Duration {
	if c.cfg.AutoscaleQuotaFreeze > 0 {
		return c.cfg.AutoscaleQuotaFreeze
	}
	return 10 * c.autoscaleInterval()
}

func (c *Controller) autoscaleCreateTimeout() time.Duration {
	if c.cfg.AgentRPCTimeout > 0 {
		return c.cfg.AgentRPCTimeout
	}
	return 2 * time.Minute
}

// noteQuotaRejection 记录本 app 的配额拒绝（processCreate 终态 FAILED 路径
// 调用）：冻结扩容，缩容保持开放（缩释放配额，是自愈方向）。
func (c *Controller) noteQuotaRejection(appID string) {
	if appID == "" {
		return
	}
	st := c.autoscaleLoop()
	st.freezeApp(appID, time.Now().Add(c.autoscaleQuotaFreeze()))
	st.clearStreak(appID)
}

// notePlacementSuccess 清除本 app 的 resources 连续拒绝计数（任一次成功
// 派发证明有容量）。
func (c *Controller) notePlacementSuccess(appID string) {
	if appID == "" {
		return
	}
	c.autoscaleLoop().clearStreak(appID)
}

// notePlacementFailure 在 placement 失败路径调用：检查本 op 的
// filter_rejection:resources:* 事件（placement 已在返回错误前落库），
// 连续达阈值后冻结扩容（同配额冻结语义）。
func (c *Controller) notePlacementFailure(ctx context.Context, op store.Operation, appID string) {
	if appID == "" {
		return
	}
	evs, err := c.store.ListSchedulerEvents(ctx, op.ProjectID, 50)
	if err != nil {
		return
	}
	for i := range evs {
		e := &evs[i]
		if e.OperationID == op.ID && e.Kind == "filter_rejection" &&
			strings.HasPrefix(e.Reason, "resources:") {
			c.autoscaleLoop().bumpStreak(appID, c.autoscaleQuotaFreeze())
			return
		}
	}
}

// reconcileAutoscale 是 10s 一拍的决策循环（只读 enabled app，只经 CAS 写）。
func (c *Controller) reconcileAutoscale(ctx context.Context) error {
	now := time.Now()
	apps, err := c.store.ListApps(ctx)
	if err != nil {
		return err // 失败 → 全量跳过，fail-closed
	}
	st := c.autoscaleLoop()
	known := make(map[string]bool, len(apps))
	for i := range apps {
		known[apps[i].ID] = true
	}
	st.purgeUnknown(known)
	// 在途 create 取证（一次查询，供全部 app 的 warming 判定）。
	inflightCreate := map[string]bool{}
	if ops, err := c.store.ListInFlightOperations(ctx); err == nil {
		for i := range ops {
			if ops[i].Kind == "create" {
				inflightCreate[ops[i].MachineID] = true
			}
		}
	}
	// DRAINING 节点集合（ID + NomadID 双空间，machine.NodeID 写法历史不一）。
	draining := map[string]bool{}
	if nodes, err := c.store.ListNodes(ctx); err == nil {
		for i := range nodes {
			if nodes[i].Draining {
				draining[nodes[i].ID] = true
				draining[nodes[i].NomadNodeID] = true
			}
		}
	}
	for i := range apps {
		app := &apps[i]
		c.reconcileAutoscaleApp(ctx, app, st, now, inflightCreate, draining)
	}
	return nil
}

// autoscaleHold 记录一次 hold（只进指标与 debug 日志，不落库；
// app/hostname 进结构化字段，detail 区分去向：无信号/过期/稳定窗/配额等）。
func (c *Controller) autoscaleHold(result string, app *store.App, detail string) {
	c.metrics.Inc("firepaas_autoscale_decisions_total", map[string]string{"result": result}, 1)
	slog.Debug("autoscale hold", "result", result,
		"app_id", app.ID, "hostname", app.Hostname, "detail", detail)
}

func (c *Controller) reconcileAutoscaleApp(ctx context.Context, app *store.App,
	st *autoscaleLoopState, now time.Time,
	inflightCreate, draining map[string]bool,
) {
	policy := app.Autoscale()
	if !policy.Enabled || app.Deleted {
		// 策略关闭/删除 = 计时清零（下次启用重新稳定窗）。
		st.clearApp(app.ID)
		return
	}
	// 策略参数变更（仍 enabled）同样重置稳定窗；过期冻结条目惰性清理
	// （无论是否记过 freeze 事件，不泄漏）。
	st.checkPolicy(app.ID, policy)
	st.collectExpiredFreeze(app.ID, now)
	target, err := c.targetDeployment(ctx, app)
	if err != nil || target == nil {
		c.autoscaleHold(autoscaleResultError, app, "target deployment missing")
		return
	}
	// 发布互斥：活跃 rollout（含查询失败 fail-closed）期间冻结。
	rl, err := c.store.ActiveRolloutForApp(ctx, app.ID)
	if err != nil || rl != nil {
		st.deleteScaleDown(app.ID)
		c.autoscaleHold(autoscaleResultHoldRollout, app, "rollout active or unknown")
		return
	}
	fields, err := c.routesCatalog().GetAutoscaleFields(ctx, app.Hostname)
	if err != nil {
		st.deleteScaleDown(app.ID)
		c.autoscaleHold(autoscaleResultHoldSignal, app, "signal read failed")
		return
	}
	sig := aggregateAutoscaleSignal([]map[string]string{fields}, policy.TargetConcurrency, now)
	if !sig.HasFresh {
		st.deleteScaleDown(app.ID)
		c.autoscaleHold(
			autoscaleResultHoldSignal,
			app,
			"no fresh signal (edge restarted or never reported recovers on next real request)",
		)
		return
	}
	machines, err := c.store.ListMachinesForApp(ctx, app.ID, false)
	if err != nil {
		st.deleteScaleDown(app.ID)
		c.autoscaleHold(autoscaleResultError, app, "list machines failed")
		return
	}
	ready, warming := 0, 0
	createTimeout := c.autoscaleCreateTimeout()
	for i := range machines {
		m := &machines[i]
		if m.DeploymentID != target.ID {
			continue // 只看目标代
		}
		if machineServing(*m) {
			if !draining[m.NodeID] {
				ready++
			}
			continue
		}
		// warming：冷启中不重复下单（超龄的不占有效容量）。
		if m.DesiredState == "DELETED" {
			continue
		}
		if m.ObservedState == "STOPPED" || m.ObservedState == "STOPPING" {
			continue
		}
		if now.Sub(m.CreatedAt) <= createTimeout || inflightCreate[m.ID] {
			warming++
		}
	}
	want, panicTriggered := computeAutoscaleWant(policy, sig, ready, warming)
	sigDetail := map[string]any{
		"served": sig.Served, "unserved": sig.Unserved, "rps": sig.TotalRPS,
		"hard_rejected": sig.PanicReject, "ready": ready, "warming": warming,
	}
	// 配额/资源冻结：只冻扩不冻缩（缩是自愈方向）。
	if _, frozen := st.frozen(app.ID, now); frozen {
		if want > app.DesiredReplicas {
			if !st.markFreezeNoted(app.ID) {
				c.autoscaleEvent(ctx, app, "freeze", app.DesiredReplicas, want, "quota", sigDetail)
			}
			st.deleteScaleDown(app.ID)
			c.autoscaleHold(autoscaleResultHoldQuota, app, "quota frozen")
			return
		}
	} else if st.isFreezeNoted(app.ID) {
		// 冻结超时自动解冻重探（重探一次是安全的；leader 切换后重置同理）。
		st.clearFreeze(app.ID)
		c.autoscaleEvent(ctx, app, "unfreeze", app.DesiredReplicas, want, "quota_recovered", sigDetail)
	}
	switch {
	case want > app.DesiredReplicas: // 快扩：立即
		reason := "load"
		if panicTriggered {
			reason = "panic"
		}
		ok, err := c.store.SetAppReplicasCAS(ctx, app.ID, want, app.DesiredReplicas)
		if err != nil {
			c.autoscaleHold(autoscaleResultError, app, "cas store error")
			return
		}
		st.deleteScaleDown(app.ID)
		if !ok {
			// 策略已变/手动接管/其他写者：跳过并计数，绝不覆盖。
			c.autoscaleHold(autoscaleResultConflict, app, "cas conflict")
			return
		}
		c.metrics.Inc("firepaas_autoscale_decisions_total",
			map[string]string{"result": autoscaleResultScaleUp}, 1)
		c.autoscaleEvent(ctx, app, "scale_up", app.DesiredReplicas, want, reason, sigDetail)
		c.maybePrefetchAutoscale(ctx, target)
	case want < app.DesiredReplicas: // 慢缩：稳定窗全程成立
		if sig.HasStaleNonzero {
			st.deleteScaleDown(app.ID)
			c.autoscaleHold(autoscaleResultHoldSignal, app, "stale nonzero signal")
			return
		}
		if !st.scaleDownElapsed(app.ID, want, now, policy.ScaleDownDelaySec) {
			c.autoscaleHold(autoscaleResultHoldSignal, app, "stable window")
			return
		}
		if want == 0 && sig.TotalRPS > 0 {
			// 在飞请求：重置稳定窗（它确实在服务流量）。
			st.deleteScaleDown(app.ID)
			c.autoscaleHold(autoscaleResultHoldSignal, app, "inflight rps blocks scale-to-zero")
			return
		}
		ok, err := c.store.SetAppReplicasCAS(ctx, app.ID, want, app.DesiredReplicas)
		if err != nil {
			c.autoscaleHold(autoscaleResultError, app, "cas store error")
			return
		}
		st.deleteScaleDown(app.ID)
		if !ok {
			c.autoscaleHold(autoscaleResultConflict, app, "cas conflict")
			return
		}
		c.metrics.Inc("firepaas_autoscale_decisions_total",
			map[string]string{"result": autoscaleResultScaleDown}, 1)
		c.autoscaleEvent(ctx, app, "scale_down", app.DesiredReplicas, want, "below_target", sigDetail)
	default:
		st.deleteScaleDown(app.ID)
	}
}

// autoscalePolicyHash 是策略变更感知用的指纹（浮点用 %v，跨拍稳定）。
func autoscalePolicyHash(p store.AutoscalePolicy) string {
	return fmt.Sprintf("%v|%d|%d|%d|%d|%v",
		p.Enabled, p.MinReplicas, p.MaxReplicas, p.TargetConcurrency,
		p.ScaleDownDelaySec, p.PanicThreshold)
}

// autoscaleEvent 写状态变化事件（扩/缩/冻结/解冻；hold 只进指标与 debug 日志）。
func (c *Controller) autoscaleEvent(ctx context.Context, app *store.App, action string,
	from, to int, reason string, sig map[string]any,
) {
	slog.Info("autoscale decision", "app_id", app.ID, "hostname", app.Hostname,
		"action", action, "from", from, "to", to, "reason", reason, "sig", sig)
	c.userEvent(ctx, app.ProjectID, app.ID, "", store.UserEventAutoscaleDecision, map[string]any{
		"action": action, "from": from, "to": to, "reason": reason, "sig": sig,
	})
}

// routesCatalog 返回信号读取用的 catalog（经小方法隔离以便单测替换——
// 生产恒为 c.cat，由 New 装配）。
func (c *Controller) routesCatalog() autoscaleFieldReader {
	return c.cat
}

// autoscaleFieldReader 是信号读取的最小面。
type autoscaleFieldReader interface {
	GetAutoscaleFields(ctx context.Context, hostname string) (map[string]string, error)
}

// maybePrefetchAutoscale 在扩容时顺手预取（尽力而为，不阻塞决策；独立于
// rollout 级去重表，按 deployment_id + 冷却去重并限频）。
func (c *Controller) maybePrefetchAutoscale(ctx context.Context, target *store.Deployment) {
	if c.placer == nil || c.placement == nil || target == nil {
		return
	}
	if !c.autoscaleLoop().prefetchAllowed(target.ID, time.Now()) {
		return
	}
	nodes, req, err := c.prefetchCandidates(ctx, target)
	if err != nil {
		slog.Debug("autoscale prefetch candidates", "error", err)
		return
	}
	top := c.placer.PrefetchTopK(req, nodes, c.cfg.PrefetchTopK)
	if len(top) == 0 {
		return
	}
	// 限频标记已在入口 prefetchAllowed 内原子完成（含 1024 上限重置）；
	// 暂态失败不单独续期（下次扩容决策按冷却重探，拉取幂等故重发无害）。
	slog.Info("autoscale prefetch dispatched", "deployment_id", target.ID, "nodes", len(top))
	for _, n := range top {
		v := c.viewForAgent(n.ID)
		if v == nil {
			continue
		}
		client := c.nodes.ClientFor(v.nomadID)
		if client == nil {
			continue
		}
		go func() {
			pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			if _, err := client.PullImage(pctx, target.ImageRef); err != nil {
				c.metrics.Inc("firepaas_prefetch_total", map[string]string{"result": "failed"}, 1)
				return
			}
			c.metrics.Inc("firepaas_prefetch_total", map[string]string{"result": "succeeded"}, 1)
		}()
	}
}
