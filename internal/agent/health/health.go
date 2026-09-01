// Package health 是 agent 的 host 侧就绪探针执行器（ADR-0008 M3 落地）。
//
// 归属：agent 在 host 侧经 workload endpoint（bridge guest IP / slot 路由）
// 对 MachineSpec.health_check 声明的 target 执行 HTTP/TCP 探针，复用 hypeman
// lib/healthcheck 的 Policy/阈值/Runtime 语义；结果映射为
// Machine.readiness（UNKNOWN/NOT_READY/READY/UNCONFIGURED），是 controller
// 更新 route_backends.readiness 的唯一来源。
//
// 执行模型（P1-1 修复）：探针由独立 Worker 周期执行，与 ListMachines gRPC
// 路径完全解耦——此前的内联实现共享 gRPC 的 10s deadline，副本数一多
// （API 允许 replicas≤100）探针会耗尽预算，后续机器的探针因 ctx 到期判
// 失败，readiness 抖动甚至触发节点摘路由。Worker 自带每轮探针预算
// （默认 8s）与单探针超时，超出预算的机器本轮跳过、沿用上次结果。
// List 路径只做 Observe（注册实例视图）+ Readiness（读缓存）。
package health

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/kernel/hypeman/lib/healthcheck"
	"github.com/kernel/hypeman/lib/instances"

	"github.com/zhu327/firepaas/internal/agent/probeflow"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PolicyJSON 是 tagHealth 里存储的探针策略（agent 内部格式，hypeman 子集）。
type PolicyJSON struct {
	Type             string `json:"type"` // http | tcp（exec 在 M3 不支持）
	Port             int    `json:"port,omitempty"`
	Path             string `json:"path,omitempty"`
	Scheme           string `json:"scheme,omitempty"`
	ExpectedStatus   int    `json:"expected_status,omitempty"`
	IntervalSeconds  int    `json:"interval_seconds,omitempty"`
	TimeoutSeconds   int    `json:"timeout_seconds,omitempty"`
	StartPeriodSec   int    `json:"start_period_seconds,omitempty"`
	FailureThreshold int    `json:"failure_threshold,omitempty"`
	SuccessThreshold int    `json:"success_threshold,omitempty"`
}

type entry struct {
	policy    *healthcheck.Policy
	runtime   *healthcheck.Runtime
	readiness pb.MachineReadiness
	changedAt time.Time
	lastProbe time.Time
	startedAt time.Time
	// inst 是最近一次 Observe 的实例视图（探针输入），只读。
	inst instances.Instance
}

// Tracker 维护每台 machine 的探针状态。
type Tracker struct {
	mu     sync.Mutex
	byID   map[string]*entry
	runner healthcheck.ProbeRunner
	now    func() time.Time
}

// New 构造 Tracker。
func New() *Tracker {
	return &Tracker{
		byID:   map[string]*entry{},
		runner: healthcheck.DefaultProbeRunner{HTTPClient: probeHTTPClient()},
		now:    time.Now,
	}
}

// Observe 观测一台实例：注册/更新策略与实例视图（探针输入）。探针执行由
// Worker 独立完成；ListMachines 路径调用本函数是 O(1) 的加锁拷贝。
// 并发安全。
func (t *Tracker) Observe(ctx context.Context, inst *instances.Instance) {
	if inst == nil {
		return
	}
	id := inst.Name
	if id == "" {
		id = inst.Id
	}
	if id == "" {
		return
	}
	policy := ParsePolicy(inst.Tags[tagHealth])
	now := t.now()

	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.byID[id]
	if !ok {
		e = &entry{
			readiness: pb.MachineReadiness_UNKNOWN,
			changedAt: now,
		}
		t.byID[id] = e
	}
	// P2-2：新 execution（实例重建，CreatedAt 变晚）必须重置探针状态机。
	// hypeman ApplyProbeResult 只在 StartedAt 为 nil 时设置，保留旧 runtime
	// 会让新代 VM 在 FailureThreshold×interval 内虚报上一代的 READY
	// （PREPARING 期间重建会提前切流）。
	if inst.CreatedAt.After(e.startedAt) {
		e.runtime = nil
		e.readiness = pb.MachineReadiness_UNKNOWN
		e.changedAt = now
	}
	e.policy = policy
	if !inst.CreatedAt.IsZero() {
		e.startedAt = inst.CreatedAt
	}
	e.inst = *inst
}

// Readiness 返回 machine 当前就绪状态（未观测过返回 UNKNOWN）。
func (t *Tracker) Readiness(machineID string) (pb.MachineReadiness, *timestamppb.Timestamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.byID[machineID]
	if !ok {
		return pb.MachineReadiness_UNKNOWN, nil
	}
	if e.policy == nil {
		return pb.MachineReadiness_UNCONFIGURED, timestamppb.New(e.changedAt)
	}
	return e.readiness, timestamppb.New(e.changedAt)
}

// Remove 删除跟踪状态（机器删除后调用）。
func (t *Tracker) Remove(machineID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.byID, machineID)
}

// Count 返回跟踪的机器数（观测用）。
func (t *Tracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.byID)
}

// Worker 周期执行到期探针。零依赖 agentd 主循环，Stop 后可重复启动。
type Worker struct {
	tracker *Tracker
	// List 是实时实例视图来源（agentd 侧注入 hypeman ListInstances）。
	list func(ctx context.Context) ([]instances.Instance, error)
	// 每轮探针总预算（默认 8s）；超出预算的机器本轮跳过。
	probeBudget time.Duration
	// 轮询节奏（默认 1s）。
	tick time.Duration
}

// NewWorker 构造探针 worker。list 返回全部实例（含 tag/IP/State）。
func NewWorker(tracker *Tracker, list func(ctx context.Context) ([]instances.Instance, error)) *Worker {
	return &Worker{
		tracker:     tracker,
		list:        list,
		probeBudget: 8 * time.Second,
		tick:        time.Second,
	}
}

// Run 阻塞执行探针循环直到 ctx 取消。每轮：
//  1. list 实例 → Observe 注册（保持 tracker 视图新鲜，含执行换代重置）；
//  2. 对到期（now-lastProbe ≥ interval）且 RUNNING 的实例执行探针，
//     受 probeBudget 预算约束。
func (w *Worker) Run(ctx context.Context) error {
	t := w.tracker
	ticker := time.NewTicker(w.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if w.list == nil {
			continue
		}
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		listed, err := w.list(listCtx)
		cancel()
		if err != nil {
			continue
		}
		for i := range listed {
			t.Observe(ctx, &listed[i])
		}
		w.probeDue(ctx)
	}
}

// probeDue 执行到期探针（预算内）。快照后无锁执行网络 IO，回写加锁。
func (w *Worker) probeDue(ctx context.Context) {
	t := w.tracker
	now := t.now()

	type job struct {
		id     string
		policy *healthcheck.Policy
		inst   instances.Instance
	}
	var jobs []job
	t.mu.Lock()
	for id, e := range t.byID {
		if e.policy == nil || e.inst.Name == "" && e.inst.Id == "" {
			continue
		}
		if !isRunning(e.inst.State) {
			continue
		}
		if !e.lastProbe.IsZero() && now.Sub(e.lastProbe) < policyInterval(e.policy) {
			continue
		}
		e.lastProbe = now // 预约本轮，避免下一 tick 重复排队
		jobs = append(jobs, job{id: id, policy: e.policy, inst: e.inst})
	}
	t.mu.Unlock()

	budget := w.probeBudget
	if budget <= 0 {
		budget = 8 * time.Second
	}
	deadline := time.Now().Add(budget)
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// P1-1：预算耗尽，剩余机器本轮跳过（保留 lastProbe 预约，
			// 下一轮再探，readiness 沿用上次值不抖动）。
			return
		}
		timeout := policyTimeout(j.policy)
		if timeout > remaining {
			timeout = remaining
		}
		hcInst := healthcheck.Instance{
			ID:             j.id,
			Name:           j.id,
			State:          healthcheck.StateRunning,
			NetworkEnabled: j.inst.NetworkEnabled,
			IP:             j.inst.IP,
			StartedAt:      &j.inst.CreatedAt,
		}
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		result := t.runner.Check(probeCtx, hcInst, j.policy)
		cancel()

		t.mu.Lock()
		if e, ok := t.byID[j.id]; ok {
			e.runtime = healthcheck.ApplyProbeResult(j.policy, hcInst, e.runtime, now, result)
			readiness := readinessFromRuntime(j.policy, hcInst, e.runtime, now)
			if readiness != e.readiness {
				e.readiness = readiness
				e.changedAt = now
			}
		}
		t.mu.Unlock()
	}
}

const tagHealth = "firepaas/health_check"

// ParsePolicy 把 tag 里的策略解析成 hypeman Policy；空/非法返回 nil
// （未声明探针，UNCONFIGURED 语义）。tag 值受 hypeman 校验限制
// （[A-Za-z0-9 _.:/=+@-]{0,256}），JSON 以 base64url 编码。
func ParsePolicy(tag string) *healthcheck.Policy {
	if tag == "" || tag == "0" || tag == "null" || tag == "1" {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(tag)
	if err != nil {
		return nil
	}
	var pj PolicyJSON
	if err := json.Unmarshal(raw, &pj); err != nil {
		return nil
	}
	return toHypemanPolicy(&pj)
}

// EncodePolicy 把 proto HealthCheckSpec 编码为 tag JSON（adapter.Create 用）。
// 返回空串表示未声明。EXEC 在 M3 不支持（需要 vsock guest agent 通道），
// 显式返回错误由调用方决定降级为 UNCONFIGURED。
func EncodePolicy(h *pb.HealthCheckSpec) (string, error) {
	if h == nil || h.Type == pb.HealthCheckSpec_TYPE_UNSPECIFIED {
		return "", nil
	}
	pj := PolicyJSON{
		IntervalSeconds:  int(h.IntervalSeconds),
		TimeoutSeconds:   int(h.TimeoutSeconds),
		FailureThreshold: int(h.UnhealthyThreshold),
		SuccessThreshold: 1,
		StartPeriodSec:   30,
	}
	switch h.Type {
	case pb.HealthCheckSpec_HTTP:
		u, err := url.Parse(h.Target)
		if err != nil || u.Scheme != "http" && u.Scheme != "https" {
			return "", fmt.Errorf("health: bad http target %q", h.Target)
		}
		pj.Type = "http"
		pj.Scheme = u.Scheme
		pj.Path = u.Path
		if pj.Path == "" {
			pj.Path = "/"
		}
		port, err := strconv.Atoi(u.Port())
		if err != nil || port == 0 {
			if u.Scheme == "https" {
				port = 443
			} else {
				port = 80
			}
		}
		pj.Port = port
		pj.ExpectedStatus = 200
	case pb.HealthCheckSpec_TCP:
		u, err := url.Parse(h.Target)
		if err != nil || u.Scheme != "tcp" {
			return "", fmt.Errorf("health: bad tcp target %q", h.Target)
		}
		port, err := strconv.Atoi(u.Port())
		if err != nil || port == 0 {
			return "", fmt.Errorf("health: bad tcp port in %q", h.Target)
		}
		pj.Type = "tcp"
		pj.Port = port
	case pb.HealthCheckSpec_EXEC:
		return "", fmt.Errorf("health: exec probe not supported in M3")
	default:
		return "", fmt.Errorf("health: unknown probe type")
	}
	raw, err := json.Marshal(pj)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func toHypemanPolicy(pj *PolicyJSON) *healthcheck.Policy {
	if pj.Type != "http" && pj.Type != "tcp" {
		return nil
	}
	p := &healthcheck.Policy{
		Type:             healthcheck.Type(pj.Type),
		Interval:         strconv.Itoa(maxInt(pj.IntervalSeconds, 1)) + "s",
		Timeout:          strconv.Itoa(maxInt(pj.TimeoutSeconds, 1)) + "s",
		StartPeriod:      strconv.Itoa(maxInt(pj.StartPeriodSec, 1)) + "s",
		FailureThreshold: maxInt(pj.FailureThreshold, 1),
		SuccessThreshold: maxInt(pj.SuccessThreshold, 1),
	}
	if p.Type == healthcheck.TypeHTTP {
		port := uint16(pj.Port)
		if port == 0 {
			port = 80
		}
		p.HTTP = &healthcheck.HTTPCheck{
			Port:           port,
			Path:           pj.Path,
			Scheme:         pj.Scheme,
			ExpectedStatus: pj.ExpectedStatus,
		}
		if p.HTTP.Path == "" {
			p.HTTP.Path = "/"
		}
		if p.HTTP.Scheme == "" {
			p.HTTP.Scheme = "http"
		}
		if p.HTTP.ExpectedStatus == 0 {
			p.HTTP.ExpectedStatus = 200
		}
	} else {
		p.TCP = &healthcheck.TCPCheck{Port: uint16(pj.Port)}
	}
	return p
}

// readinessFromRuntime 把 hypeman Runtime 映射为 proto readiness：
// healthy→READY；unhealthy→NOT_READY；starting/unknown→UNKNOWN。
func readinessFromRuntime(policy *healthcheck.Policy, inst healthcheck.Instance, runtime *healthcheck.Runtime, now time.Time) pb.MachineReadiness {
	snap := healthcheck.Snapshot(policy, inst.State, runtime)
	switch snap.Status {
	case healthcheck.StatusHealthy:
		return pb.MachineReadiness_READY
	case healthcheck.StatusUnhealthy:
		return pb.MachineReadiness_NOT_READY
	case healthcheck.StatusStarting, healthcheck.StatusUnknown:
		return pb.MachineReadiness_UNKNOWN
	default: // disabled
		return pb.MachineReadiness_UNCONFIGURED
	}
}

func isRunning(s instances.State) bool {
	return s == instances.StateRunning
}

func policyInterval(p *healthcheck.Policy) time.Duration {
	d, err := time.ParseDuration(p.Interval)
	if err != nil || d <= 0 {
		return 10 * time.Second
	}
	return d
}

func policyTimeout(p *healthcheck.Policy) time.Duration {
	d, err := time.ParseDuration(p.Timeout)
	if err != nil || d <= 0 {
		return 2 * time.Second
	}
	return d
}

// ProbeHTTPClient 是探针共享 HTTP 客户端（agentd 装配 RecordingRunner 用）。
func ProbeHTTPClient() *http.Client { return probeHTTPClient() }

// probeHTTPClient 是探针共享 HTTP 客户端。Timeout 是硬上限（防 SSRF 型
// 慢响应卡死 worker）；单探针实际超时由 policyTimeout（截断到剩余预算）
// 的 per-request ctx 控制（P3：此前硬编码 2s 截断用户配置）。
func probeHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// v1.1（ADR-0017）：探针连接登记 runner
// ---------------------------------------------------------------------------

// RecordingRunner 包装默认探针执行器：dial 前显式 bind 固定本地端口，并把
// 四元组登记进 probeflow registry（在 SYN 发出之前），使 agentd 的
// auto-standby conntrack 过滤器能精确剔除探针连接（不清闲判定）。
type RecordingRunner struct {
	inner  healthcheck.ProbeRunner
	reg    *probeflow.Registry
	client *http.Client // 登记型 HTTP 客户端（连接复用，避免每次探针新建池）
}

// NewRecordingRunner 构造登记 runner。reg 为 nil 时直接透传 inner。
func NewRecordingRunner(inner healthcheck.ProbeRunner, reg *probeflow.Registry) healthcheck.ProbeRunner {
	if reg == nil || inner == nil {
		return inner
	}
	r := &RecordingRunner{inner: inner, reg: reg}
	r.client = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:         r.recordingDialContext(reg),
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: 0,
		},
	}
	return r
}

// SetRunner 替换 Tracker 的探针执行器（agentd 装配 RecordingRunner 用）。
func (t *Tracker) SetRunner(r healthcheck.ProbeRunner) { t.runner = r }

// recordingDialContext returns a DialContext that records before SYN and
// releases exactly when the resulting socket closes. No timer is involved:
// keepalive probes stay filtered while alive, and a later port reuse is clean.
func (r *RecordingRunner) recordingDialContext(reg *probeflow.Registry) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dstHost, dstPortStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		dstIP, err := netip.ParseAddr(dstHost)
		if err != nil {
			return nil, err
		}
		dstPort, err := strconv.Atoi(dstPortStr)
		if err != nil || dstPort <= 0 || dstPort > 65535 {
			return nil, fmt.Errorf("invalid probe destination %q", addr)
		}
		dst := netip.AddrPortFrom(dstIP.Unmap(), uint16(dstPort))
		var srcPort uint16
		dialer := &net.Dialer{
			Timeout: 30 * time.Second,
			Control: func(network, address string, c syscall.RawConn) error {
				var opErr error
				err := c.Control(func(fd uintptr) {
					if e := syscall.Bind(int(fd), &syscall.SockaddrInet4{Port: 0}); e != nil {
						opErr = e
						return
					}
					sa, e := syscall.Getsockname(int(fd))
					if e != nil {
						opErr = e
						return
					}
					local, ok := sa.(*syscall.SockaddrInet4)
					if !ok {
						opErr = fmt.Errorf("probe dial only supports IPv4 local sockets")
						return
					}
					srcPort = uint16(local.Port)
					reg.Record(srcPort, dst)
				})
				if err != nil {
					return err
				}
				return opErr
			},
		}
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			reg.Release(srcPort, dst)
			return nil, err
		}
		return &recordedConn{Conn: conn, release: func() { reg.Release(srcPort, dst) }}, nil
	}
}

type recordedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *recordedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// Check 实现 ProbeRunner：HTTP/TCP 走登记型 dialer；EXEC 透传 inner。
func (r *RecordingRunner) Check(ctx context.Context, inst healthcheck.Instance, policy *healthcheck.Policy) healthcheck.ProbeResult {
	if policy != nil {
		switch policy.Type {
		case healthcheck.TypeHTTP:
			return r.checkHTTP(ctx, inst, *policy.HTTP)
		case healthcheck.TypeTCP:
			return r.checkTCP(ctx, inst, *policy.TCP)
		}
	}
	return r.inner.Check(ctx, inst, policy)
}

func (r *RecordingRunner) checkHTTP(ctx context.Context, inst healthcheck.Instance, check healthcheck.HTTPCheck) healthcheck.ProbeResult {
	if !inst.NetworkEnabled || inst.IP == "" {
		return healthcheck.ProbeResult{Success: false, Error: "instance has no network address"}
	}
	u := url.URL{
		Scheme: check.Scheme,
		Host:   net.JoinHostPort(inst.IP, strconv.Itoa(int(check.Port))),
		Path:   check.Path,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return healthcheck.ProbeResult{Success: false, Error: err.Error()}
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return healthcheck.ProbeResult{Success: false, Error: err.Error()}
	}
	// Close rather than drain/reuse: the registry is socket-lifecycle based, and
	// one short-lived connection per readiness probe prevents both stale flow
	// records and an unrelated long-lived conntrack activity signal.
	resp.Body.Close()
	if resp.StatusCode != check.ExpectedStatus {
		return healthcheck.ProbeResult{
			Success: false,
			Error:   fmt.Sprintf("expected HTTP status %d, got %d", check.ExpectedStatus, resp.StatusCode),
		}
	}
	return healthcheck.ProbeResult{Success: true}
}

func (r *RecordingRunner) checkTCP(ctx context.Context, inst healthcheck.Instance, check healthcheck.TCPCheck) healthcheck.ProbeResult {
	if !inst.NetworkEnabled || inst.IP == "" {
		return healthcheck.ProbeResult{Success: false, Error: "instance has no network address"}
	}
	conn, err := r.recordingDialContext(r.reg)(ctx, "tcp",
		net.JoinHostPort(inst.IP, strconv.Itoa(int(check.Port))))
	if err != nil {
		return healthcheck.ProbeResult{Success: false, Error: err.Error()}
	}
	_ = conn.Close()
	return healthcheck.ProbeResult{Success: true}
}
