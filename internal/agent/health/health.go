// Package health 是 agent 的 host 侧就绪探针执行器（ADR-0008 M3 落地）。
//
// 归属：agent 在 host 侧经 workload endpoint（bridge guest IP / slot 路由）
// 对 MachineSpec.health_check 声明的 target 执行 HTTP/TCP 探针，复用 hypeman
// lib/healthcheck 的 Policy/阈值/Runtime 语义；结果映射为
// Machine.readiness（UNKNOWN/NOT_READY/READY/UNCONFIGURED），是 controller
// 更新 route_backends.readiness 的唯一来源。
//
// 执行节奏：不单开 goroutine，挂在 ListMachines 观测路径上（controller 每
// 5s、nodemanager 每 20s 拉一次），到 interval 才真正探一次——单线程、
// 无锁竞争、天然背压。
package health

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/kernel/hypeman/lib/healthcheck"
	"github.com/kernel/hypeman/lib/instances"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
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
	policy     *healthcheck.Policy
	runtime    *healthcheck.Runtime
	readiness  pb.MachineReadiness
	changedAt  time.Time
	lastProbe  time.Time
	startedAt  time.Time
	guestReady bool
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

// Observe 观测一台实例：注册/更新策略，到期执行探针并推进状态机。
// 必须在 ListMachines 路径对每台实例调用；并发安全。
func (t *Tracker) Observe(ctx context.Context, inst *instances.Instance) {
	id := inst.Name
	if id == "" {
		id = inst.Id
	}
	policy := ParsePolicy(inst.Tags[tagHealth])
	now := t.now()

	t.mu.Lock()
	e, ok := t.byID[id]
	if !ok {
		e = &entry{
			readiness: pb.MachineReadiness_UNKNOWN,
			changedAt: now,
			startedAt: inst.CreatedAt,
		}
		t.byID[id] = e
	}
	e.policy = policy
	if !inst.CreatedAt.IsZero() {
		e.startedAt = inst.CreatedAt
	}
	startedAt := e.startedAt
	due := policy != nil && (e.lastProbe.IsZero() || now.Sub(e.lastProbe) >= policyInterval(policy))
	t.mu.Unlock()

	if !due || !isRunning(inst.State) {
		return
	}

	hcInst := healthcheck.Instance{
		ID:             id,
		Name:           id,
		State:          healthcheck.StateRunning,
		NetworkEnabled: inst.NetworkEnabled,
		IP:             inst.IP,
		StartedAt:      &startedAt,
	}
	probeCtx, cancel := context.WithTimeout(ctx, policyTimeout(policy))
	result := t.runner.Check(probeCtx, hcInst, policy)
	cancel()

	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok = t.byID[id]
	if !ok {
		return
	}
	e.lastProbe = now
	e.runtime = healthcheck.ApplyProbeResult(policy, hcInst, e.runtime, now, result)

	readiness := readinessFromRuntime(policy, hcInst, e.runtime, now)
	if readiness != e.readiness {
		e.readiness = readiness
		e.changedAt = now
	}
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

func probeHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Second}
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
