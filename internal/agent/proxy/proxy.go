// Package proxy 是 agent 的 workload 流量入口（M1.6 v0）。
//
// M1 身份降级（ADR-0006）：先接受 edge 单向下发的
// X-Firepaas-Machine-ID / X-Firepaas-Execution-ID 头并校验 execution；
// mTLS + proxy credential 在 M1.3/M4 补齐。
//
// v1.1（ADR-0022）：接受 edge 的 X-Firepaas-App-Port 请求头（命中 route 的
// service internal_port）；缺失 = 旧行为（主 service 端口）。未声明端口被
// services 白名单拒绝（502）。
package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
	"github.com/zhu327/firepaas/shared/pkg/h2transport"
)

const (
	HeaderMachineID   = "X-Firepaas-Machine-ID"
	HeaderExecutionID = "X-Firepaas-Execution-ID"
	// HeaderAppPort（v1.1，ADR-0022）：edge→proxy 的目标 service 端口头。
	// 缺失 = 旧行为（spec 声明的主 service 端口），向后兼容。
	HeaderAppPort = "X-Firepaas-App-Port"
	// HeaderRequestID 是 edge→agent 的请求关联 ID（只进日志；转发 guest 前
	// 剥离）。缺失时日志记空串，不阻塞请求。
	HeaderRequestID = "X-Firepaas-Request-ID"
	// HeaderRetryable marks an agent-generated 502 that edge may retry after
	// refreshing its route. It is strictly an internal agent→edge signal and
	// must never be forwarded to a workload or client.
	HeaderRetryable = "X-Firepaas-Proxy-Retryable"
	retryableValue  = "true"
)

type targetKey struct{}

// logFieldsKey 承载请求日志关联字段（edge 传入的 request_id + 路由归属），
// 供 Director 之后的 ErrorHandler 使用（Director 已剥离内部头）。
type logFieldsKey struct{}

type logFields struct {
	requestID   string
	machineID   string
	executionID string
}

// Proxy 按 machine_id + execution_id 把流量转发到 workload endpoint。
// ReverseProxy 与 Transport 在构造时创建一次并复用（评审 P3：连接池不得
// 每请求新建）；每次请求仅解析目标并挂到 request context。
type Proxy struct {
	machines endpointResolver
	creds    credentialVerifier // nil = 不校验凭证（测试/过渡期）
	reverse  *httputil.ReverseProxy

	// W3.3：ServeCredential 的令牌桶守卫（内存状态，单节点语义——每个
	// agent 独立 guard，无跨节点协调；进程重启状态清零）。两层设计（P2
	// 修正，替换“单一全局桶按全量请求扣减”）：
	//  1) 查表前扣 lookup 桶：Creds.LookupByDigest 是 mutex 下的
	//     O(在役 machine) 恒时扫描，全局天花板保护 agent 本体；
	//  2) 未命中后扣 miss 桶：扫描者用坏凭证打满自己的 403 速率，
	//     合法（命中）凭证只受 lookup 天花板约束，不会被 miss 风暴 429；
	//     不做全局 miss 熔断（熔断即未认证 DoS 开关，review P1）。
	// 两者均可用 SetCredentialLimits 调整（默认见下方常量）。
	credMu        sync.Mutex
	credTokens    float64 // lookup 桶剩余令牌
	credMissToks  float64 // miss 桶剩余令牌
	credRate      float64 // lookup 桶速率（token/s）
	credBurst     float64 // lookup 桶容量
	credMissRate  float64 // miss 桶速率
	credMissBurst float64 // miss 桶容量
	credLast      time.Time
	credLookups   map[string]*atomic.Uint64
	nowFn         func() time.Time
}

// W3.3 凭证 guard 默认值（SetCredentialLimits / 环境变量可覆盖）：
// lookup 是回查 CPU 天花板，取正常 mesh ingress 流量的量级；miss 桶只限制
// 扫描者的 403 产出。突发 = 2×速率。
const (
	defaultCredentialLookupRPS   = 1000
	defaultCredentialLookupBurst = 2 * defaultCredentialLookupRPS
	defaultCredentialMissRPS     = 50
	defaultCredentialMissBurst   = 2 * defaultCredentialMissRPS
)

// 凭证查找结果标签（Prometheus 导出名冻结为
// firepaas_proxy_credential_lookup_total{result}，见 CredentialLookupStats）。
const (
	credentialResultOK      = "ok"
	credentialResultDenied  = "denied"
	credentialResultLimited = "limited"
)

// credentialVerifier 校验 execution-bound proxy credential（M4）。
type credentialVerifier interface {
	Verify(machineID, executionID, rawCredential string) bool
	// LookupByDigest（G2d fabric ingress）：凭证反查归属。实现方：
	// *state.Creds。
	LookupByDigest(rawCredential string) (machineID, executionID string, ok bool)
}

// endpointResolver 是 Proxy 对机器端点解析的依赖（*machine.Adapter 实现；
// 测试注入替身）。
type endpointResolver interface {
	GetEndpointForPort(
		ctx context.Context,
		machineID, executionID string,
		wantPort int,
	) (ip string, port int, err error)
}

// New 构造 Proxy（不校验凭证：仅测试用）。
func New(machines *machine.Adapter) *Proxy {
	return NewWithVerifier(machines, nil)
}

// NewForTest 构造带替身 resolver 的 Proxy（终结器/代理层测试）。
func NewForTest(
	creds credentialVerifier,
	resolve func(machineID, executionID string, wantPort int) (string, int, error),
) *Proxy {
	p := &Proxy{
		machines: resolverFunc(resolve),
		creds:    creds,
		reverse:  newReverseProxy(),
	}
	p.initCredentialGuard()
	return p
}

type resolverFunc func(machineID, executionID string, wantPort int) (string, int, error)

func (f resolverFunc) GetEndpointForPort(
	_ context.Context,
	machineID, executionID string,
	wantPort int,
) (string, int, error) {
	return f(machineID, executionID, wantPort)
}

// NewWithVerifier 构造带 credential 校验的 Proxy。
func NewWithVerifier(machines *machine.Adapter, creds credentialVerifier) *Proxy {
	p := &Proxy{machines: machines, creds: creds, reverse: newReverseProxy()}
	p.initCredentialGuard()
	return p
}

// initCredentialGuard 初始化凭证 guard 状态（构造器共用）。
func (p *Proxy) initCredentialGuard() {
	p.nowFn = time.Now
	p.credRate = defaultCredentialLookupRPS
	p.credBurst = defaultCredentialLookupBurst
	p.credMissRate = defaultCredentialMissRPS
	p.credMissBurst = defaultCredentialMissBurst
	p.credTokens = p.credBurst
	p.credMissToks = p.credMissBurst
	p.credLast = p.nowFn()
	p.credLookups = map[string]*atomic.Uint64{}
}

// SetCredentialLimits 覆盖 ingress 凭证限流（<=0 = 保持当前值）。装配点启动
// 时调用一次；调用后两个桶重置为满额。
func (p *Proxy) SetCredentialLimits(lookupRPS, missRPS float64) {
	p.credMu.Lock()
	defer p.credMu.Unlock()
	if lookupRPS > 0 {
		p.credRate, p.credBurst = lookupRPS, 2*lookupRPS
	}
	if missRPS > 0 {
		p.credMissRate, p.credMissBurst = missRPS, 2*missRPS
	}
	p.credTokens = p.credBurst
	p.credMissToks = p.credMissBurst
	p.credLast = p.nowFn()
}

// CredentialLookupStats 返回凭证查找计数快照（W3.3，key = result 标签）。
// Prometheus 导出名冻结为 firepaas_proxy_credential_lookup_total{result}，
// result ∈ ok|denied|limited。agentd /metrics 接线是 Phase2
// （需改 cmd/agentd/main.go，本波次 allowlist 之外）；本包只提供内存计数 +
// 本快照。secret/credential 原值永不进指标 label（仅结果分类）。
func (p *Proxy) CredentialLookupStats() map[string]uint64 {
	p.credMu.Lock()
	defer p.credMu.Unlock()
	out := make(map[string]uint64, len(p.credLookups))
	for k, v := range p.credLookups {
		out[k] = v.Load()
	}
	return out
}

func (p *Proxy) countCredentialLookup(result string) {
	p.credMu.Lock()
	defer p.credMu.Unlock()
	c, ok := p.credLookups[result]
	if !ok {
		c = &atomic.Uint64{}
		p.credLookups[result] = c
	}
	c.Add(1)
}

// refillLocked 按流逝时间补充两个桶（调用方持有 credMu）。
func (p *Proxy) refillLocked(now time.Time) {
	elapsed := now.Sub(p.credLast).Seconds()
	if elapsed <= 0 {
		return
	}
	p.credTokens = min(p.credTokens+elapsed*p.credRate, p.credBurst)
	p.credMissToks = min(p.credMissToks+elapsed*p.credMissRate, p.credMissBurst)
	p.credLast = now
}

// takeCredentialLookup 在查表前扣减全局 lookup 桶；桶空返回 false（调用方 429）。
func (p *Proxy) takeCredentialLookup() bool {
	p.credMu.Lock()
	defer p.credMu.Unlock()
	p.refillLocked(p.nowFn())
	if p.credTokens < 1 {
		return false
	}
	p.credTokens--
	return true
}

// takeCredentialMiss 在未命中后扣减 miss 桶；桶空返回 false（调用方 429）。
// 命中不扣 miss 桶——miss 风暴不得影响合法凭证。
func (p *Proxy) takeCredentialMiss() bool {
	p.credMu.Lock()
	defer p.credMu.Unlock()
	p.refillLocked(p.nowFn())
	if p.credMissToks < 1 {
		return false
	}
	p.credMissToks--
	return true
}

// newWorkloadRoundTripper 构造 agent→workload 的转发 Transport：base 与既有
// 口径一致（连接池复用）并显式启用 H2（ForceAttemptHTTP2，mTLS/ALPN 路径）；
// gRPC 明文经 h2transport 分流到 h2c prior-knowledge（见
// shared/pkg/h2transport）。ReverseProxy 与 Transport 仍在构造时创建一次复用。
func newWorkloadRoundTripper() http.RoundTripper {
	base := &http.Transport{
		MaxIdleConns:        64,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		MaxIdleConnsPerHost: 64,
		ForceAttemptHTTP2:   true,
	}
	return h2transport.New(base)
}

func newReverseProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			target, _ := req.Context().Value(targetKey{}).(*url.URL)
			if target == nil {
				return
			}
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host
			// 内部转发头不进入 guest（request id 同步剥离；关联只到 agent 日志）。
			req.Header.Del(HeaderMachineID)
			req.Header.Del(HeaderExecutionID)
			req.Header.Del(HeaderRequestID)
			req.Header.Del(traffic.HeaderCredential)
			req.Header.Del(HeaderAppPort)
			req.Header.Del(HeaderRetryable)
		},
		// A workload must not be able to forge the agent→edge retry signal.
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Del(HeaderRetryable)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// P0#4：transport 错误正文不得携带 guest IP:port 等内部拓扑——
			// 对 edge 只回固定文案，拨号细节留在本机日志。retryable 头
			// 语义不变（edge 仍可按它决定是否换 backend 重试）。
			fields, _ := r.Context().Value(logFieldsKey{}).(logFields)
			slog.Warn("workload proxy transport error",
				"request_id", fields.requestID,
				"machine_id", fields.machineID,
				"execution_id", fields.executionID,
				"method", r.Method, "path", r.URL.Path, "error", err)
			w.Header().Set(HeaderRetryable, retryableValue)
			http.Error(w, "workload upstream unreachable", http.StatusBadGateway)
		},
		Transport: newWorkloadRoundTripper(),
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	machineID := r.Header.Get(HeaderMachineID)
	executionID := r.Header.Get(HeaderExecutionID)
	if machineID == "" || executionID == "" {
		http.Error(w, "missing machine/execution routing headers", http.StatusBadRequest)
		return
	}

	// M4（ADR-0006）：execution-bound credential 摘要校验。缺头/错值一律 403，
	// 不区分原因；execution 替换/删除后立即失效。
	if p.creds != nil && !p.creds.Verify(machineID, executionID,
		r.Header.Get(traffic.HeaderCredential)) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	// v1.1（ADR-0022）：目标 service 端口（缺失/非法 = 0 → 主端口旧行为）。
	wantPort := 0
	if raw := r.Header.Get(HeaderAppPort); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			http.Error(w, "invalid "+HeaderAppPort, http.StatusBadRequest)
			return
		}
		wantPort = port
	}

	p.serveTarget(w, r, machineID, executionID, wantPort)
}

// serveTarget 是两条入口共享的转发路径（legacy :5107 头路由与 G2d
// fabric ingress 凭证路由）：endpoint 解析 → 反向代理到 guest。
func (p *Proxy) serveTarget(w http.ResponseWriter, r *http.Request, machineID, executionID string, wantPort int) {
	// 关联字段进 context：Director 会剥离内部头，ErrorHandler 只能从
	// context 取 request_id 与路由归属。
	r = r.WithContext(context.WithValue(r.Context(), logFieldsKey{}, logFields{
		requestID:   r.Header.Get(HeaderRequestID),
		machineID:   machineID,
		executionID: executionID,
	}))
	ip, port, err := p.machines.GetEndpointForPort(r.Context(), machineID, executionID, wantPort)
	if err != nil {
		// Endpoint lookup failure means this agent cannot serve the catalogued
		// backend (for example a stale rolling route), rather than a workload
		// response. Tell edge that this particular 502 is safe to retry.
		w.Header().Set(HeaderRetryable, retryableValue)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	target := &url.URL{Scheme: "http", Host: fmt.Sprintf("%s:%d", ip, port)}
	r = r.WithContext(context.WithValue(r.Context(), targetKey{}, target))
	p.reverse.ServeHTTP(w, r)
}

// ServeCredential 是 G2d fabric ingress 入口（ADR-0040 §14）：凭证是唯一
// 路由依据（无 X-Firepaas-Machine/Execution 头）；由 creds 反查归属后走
// 共享转发路径。wantPort = 目标 service 端口（0 = 主端口）。
// W3.3（P2 修正）：两层令牌桶——查表前扣全局 lookup 桶（回查 CPU 天花板），
// 未命中后扣 miss 桶（扫描者 403 速率）；命中流量不受 miss 风暴影响。
// 所有拒绝都不区分细节、不回显凭证、不记凭证原值日志。
func (p *Proxy) ServeCredential(w http.ResponseWriter, r *http.Request, rawCredential string, wantPort int) {
	if p.creds == nil {
		p.countCredentialLookup(credentialResultDenied)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !p.takeCredentialLookup() {
		p.countCredentialLookup(credentialResultLimited)
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	machineID, executionID, ok := p.creds.LookupByDigest(rawCredential)
	if !ok {
		if !p.takeCredentialMiss() {
			p.countCredentialLookup(credentialResultLimited)
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		p.countCredentialLookup(credentialResultDenied)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	p.countCredentialLookup(credentialResultOK)
	p.serveTarget(w, r, machineID, executionID, wantPort)
}
