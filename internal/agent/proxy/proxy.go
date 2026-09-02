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
	"time"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

const (
	HeaderMachineID   = "X-Firepaas-Machine-ID"
	HeaderExecutionID = "X-Firepaas-Execution-ID"
	// HeaderAppPort（v1.1，ADR-0022）：edge→proxy 的目标 service 端口头。
	// 缺失 = 旧行为（spec 声明的主 service 端口），向后兼容。
	HeaderAppPort = "X-Firepaas-App-Port"
	// HeaderRetryable marks an agent-generated 502 that edge may retry after
	// refreshing its route. It is strictly an internal agent→edge signal and
	// must never be forwarded to a workload or client.
	HeaderRetryable = "X-Firepaas-Proxy-Retryable"
	retryableValue  = "true"
)

type targetKey struct{}

// Proxy 按 machine_id + execution_id 把流量转发到 workload endpoint。
// ReverseProxy 与 Transport 在构造时创建一次并复用（评审 P3：连接池不得
// 每请求新建）；每次请求仅解析目标并挂到 request context。
type Proxy struct {
	machines *machine.Adapter
	creds    credentialVerifier // nil = 不校验凭证（测试/过渡期）
	reverse  *httputil.ReverseProxy
}

// credentialVerifier 校验 execution-bound proxy credential（M4）。
type credentialVerifier interface {
	Verify(machineID, executionID, rawCredential string) bool
}

// New 构造 Proxy（不校验凭证：仅测试用）。
func New(machines *machine.Adapter) *Proxy {
	return NewWithVerifier(machines, nil)
}

// NewWithVerifier 构造带 credential 校验的 Proxy。
func NewWithVerifier(machines *machine.Adapter, creds credentialVerifier) *Proxy {
	p := &Proxy{machines: machines, creds: creds}
	p.reverse = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			target, _ := req.Context().Value(targetKey{}).(*url.URL)
			if target == nil {
				return
			}
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host
			// 内部转发头不进入 guest。
			req.Header.Del(HeaderMachineID)
			req.Header.Del(HeaderExecutionID)
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
			slog.Warn("workload proxy transport error",
				"method", r.Method, "path", r.URL.Path, "error", err)
			w.Header().Set(HeaderRetryable, retryableValue)
			http.Error(w, "workload upstream unreachable", http.StatusBadGateway)
		},
		Transport: &http.Transport{
			MaxIdleConns:        64,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  true,
			MaxIdleConnsPerHost: 64,
		},
	}
	return p
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
