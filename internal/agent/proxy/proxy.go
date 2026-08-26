// Package proxy 是 agent 的 workload 流量入口（M1.6 v0）。
//
// M1 身份降级（ADR-0006）：先接受 edge 单向下发的
// X-Firepaas-Machine-ID / X-Firepaas-Execution-ID 头并校验 execution；
// mTLS + proxy credential 在 M1.3/M4 补齐。
package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/example/firepaas/internal/agent/machine"
)

const (
	HeaderMachineID   = "X-Firepaas-Machine-ID"
	HeaderExecutionID = "X-Firepaas-Execution-ID"
)

type targetKey struct{}

// Proxy 按 machine_id + execution_id 把流量转发到 workload endpoint。
// ReverseProxy 与 Transport 在构造时创建一次并复用（评审 P3：连接池不得
// 每请求新建）；每次请求仅解析目标并挂到 request context。
type Proxy struct {
	machines *machine.Adapter
	reverse  *httputil.ReverseProxy
}

// New 构造 Proxy。
func New(machines *machine.Adapter) *Proxy {
	p := &Proxy{machines: machines}
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
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadGateway)
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

	ip, port, err := p.machines.GetEndpoint(r.Context(), machineID, executionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	target := &url.URL{Scheme: "http", Host: fmt.Sprintf("%s:%d", ip, port)}
	r = r.WithContext(context.WithValue(r.Context(), targetKey{}, target))
	p.reverse.ServeHTTP(w, r)
}
