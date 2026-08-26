// Package proxy 是 agent 的 workload 流量入口（M1.6 v0）。
//
// M1 身份降级（ADR-0006）：先接受 edge 单向下发的
// X-Firepaas-Machine-ID / X-Firepaas-Execution-ID 头并校验 execution；
// mTLS + proxy credential 在 M1.3/M4 补齐。
package proxy

import (
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

// Proxy 按 machine_id + execution_id 把流量转发到 workload endpoint。
type Proxy struct {
	machines *machine.Adapter
}

// New 构造 Proxy。
func New(machines *machine.Adapter) *Proxy { return &Proxy{machines: machines} }

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

	target, err := url.Parse(fmt.Sprintf("http://%s:%d", ip, port))
	if err != nil {
		http.Error(w, "bad target", http.StatusInternalServerError)
		return
	}

	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
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
			MaxIdleConns:       64,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true,
		},
	}
	rp.ServeHTTP(w, r)
}
