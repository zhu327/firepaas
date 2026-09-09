// Package fabricingress 是节点 fabric ingress 终结器（ADR-0040 §14，G2d）。
//
// 职责与边界：
//   - 监听 [节点 ULA]:port（随 fabric 快照 NodePrefix 幂等绑定/重绑——
//     仅 mesh 内可达，未配 WG 的主机不可路由到该地址；peer 准入由 WG
//     加密承担（eBPF 不查本路径，§14）；
//   - 请求契约：X-Firepaas-Credential 头是**唯一路由与授权依据**
//     （无 X-Firepaas-Machine/Execution 头）：终结器按凭证摘要反查
//     (machine, execution)（state.Creds，Create 时只存摘要——HMAC 密钥
//     不出控制面，与 legacy :5107 同一口径），未知/错值一律 403；
//   - 目标 service 端口经 X-Firepaas-App-Port 传递（目标参数，非路由
//     依据；缺失 = 主端口），转发前剥离全部内部头——与 legacy 代理
//     同一出口纪律；
//   - 转发复用 proxy.Proxy（endpoint 解析/autoresume/重试语义完全一致）；
//   - legacy :5107 代理保留（§14：兼容入口），两条路径互不影响。
package fabricingress

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/agent/proxy"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

// Server 是 fabric ingress HTTP 终结器。
type Server struct {
	proxy *proxy.Proxy
	port  int

	mu     sync.Mutex
	bind   string
	ln     net.Listener
	srv    *http.Server
	closed bool
}

// New 构造终结器（未监听；Ensure 按快照 NodePrefix 幂等绑定）。
// port 是 fabric ingress 端口（与控制面 FIREPAAS_AGENT_FABRIC_INGRESS_PORT
// 同值，写入 mesh:endpoint 投影供 edge 寻址）。
func New(p *proxy.Proxy, port int) *Server {
	s := &Server{proxy: p, port: port}
	return s
}

// Ensure 幂等确保监听在节点 ULA 的 :port。nodePrefix 为空 = 快照未生效，
// 不启动。addr 变化时重绑。绑定失败返回错误（调用方降级记日志，不阻断
// agentd——edge 会因 mesh:endpoint 不可达回落 legacy 路径）。
func (s *Server) Ensure(ctx context.Context, nodePrefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("fabricingress: closed")
	}
	if nodePrefix == "" {
		return nil
	}
	ula := nodeBaseAddr(nodePrefix)
	if ula == "" {
		return fmt.Errorf("fabricingress: bad node prefix %q", nodePrefix)
	}
	addr := net.JoinHostPort(ula, strconv.Itoa(s.port))
	if s.bind == addr && s.ln != nil {
		return nil
	}
	s.closeLocked()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("fabricingress: listen %s: %w", addr, err)
	}
	srv := &http.Server{
		Handler: s,
		// W4：ReadHeaderTimeout 防慢头 slowloris；IdleTimeout 回收空闲
		// 长连（mesh 内 edge 常连）。Read/WriteTimeout 保持 0：本终结器
		// 是流式透传（SSE/WebSocket 经 proxy.Proxy 劫持），整体读写
		// 限时将误杀长流；上游 edge Transport ResponseHeaderTimeout
		// 30s 兜底远端僵死。
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	s.bind, s.ln, s.srv = addr, ln, srv
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Warn("fabric ingress serve", "addr", addr, "error", err)
		}
	}()
	slog.Info("fabric ingress listening", "addr", addr)
	return nil
}

// Close 停止监听（幂等）。
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.closeLocked()
}

func (s *Server) closeLocked() {
	if s.srv != nil {
		_ = s.srv.Close()
	}
	s.ln, s.srv, s.bind = nil, nil, ""
}

// ServeHTTP：凭证 = 唯一路由依据。端口经 X-Firepaas-App-Port（目标参数）。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	wantPort := 0
	if raw := r.Header.Get(proxy.HeaderAppPort); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil || port < 1 || port > 65535 {
			http.Error(w, "invalid "+proxy.HeaderAppPort, http.StatusBadRequest)
			return
		}
		wantPort = port
	}
	// 剥离调用方可控的内部头（终结器只用凭证与端口参数；转发路径的
	// Director 会再剥一次，这里先保证凭证之外无任何路由信息可注入）。
	r.Header.Del(proxy.HeaderMachineID)
	r.Header.Del(proxy.HeaderExecutionID)
	r.Header.Del(proxy.HeaderRetryable)
	cred := r.Header.Get(traffic.HeaderCredential)
	s.proxy.ServeCredential(w, r, cred, wantPort)
}

// nodeBaseAddr 取 /64 前缀基址（节点 ULA；空 = 非法前缀）。
func nodeBaseAddr(raw string) string {
	ip, _, err := net.ParseCIDR(raw)
	if err != nil {
		return ""
	}
	return ip.String()
}
