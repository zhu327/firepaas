// Package h2transport 为 edge→agent 与 agent→workload 两条转发路径提供
// HTTP/2 能力（私有化 Fly.io 对标：gRPC workload 必须端到端跑在 HTTP/2 上）。
//
// 背景：Go 1.25 标准库 http.Transport 无法对 http:// 自动协商 h2c（含
// UnencryptedHTTP2 但含 HTTP1 时走 HTTP/1.1；只含 UnencryptedHTTP2 时强制
// prior-knowledge，会打断只懂 HTTP/1.1 的后端）。因此按内容类型分流：
//   - gRPC（Content-Type: application/grpc*）+ 明文 http → h2c prior-knowledge
//     （golang.org/x/net/http2.Transport，AllowHTTP）；
//   - 其余（或 https，由 ALPN 协商）→ 调用方传入的 base Transport（必须置
//     ForceAttemptHTTP2=true，否则自定义 Dialer/TLSConfig 会保守关闭 H2）。
//
// 服务端侧（cmd/agentd :5107、cmd/edge-proxy 明文监听）经 ServerProtocols
// 启用标准库原生 h2c（同端口双协议，兼容 HTTP/1.1；TLS 路径由 ALPN 协商）。
package h2transport

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// grpcContentTypePrefix 是 gRPC 的内容类型前缀（含 application/grpc-web 等变体）。
const grpcContentTypePrefix = "application/grpc"

// dialTimeout 与转发路径既有 Dialer 口径一致（拨号 3s：节点失联/WG 断链时
// SYN 无 RST，不能吞掉请求预算）。
const dialTimeout = 3 * time.Second

// IsGRPC 判定请求是否为 gRPC（含 grpc-web 变体）：Content-Type 前缀匹配，
// 大小写不敏感；缺头一律按非 gRPC 处理（保守走 HTTP/1.1）。
func IsGRPC(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return false
	}
	// 形如 "application/grpc+proto"：取 ';' 前的主类型再比前缀。
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.TrimSpace(strings.ToLower(ct))
	return strings.HasPrefix(ct, grpcContentTypePrefix)
}

// ServerProtocols 返回监听端口的服务端协议配置（替代已废弃的
// x/net/http2/h2c 包；标准库原生同端口双协议）：tlsCfg==nil（明文）时启用
// HTTP/1 + h2c prior-knowledge；TLS 路径返回 nil（ALPN 默认协商 H2，
// 不显式收窄——收窄会关掉 TLS 上的 H2）。
func ServerProtocols(tlsCfg *tls.Config) *http.Protocols {
	if tlsCfg != nil {
		return nil
	}
	p := &http.Protocols{}
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return p
}

// RoundTripper 按内容类型把 gRPC 明文请求发往 h2c，其余走 base。
type RoundTripper struct {
	base http.RoundTripper

	mu  sync.Mutex
	h2c *http2.Transport
}

// New 包装 base（nil = http.DefaultTransport）。h2c Transport 惰性构造一次。
func New(base http.RoundTripper) *RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &RoundTripper{base: base}
}

// Base 返回被包装的非 gRPC 路径 Transport（测试/观测用）。
func (t *RoundTripper) Base() http.RoundTripper { return t.base }

func (t *RoundTripper) h2cTransport() *http2.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.h2c == nil {
		dialer := &net.Dialer{Timeout: dialTimeout}
		t.h2c = &http2.Transport{
			// AllowHTTP=true：明文 prior-knowledge h2c（无 TLS/ALPN）。
			// DialTLSContext 在 AllowHTTP 下实际发起明文 TCP 拨号（x/net 约定）。
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, network, addr)
			},
			DisableCompression: true,
			// 评审 blockers：h2c 路径绕过了 base 的 ResponseHeaderTimeout
			// （首字节 30s）。x/net 无首字节超时字段，用连接级健康检查
			// 对齐半开兜底语义：30s 无帧即 ping，15s 无应答即关；
			// 首字节超时仍由调用方请求级 context 负责（gRPC 长流不能被
			// 整体 30s 掐断，故不在此加 RoundTrip 级超时）。
			ReadIdleTimeout: 30 * time.Second,
			PingTimeout:     15 * time.Second,
		}
	}
	return t.h2c
}

// RoundTrip 实现 http.RoundTripper。
func (t *RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "http" && IsGRPC(req) {
		return t.h2cTransport().RoundTrip(req)
	}
	return t.base.RoundTrip(req)
}
