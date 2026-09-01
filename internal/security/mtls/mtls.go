// Package mtls 提供 M1.3 静态证书的 TLS 配置加载（ADR-0006 降级路径）。
// 生产形态（轮换/授权矩阵）在 M5 补齐；本包只负责“有证书才能连”，
// 并按调用方证书 CN 做最小授权白名单（M1.2 评审补齐，P1-2）。
package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ServerConfig 构造 require-and-verify-client-cert 的服务端 TLS 配置。
func ServerConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server keypair: %w", err)
	}
	pool, err := loadCA(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ClientConfig 构造客户端 TLS 配置（serverName 通常为 agentd/127.0.0.1）。
func ClientConfig(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load client keypair: %w", err)
	}
	pool, err := loadCA(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func loadCA(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates parsed from %s", caFile)
	}
	return pool, nil
}

// PeerCN 从 gRPC 上下文提取已验证客户端证书的 CommonName。
// 返回错误表示：没有 peer、不是 TLS、或客户端证书未通过验证。
func PeerCN(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("no peer in context")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", errors.New("peer auth info is not TLS")
	}
	if len(ti.State.VerifiedChains) == 0 {
		return "", errors.New("client certificate not verified")
	}
	if len(ti.State.PeerCertificates) == 0 {
		return "", errors.New("no client certificate")
	}
	return ti.State.PeerCertificates[0].Subject.CommonName, nil
}

// UnaryServerIdentityInterceptor 返回限制 gRPC 调用方身份（证书 CN 白名单）的
// unary 拦截器。仅在服务端启用 mTLS 时安装；未持证/未验证/CN 不在白名单的
// 调用分别返回 Unauthenticated/PermissionDenied。
func UnaryServerIdentityInterceptor(allowedCNs []string) grpc.UnaryServerInterceptor {
	allowed := allowedIdentities(allowedCNs)
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorizePeer(ctx, allowed); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerIdentityInterceptor applies the same verified-client CN allowlist
// as UnaryServerIdentityInterceptor to server-, client-, and bidirectional streams.
func StreamServerIdentityInterceptor(allowedCNs []string) grpc.StreamServerInterceptor {
	allowed := allowedIdentities(allowedCNs)
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorizePeer(stream.Context(), allowed); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func allowedIdentities(allowedCNs []string) map[string]bool {
	allowed := make(map[string]bool, len(allowedCNs))
	for _, cn := range allowedCNs {
		if cn = strings.TrimSpace(cn); cn != "" {
			allowed[cn] = true
		}
	}
	return allowed
}

func authorizePeer(ctx context.Context, allowed map[string]bool) error {
	cn, err := PeerCN(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if !allowed[cn] {
		return status.Errorf(codes.PermissionDenied, "client identity %q is not allowed", cn)
	}
	return nil
}

// RequireClientIdentity 返回限制 HTTP 调用方身份（证书 CN 白名单）的中间件。
// 用于 agent workload proxy（5107）：TLS 模式下只接受白名单身份（默认 edge）。
func RequireClientIdentity(next http.Handler, allowedCNs []string) http.Handler {
	allowed := make(map[string]bool, len(allowedCNs))
	for _, cn := range allowedCNs {
		if cn = strings.TrimSpace(cn); cn != "" {
			allowed[cn] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "client certificate required", http.StatusUnauthorized)
			return
		}
		cn := r.TLS.PeerCertificates[0].Subject.CommonName
		if !allowed[cn] {
			http.Error(w, fmt.Sprintf("client identity %q is not allowed", cn), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SplitAllowlist 解析逗号分隔的 CN 白名单环境变量；空/未设置时返回默认值。
func SplitAllowlist(v, def string) []string {
	v = strings.TrimSpace(v)
	if v == "" {
		v = def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
