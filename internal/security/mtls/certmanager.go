package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// CertManager 管理一张证书的热重载与到期观测（生产就绪计划契约 C-1）。
//
//   - 当前证书由 RWMutex 保护，GetCertificate/GetClientCertificate 供
//     tls.Config 在每次握手时取最新证书（连接不复读配置，轮换即时生效）；
//   - 后台按 reloadEvery 周期重读证书/私钥文件；重读失败保留旧证书并
//     log warn（fail-operational：过期或损坏的新文件不中断服务）；
//   - notAfterHook 在每次成功加载（首次 + 每次重载）后被调用，调用方
//     据此导出 firepaas_tls_cert_not_after_seconds 到期指标；
//   - Close 停止后台 goroutine，进程退出时调用。
type CertManager struct {
	certFile string
	keyFile  string
	logger   *slog.Logger
	hook     func(time.Time)

	mu       sync.RWMutex
	current  *tls.Certificate
	notAfter time.Time

	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewCertManager 加载初始证书（失败即返回错误，fail-closed 启动），
// reloadEvery > 0 时启动后台周期重载。notAfterHook 可为 nil。
func NewCertManager(
	certFile, keyFile string,
	reloadEvery time.Duration,
	logger *slog.Logger,
	notAfterHook func(expiry time.Time),
) (*CertManager, error) {
	if logger == nil {
		logger = slog.Default()
	}
	m := &CertManager{
		certFile: certFile,
		keyFile:  keyFile,
		logger:   logger,
		hook:     notAfterHook,
		done:     make(chan struct{}),
	}
	if err := m.reload(); err != nil {
		return nil, err
	}
	if reloadEvery > 0 {
		m.wg.Add(1)
		go m.run(reloadEvery)
	}
	return m, nil
}

// GetCertificate 供 tls.Config.GetCertificate（server 侧）使用。
func (m *CertManager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current, nil
}

// GetClientCertificate 供 tls.Config.GetClientCertificate（client 侧）使用。
func (m *CertManager) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current, nil
}

// NotAfter 返回当前证书的过期时间。
func (m *CertManager) NotAfter() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.notAfter
}

// ClientTLSConfig 构造客户端 TLS 配置：证书经 CertManager 热重载提供。
// RootCAs 只在构造时加载一次（CA 轮换不在契约 C-1 范围内，重签同级 CA
// 需进程重启；证书本身轮换通过热重载覆盖）。
func (m *CertManager) ClientTLSConfig(caFile, serverName string) (*tls.Config, error) {
	pool, err := loadCA(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		GetClientCertificate: m.GetClientCertificate,
		RootCAs:              pool,
		ServerName:           serverName,
		MinVersion:           tls.VersionTLS12,
	}, nil
}

// Close 停止后台重载 goroutine；可重复调用。
func (m *CertManager) Close() {
	m.stopOnce.Do(func() { close(m.done) })
	m.wg.Wait()
}

func (m *CertManager) run(every time.Duration) {
	defer m.wg.Done()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			if err := m.reload(); err != nil {
				// 重载失败保留当前证书继续服务（新文件可能写入不完整）。
				m.logger.Warn("TLS 证书热重载失败，保留当前证书", "cert", m.certFile, "error", err)
			}
		}
	}
}

// reload 从磁盘重新加载 keypair 并原子替换当前证书。
func (m *CertManager) reload() error {
	cert, err := tls.LoadX509KeyPair(m.certFile, m.keyFile)
	if err != nil {
		return fmt.Errorf("reload keypair %s: %w", m.certFile, err)
	}
	if cert.Leaf == nil {
		// 防御：Go >= 1.23 LoadX509KeyPair 已填充 Leaf；老行为下自行解析。
		leaf, parseErr := x509.ParseCertificate(cert.Certificate[0])
		if parseErr != nil {
			return fmt.Errorf("parse leaf of %s: %w", m.certFile, parseErr)
		}
		cert.Leaf = leaf
	}
	expiry := cert.Leaf.NotAfter
	m.mu.Lock()
	m.current = &cert
	m.notAfter = expiry
	m.mu.Unlock()
	if m.hook != nil {
		m.hook(expiry)
	}
	return nil
}
