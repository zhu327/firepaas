package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// webhook_scaleup.go：Wave5——NodeScaleUpNotifier 的最小解耦实现。
//
// 为什么先做 webhook 而不是直连 Nomad/云 API：扩容目标环境（Nomad
// Autoscaler vs 云 API）、凭证、配额上限、幂等语义都是部署相关决策；
// webhook 把"信号产生"（controller，已有节流+durable 事件）与"容量执行"
// （外部 autoscaler）解耦：任何能收 HTTP POST 的系统（Nomad Autoscaler
// 的 webhook policy、云函数、自研 operator）都能消费，无 SDK 绑定、可单测。
//
// 投递语义：at-least-once 尽力投递（失败只记日志，不阻塞派发——信号的
// durable 形态是 scheduler event，见 scaleup_signal.go）；接收方必须以
// (pool, operation_id) 去重（op 重入列 + leader 切换可能重发，节流后仍有
// 小窗口重复）。

// WebhookScaleUpNotifier 把扩容信号 POST 为 JSON 到配置 URL。
type WebhookScaleUpNotifier struct {
	// URL 是接收端点；空 = 禁用（NotifyScaleUp 直接返回 nil，方便"配了
	// 对象但没配地址"的装配路径）。
	URL string
	// Client 为 nil 时用带 10s 超时的默认客户端。
	Client *http.Client
	// Token 非空时以 Bearer 附带（接收方做细粒度鉴权；空 = 无鉴权头，
	// 依赖网络层 ACL，与 agent metrics 端点同纪律）。
	Token string
}

func (w *WebhookScaleUpNotifier) client() *http.Client {
	if w.Client != nil {
		return w.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

// NotifyScaleUp 实现 NodeScaleUpNotifier。
func (w *WebhookScaleUpNotifier) NotifyScaleUp(ctx context.Context, sig ScaleUpSignal) error {
	if w == nil || w.URL == "" {
		return nil
	}
	raw, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("encode scale-up signal: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build scale-up webhook: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if w.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.Token)
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return fmt.Errorf("post scale-up webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("scale-up webhook status %d", resp.StatusCode)
	}
	return nil
}
