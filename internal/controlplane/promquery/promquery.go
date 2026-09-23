// Package promquery 是控制面查询 Prometheus HTTP API 的极简客户端
// （Wave3 rollout 分析与 Wave4 autoscale CPU/自定义信号共用）。
//
// 只用标准库：GET /api/v1/query?query=…&time=…，解析 data.result[0].value[1]。
// 瞬时向量取首个样本；空结果返回 ErrNoData（调用方按 fail-closed 处理）。
package promquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// ErrNoData 表示查询成功但无样本（selector 未命中，非调用方错误）。
var ErrNoData = errors.New("prometheus query returned no samples")

// Client 查询单台 Prometheus（中央抓取拓扑；lab 与生产同地址不同值）。
type Client struct {
	BaseURL string // 如 http://prometheus:9090
	HTTP    *http.Client
	Timeout time.Duration
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 10 * time.Second
}

type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Value []any `json:"value"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

// Query 执行瞬时 PromQL 查询，返回首个样本值。
func (c *Client) Query(ctx context.Context, expr string) (float64, error) {
	if c.BaseURL == "" {
		return 0, fmt.Errorf("prometheus base URL not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	u := c.BaseURL + "/api/v1/query?" + url.Values{
		"query": {expr},
		"time":  {time.Now().UTC().Format(time.RFC3339)},
	}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, fmt.Errorf("prometheus query build: %w", err)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus query: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus query status %d", resp.StatusCode)
	}
	var qr queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		return 0, fmt.Errorf("prometheus query decode: %w", err)
	}
	if qr.Status != "success" {
		return 0, fmt.Errorf("prometheus query failed: %s", qr.Error)
	}
	if len(qr.Data.Result) == 0 || len(qr.Data.Result[0].Value) < 2 {
		return 0, ErrNoData
	}
	raw := qr.Data.Result[0].Value[1]
	switch v := raw.(type) {
	case string:
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
			return 0, fmt.Errorf("prometheus sample %q: %w", v, err)
		}
		return f, nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("prometheus sample of unexpected type %T", raw)
	}
}
