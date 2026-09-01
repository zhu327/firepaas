// Package traffic 实现 execution-bound proxy credential 的派生（ADR-0006）。
//
// credential = hex(HMAC-SHA256(key, machineID + "/" + executionID))[:32]
//
// 设计要点：
//   - **确定性**：同 (machine, execution) 重复派生结果一致 → controller 重试
//     （ACK 丢失补账、操作重派）自动满足 agent ledger 的 request hash 幂等；
//   - **零持久化**：任何组件都不保存原文；API 按需现算给 edge，agent 在
//     Create 时收到原文只留摘要（state.Creds）；
//   - **自动轮换**：execution 替换 → credential 变化；agent 删除 machine →
//     摘要撤销。key 轮换 = 换环境变量并重建（运维文档化）。
package traffic

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// HeaderCredential 是 edge→agent proxy 携带凭证的请求头。
const HeaderCredential = "X-Firepaas-Credential"

// Signer 持有 HMAC 密钥。
type Signer struct {
	key []byte
}

// NewSigner 由部署注入的密钥构造（建议 ≥32 字节随机值）。
func NewSigner(key []byte) (*Signer, error) {
	if len(key) < 32 {
		return nil, fmt.Errorf("traffic: key must be >= 32 bytes, got %d", len(key))
	}
	return &Signer{key: key}, nil
}

// Token 计算指定 machine/execution 的 proxy credential。
func (s *Signer) Token(machineID, executionID string) string {
	mac := hmac.New(sha256.New, s.key)
	_, _ = fmt.Fprintf(mac, "%s/%s", machineID, executionID)
	return hex.EncodeToString(mac.Sum(nil))[:32]
}
