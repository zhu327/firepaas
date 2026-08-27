// creds.go：M4 proxy credential 验证材料的本机持久化（ADR-0006 收口）。
//
// 红线：只保存 **SHA-256 摘要** 与 execution 绑定，绝无原始凭证（与 operation
// ledger 同哲学——"摘要可持久化、原文不落盘"）。agent 重启后凭摘要仍能校验
// edge 请求；丢失的场景只有状态文件损坏，届时校验失败 = fail-closed。
package state

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CredEntry 单台 machine 的验证材料（仅摘要）。
type CredEntry struct {
	ExecutionID string    `json:"execution_id"`
	Digest      string    `json:"digest"` // hex(sha256(raw credential))
	UpdatedAt   time.Time `json:"updated_at"`
}

// Creds 是 machine_id → CredEntry 的持久化映射，带原子落盘。
type Creds struct {
	mu      sync.Mutex
	path    string
	entries map[string]CredEntry
}

// OpenCreds 加载验证材料；文件不存在时从空状态开始。
func OpenCreds(path string) (*Creds, error) {
	c := &Creds{path: path, entries: map[string]CredEntry{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read creds: %w", err)
	}
	if len(data) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(data, &c.entries); err != nil {
		return nil, fmt.Errorf("parse creds %s: %w", path, err)
	}
	return c, nil
}

// Digest 计算 raw 凭证的摘要（hex）。
func Digest(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// Set 记录/替换 machine 的验证材料（create 成功或换代重发时调用）。
func (c *Creds) Set(machineID, executionID, digest string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[machineID] = CredEntry{
		ExecutionID: executionID, Digest: digest, UpdatedAt: time.Now().UTC(),
	}
	return c.persistLocked()
}

// Drop 撤销验证材料（delete / execution 替换）。不存在时为 no-op。
func (c *Creds) Drop(machineID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[machineID]; !ok {
		return nil
	}
	delete(c.entries, machineID)
	return c.persistLocked()
}

// Verify 校验请求携带的原始凭证。执行绑定必须完全一致（execution 替换后旧
// 凭证立即失效），摘要恒时比较。
func (c *Creds) Verify(machineID, executionID, rawCredential string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[machineID]
	if !ok || e.ExecutionID != executionID || rawCredential == "" {
		return false
	}
	want, err := hex.DecodeString(e.Digest)
	if err != nil {
		return false
	}
	got := sha256.Sum256([]byte(rawCredential))
	return hmac.Equal(want, got[:])
}

func (c *Creds) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c.entries, "", " ")
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
