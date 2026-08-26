// Package state 实现 agent 侧 operation ledger（M1.4）。
//
// 语义（mvp-plan §5.5）：
//   - 同一 operation_id 重试返回已记录结果；
//   - 相同 operation_id、不同 request hash 被拒绝；
//   - 重启后从磁盘重放。
//
// 只持久化 request hash 与结果 JSON，不持久化 secret_env / proxy_credential
// 等敏感字段（ADR-0010/ADR-0013）。
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrRequestHashConflict 表示同一 operation_id 携带了不同的 request hash。
var ErrRequestHashConflict = errors.New("operation_id already completed with a different request hash")

// Record 是 ledger 中一条已完成操作的持久化记录。
type Record struct {
	OperationID string          `json:"operation_id"`
	RequestHash string          `json:"request_hash"`
	Result      json.RawMessage `json:"result"`
	CreatedAt   time.Time       `json:"created_at"`
}

// Ledger 是进程内的 operation ledger，带原子落盘。
type Ledger struct {
	mu      sync.Mutex
	path    string
	records map[string]Record
}

// Open 加载 ledger；文件不存在时从空状态开始。
func Open(path string) (*Ledger, error) {
	l := &Ledger{path: path, records: map[string]Record{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ledger: %w", err)
	}
	if len(data) == 0 {
		return l, nil
	}
	if err := json.Unmarshal(data, &l.records); err != nil {
		return nil, fmt.Errorf("parse ledger %s: %w", path, err)
	}
	return l, nil
}

// Check 返回 operation_id 已记录的 result（若存在且 hash 匹配）。
func (l *Ledger) Check(operationID, requestHash string) (json.RawMessage, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.records[operationID]
	if !ok {
		return nil, false, nil
	}
	if rec.RequestHash != requestHash {
		return nil, false, ErrRequestHashConflict
	}
	return rec.Result, true, nil
}

// Put 持久化一条结果。若已有同 operation_id：
//   - hash 相同：幂等成功（保留首次结果）；
//   - hash 不同：返回 ErrRequestHashConflict。
func (l *Ledger) Put(operationID, requestHash string, result json.RawMessage) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec, ok := l.records[operationID]; ok {
		if rec.RequestHash != requestHash {
			return ErrRequestHashConflict
		}
		return nil
	}
	l.records[operationID] = Record{
		OperationID: operationID,
		RequestHash: requestHash,
		Result:      result,
		CreatedAt:   time.Now().UTC(),
	}
	return l.persistLocked()
}

func (l *Ledger) persistLocked() error {
	data, err := json.MarshalIndent(l.records, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ledger: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return fmt.Errorf("create ledger dir: %w", err)
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write ledger tmp: %w", err)
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return fmt.Errorf("rename ledger: %w", err)
	}
	return nil
}
