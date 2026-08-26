// Package state 实现 agent 侧 operation ledger（M1.4）。
//
// 语义（mvp-plan §5.5）：
//   - 同一 operation_id 重试返回已记录结果；
//   - 相同 operation_id、不同 request hash 被拒绝；
//   - 重启后从磁盘重放；
//   - 记录带 machine_id：machine 删除后立即清理同 machine 的历史记录，
//     另有可配置的年龄 GC 窗口防止无限增长（M1 评审 P2-5）。
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
	MachineID   string          `json:"machine_id,omitempty"` // M1 评审前的历史记录可能为空
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
func (l *Ledger) Put(operationID, machineID, requestHash string, result json.RawMessage) error {
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
		MachineID:   machineID,
		RequestHash: requestHash,
		Result:      result,
		CreatedAt:   time.Now().UTC(),
	}
	return l.persistLocked()
}

// PruneBefore 删除 createdAt 早于 cutoff 的记录（年龄 GC 窗口），返回删除条数。
func (l *Ledger) PruneBefore(cutoff time.Time) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for id, rec := range l.records {
		if rec.CreatedAt.Before(cutoff) {
			delete(l.records, id)
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	if err := l.persistLocked(); err != nil {
		return removed, err
	}
	return removed, nil
}

// PruneMachineExcept 删除指定 machine 的全部记录（保留 keepOperationID 一条）。
// 在 machine 删除成功后调用：避免后续重放旧 create 时返回已删除 VM 的“成功”
// 结果，同时保留 delete 自身的去重记录。machineID 为空的旧记录不受影响。
func (l *Ledger) PruneMachineExcept(machineID, keepOperationID string) (int, error) {
	if machineID == "" {
		return 0, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for id, rec := range l.records {
		if rec.MachineID == machineID && id != keepOperationID {
			delete(l.records, id)
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	if err := l.persistLocked(); err != nil {
		return removed, err
	}
	return removed, nil
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
