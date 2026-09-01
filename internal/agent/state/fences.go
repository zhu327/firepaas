// fences.go 实现 machine 级 generation 高水位（M1 评审 P0-2 修复）。
//
// 语义（architecture.md §6 / protos/agent/v1 注释）：
//   - agent 只接受不早于已知 generation 的变更请求；
//   - Check：generation < MaxGeneration → ErrStaleGeneration（FailedPrecondition）；
//   - Advance：仅在变更成功后推进高水位（失败/重试不推进）；
//   - machine 删除后保留高水位：更旧的 re-create 仍被拒绝，直到 GC 窗口过期。
//
// 并发说明：Check 与 Advance 各自原子。WithMachine 将同一 machine 的
// "检查→执行→推进"临界区串行化，供会触及实际实例的 RPC 使用。
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

// ErrStaleGeneration 表示请求的 generation 早于该 machine 已知的最高 generation。
var ErrStaleGeneration = errors.New("stale generation")

// FenceEntry 记录单个 machine 的 generation 高水位。
type FenceEntry struct {
	MaxGeneration   uint64    `json:"max_generation"`
	LastExecutionID string    `json:"last_execution_id,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Fences 是 machine_id → FenceEntry 的持久化映射，带原子落盘。
type Fences struct {
	mu      sync.Mutex
	path    string
	entries map[string]FenceEntry
	// machineLocks 只在进程内保护同一 machine 的复合操作；fence 本身仍由
	// mu 保护并持久化。锁不落盘，进程重启后没有遗留持锁状态。
	machineLocks map[string]*sync.Mutex
}

// OpenFences 加载 fences；文件不存在时从空状态开始。
func OpenFences(path string) (*Fences, error) {
	f := &Fences{path: path, entries: map[string]FenceEntry{}, machineLocks: map[string]*sync.Mutex{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return f, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read fences: %w", err)
	}
	if len(data) == 0 {
		return f, nil
	}
	if err := json.Unmarshal(data, &f.entries); err != nil {
		return nil, fmt.Errorf("parse fences %s: %w", path, err)
	}
	return f, nil
}

// LockMachine acquires the process-local lock for one serialization key and
// returns its unlock function. It lets protocol owners keep the lock across
// fence checks, runtime effects, and durable completion without hiding those
// ordered steps in a callback wrapper. The returned function must be called
// exactly once; callers must not recursively lock the same key.
func (f *Fences) LockMachine(machineID string) func() {
	f.mu.Lock()
	lock := f.machineLocks[machineID]
	if lock == nil {
		lock = &sync.Mutex{}
		f.machineLocks[machineID] = lock
	}
	f.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

// WithMachine is retained for existing callers.
func (f *Fences) WithMachine(machineID string, fn func() error) error {
	unlock := f.LockMachine(machineID)
	defer unlock()
	return fn()
}

// Check 校验 generation 不早于该 machine 的已知高水位；未知 machine 任何
// generation 都通过（generation >= MaxGeneration 均通过，包括相等——
// 相等即同代请求，幂等性由 operation ledger 承担）。
func (f *Fences) Check(machineID string, generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[machineID]
	if ok && generation < e.MaxGeneration {
		return fmt.Errorf("%w: machine %s is at generation %d, request carried %d",
			ErrStaleGeneration, machineID, e.MaxGeneration, generation)
	}
	return nil
}

// Advance 推进高水位（只在变更成功后调用）。generation 低于当前高水位时为
// no-op（正常流程 Check 已拦截；防御性不回退）。executionID 仅作调试信息。
func (f *Fences) Advance(machineID string, generation uint64, executionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.entries[machineID]
	if ok && generation < e.MaxGeneration {
		return nil
	}
	if ok && generation == e.MaxGeneration {
		// 同代新操作（如 create 后的同代 delete）：只刷新调试字段。
		e.LastExecutionID = executionID
		e.UpdatedAt = time.Now().UTC()
		f.entries[machineID] = e
		return f.persistLocked()
	}
	f.entries[machineID] = FenceEntry{
		MaxGeneration:   generation,
		LastExecutionID: executionID,
		UpdatedAt:       time.Now().UTC(),
	}
	return f.persistLocked()
}

// PruneBefore 删除 UpdatedAt 早于 cutoff 的条目（与 ledger 共享 GC 窗口），
// 返回删除条数。machine 已删除且高水位在其保留窗口内仍拒绝旧请求。
func (f *Fences) PruneBefore(cutoff time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	removed := 0
	for id, e := range f.entries {
		if e.UpdatedAt.Before(cutoff) {
			delete(f.entries, id)
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	if err := f.persistLocked(); err != nil {
		return removed, err
	}
	return removed, nil
}

func (f *Fences) persistLocked() error {
	data, err := json.MarshalIndent(f.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal fences: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return fmt.Errorf("create fences dir: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.OpenFile(f.path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open fences tmp: %w", err)
	}
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write fences tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("fsync fences tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("close fences tmp: %w", err)
	}
	if err := os.Rename(tmp.Name(), f.path); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("rename fences: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open fences dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync fences dir: %w", err)
	}
	return nil
}
