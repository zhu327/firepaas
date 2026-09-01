// Package state implements the agent-side durable operation ledger.
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

var ErrRequestHashConflict = errors.New("operation_id already exists with a different request hash")

// OperationStatus is omitted by legacy completed records. An empty status is
// therefore interpreted as completed when a ledger written by an older agent is loaded.
type OperationStatus string

const (
	StatusInProgress OperationStatus = "in_progress"
	StatusCompleted  OperationStatus = "completed"
)

// Record is one durable operation. Kind and identity contain only stable,
// non-secret recovery coordinates (for example snapshot_id or volume_id).
type Record struct {
	OperationID string          `json:"operation_id"`
	MachineID   string          `json:"machine_id,omitempty"`
	ExecutionID string          `json:"execution_id,omitempty"`
	Generation  uint64          `json:"generation,omitempty"`
	Kind        string          `json:"kind,omitempty"`
	Identity    json.RawMessage `json:"identity,omitempty"`
	RequestHash string          `json:"request_hash"`
	Status      OperationStatus `json:"status,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at,omitempty"`
}

func (r Record) Completed() bool { return r.Status == "" || r.Status == StatusCompleted }

type Ledger struct {
	mu      sync.Mutex
	path    string
	records map[string]Record
}

func Open(path string) (*Ledger, error) {
	l := &Ledger{path: path, records: map[string]Record{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ledger: %w", err)
	}
	if len(data) != 0 {
		if err := json.Unmarshal(data, &l.records); err != nil {
			return nil, fmt.Errorf("parse ledger %s: %w", path, err)
		}
	}
	return l, nil
}

// Get returns either an in-progress claim or a completed result.
func (l *Ledger) Get(operationID, requestHash string) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.records[operationID]
	if !ok {
		return Record{}, false, nil
	}
	if rec.RequestHash != requestHash {
		return Record{}, false, ErrRequestHashConflict
	}
	return rec, true, nil
}

// Check preserves the original API: only completed records are replayable.
func (l *Ledger) Check(operationID, requestHash string) (json.RawMessage, bool, error) {
	rec, ok, err := l.Get(operationID, requestHash)
	if err != nil || !ok || !rec.Completed() {
		return nil, false, err
	}
	return rec.Result, true, nil
}

// Begin durably claims an operation before its first external side effect.
// existing is true for both an in-progress retry and a completed replay.
func (l *Ledger) Begin(rec Record) (stored Record, existing bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if old, ok := l.records[rec.OperationID]; ok {
		if old.RequestHash != rec.RequestHash {
			return Record{}, false, ErrRequestHashConflict
		}
		return old, true, nil
	}
	now := time.Now().UTC()
	rec.Status = StatusInProgress
	rec.Result = nil
	rec.CreatedAt = now
	rec.UpdatedAt = now
	l.records[rec.OperationID] = rec
	if err := l.persistLocked(); err != nil {
		delete(l.records, rec.OperationID)
		return Record{}, false, err
	}
	return rec, false, nil
}

// Complete atomically changes a durable claim to completed and records its response.
func (l *Ledger) Complete(operationID, requestHash string, result json.RawMessage) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.records[operationID]
	if !ok {
		return fmt.Errorf("operation %s has no durable claim", operationID)
	}
	if rec.RequestHash != requestHash {
		return ErrRequestHashConflict
	}
	if rec.Completed() {
		return nil
	}
	old := rec
	rec.Status = StatusCompleted
	rec.Result = append(json.RawMessage(nil), result...)
	rec.UpdatedAt = time.Now().UTC()
	l.records[operationID] = rec
	if err := l.persistLocked(); err != nil {
		l.records[operationID] = old
		return err
	}
	return nil
}

// Put remains compatible with pre-claim callers and writes a completed record.
func (l *Ledger) Put(operationID, machineID, requestHash string, result json.RawMessage) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec, ok := l.records[operationID]; ok {
		if rec.RequestHash != requestHash {
			return ErrRequestHashConflict
		}
		if rec.Completed() {
			return nil
		}
		old := rec
		rec.Status, rec.Result, rec.UpdatedAt = StatusCompleted, append(json.RawMessage(nil), result...), time.Now().UTC()
		l.records[operationID] = rec
		if err := l.persistLocked(); err != nil {
			l.records[operationID] = old
			return err
		}
		return nil
	}
	now := time.Now().UTC()
	l.records[operationID] = Record{OperationID: operationID, MachineID: machineID,
		RequestHash: requestHash, Status: StatusCompleted, Result: result, CreatedAt: now, UpdatedAt: now}
	if err := l.persistLocked(); err != nil {
		delete(l.records, operationID)
		return err
	}
	return nil
}

// Claim is retained for existing callers; its supplied result represents a
// completed tombstone, as it did in the old ledger model.
func (l *Ledger) Claim(operationID, machineID, requestHash string, result json.RawMessage) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec, ok := l.records[operationID]; ok {
		if rec.RequestHash != requestHash {
			return false, ErrRequestHashConflict
		}
		return false, nil
	}
	now := time.Now().UTC()
	l.records[operationID] = Record{OperationID: operationID, MachineID: machineID,
		RequestHash: requestHash, Status: StatusCompleted, Result: result, CreatedAt: now, UpdatedAt: now}
	if err := l.persistLocked(); err != nil {
		delete(l.records, operationID)
		return false, err
	}
	return true, nil
}

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

// persistLocked uses the crash-safe sequence write temp, fsync temp, rename,
// fsync parent. A successful return means both data and directory entry are durable.
func (l *Ledger) persistLocked() error {
	data, err := json.MarshalIndent(l.records, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ledger: %w", err)
	}
	dir := filepath.Dir(l.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create ledger dir: %w", err)
	}
	tmp, err := os.OpenFile(l.path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open ledger tmp: %w", err)
	}
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write ledger tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("fsync ledger tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("close ledger tmp: %w", err)
	}
	if err := os.Rename(tmp.Name(), l.path); err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("rename ledger: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open ledger dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync ledger dir: %w", err)
	}
	return nil
}
