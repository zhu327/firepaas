// Package mutation owns the agent's durable fenced-mutation protocol. Runtime
// effects and transport error mapping stay at the server/adapter boundary.
package mutation

import (
	"encoding/json"

	"github.com/zhu327/firepaas/internal/agent/state"
)

var (
	ErrConflict = state.ErrRequestHashConflict
	ErrStale    = state.ErrStaleGeneration
)

type Identity struct {
	OperationID string
	MachineID   string
	ExecutionID string
	Generation  uint64
	Kind        string
	RequestHash string
	Coordinates json.RawMessage
}

type Protocol struct {
	ledger *state.Ledger
	fences *state.Fences
}

func New(ledger *state.Ledger, fences *state.Fences) *Protocol {
	return &Protocol{ledger: ledger, fences: fences}
}

type Codec[T any] struct {
	Encode func(T) (json.RawMessage, error)
	Decode func(json.RawMessage) (T, error)
}

type CreateMachine[T any] struct {
	Identity
	Prepare           func() (release func(), err error)
	Effect            func() (T, error)
	PersistCredential func() error
	Codec             Codec[T]
}

// RunCreateMachine preserves the legacy ordering: replay; lock; fence; runtime
// create; fence advance; unlock; credential digest; completed result.
func RunCreateMachine[T any](p *Protocol, op CreateMachine[T]) (T, error) {
	var zero T
	if raw, ok, err := p.ledger.Check(op.OperationID, op.RequestHash); err != nil {
		return zero, err
	} else if ok {
		return op.Codec.Decode(raw)
	}
	if op.Prepare != nil {
		release, err := op.Prepare()
		if err != nil {
			return zero, err
		}
		if release != nil {
			defer release()
		}
	}
	unlock := p.fences.LockMachine(op.MachineID)
	if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
		unlock()
		return zero, err
	}
	out, err := op.Effect()
	if err == nil {
		err = p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID)
	}
	unlock()
	if err != nil {
		return zero, err
	}
	if op.PersistCredential != nil {
		if err := op.PersistCredential(); err != nil {
			return zero, err
		}
	}
	raw, err := op.Codec.Encode(out)
	if err != nil {
		return zero, err
	}
	if err := p.ledger.Put(op.OperationID, op.MachineID, op.RequestHash, raw); err != nil {
		return zero, err
	}
	return out, nil
}

type DeleteMachine struct {
	Identity
	Effect         func() error
	DropCredential func()
}

// RunDeleteMachine preserves completion/prune/fence ordering under the machine
// lock. Effect is also responsible for mapping NotFound and dropping its stale
// credential, matching the historical exceptional path.
func RunDeleteMachine(p *Protocol, op DeleteMachine) (bool, error) {
	if _, ok, err := p.ledger.Check(op.OperationID, op.RequestHash); err != nil || ok {
		return ok, err
	}
	unlock := p.fences.LockMachine(op.MachineID)
	defer unlock()
	if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
		return false, err
	}
	if err := op.Effect(); err != nil {
		return false, err
	}
	if op.DropCredential != nil {
		op.DropCredential()
	}
	if err := p.ledger.Put(op.OperationID, op.MachineID, op.RequestHash, []byte(`{}`)); err != nil {
		return false, err
	}
	_, _ = p.ledger.PruneMachineExcept(op.MachineID, op.OperationID)
	if err := p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID); err != nil {
		return false, err
	}
	return false, nil
}

type Lifecycle[T any] struct {
	Identity
	Effect func() (T, error)
	Codec  Codec[T]
}

// RunLifecycle preserves pause/resume's intentionally unlocked, non-advancing
// post-effect protocol.
func RunLifecycle[T any](p *Protocol, op Lifecycle[T]) (T, error) {
	var zero T
	if raw, ok, err := p.ledger.Check(op.OperationID, op.RequestHash); err != nil {
		return zero, err
	} else if ok {
		return op.Codec.Decode(raw)
	}
	if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
		return zero, err
	}
	out, err := op.Effect()
	if err != nil {
		return zero, err
	}
	raw, err := op.Codec.Encode(out)
	if err != nil {
		return zero, err
	}
	if err := p.ledger.Put(op.OperationID, op.MachineID, op.RequestHash, raw); err != nil {
		return zero, err
	}
	return out, nil
}

type Recovery[T any] struct {
	Value T
	Found bool
}

type ClaimedMutation[T any] struct {
	Identity
	SerializationKey string
	Recover          func() (Recovery[T], error)
	Effect           func() (T, error)
	Codec            Codec[T]
}

// RunResourceMutation runs an inventory-recoverable mutation serialized by a
// resource key. It deliberately has no generation-fence controls.
func RunResourceMutation[T any](p *Protocol, op ClaimedMutation[T]) (T, error) {
	return runClaimed(p, op, false, nil, nil)
}

// RunMachineMutation runs an inventory-recoverable machine mutation with a
// mandatory generation check before recovery or effect.
func RunMachineMutation[T any](p *Protocol, op ClaimedMutation[T]) (T, error) {
	return runClaimed(p, op, true, nil, nil)
}

func runClaimed[T any](p *Protocol, op ClaimedMutation[T], checkFence bool, beforeResult, beforeComplete func() error) (T, error) {
	var zero T
	key := op.SerializationKey
	if key == "" {
		key = op.MachineID
	}
	unlock := p.fences.LockMachine(key)
	defer unlock()
	rec, existed, err := p.ledger.Begin(state.Record{
		OperationID: op.OperationID, MachineID: op.MachineID, ExecutionID: op.ExecutionID,
		Generation: op.Generation, Kind: op.Kind, Identity: op.Coordinates, RequestHash: op.RequestHash,
	})
	if err != nil {
		return zero, err
	}
	if rec.Completed() {
		return op.Codec.Decode(rec.Result)
	}
	if checkFence {
		if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
			return zero, err
		}
	}
	var out T
	if existed && op.Recover != nil {
		recovered, err := op.Recover()
		if err != nil {
			return zero, err
		}
		if recovered.Found {
			out = recovered.Value
			return completeClaimed(p, op, out, beforeResult, beforeComplete)
		}
	}
	out, err = op.Effect()
	if err != nil {
		return zero, err
	}
	return completeClaimed(p, op, out, beforeResult, beforeComplete)
}

func completeClaimed[T any](p *Protocol, op ClaimedMutation[T], out T, beforeResult, beforeComplete func() error) (T, error) {
	var zero T
	if beforeResult != nil {
		if err := beforeResult(); err != nil {
			return zero, err
		}
	}
	if beforeComplete != nil {
		if err := beforeComplete(); err != nil {
			return zero, err
		}
	}
	raw, err := op.Codec.Encode(out)
	if err != nil {
		return zero, err
	}
	if err := p.ledger.Complete(op.OperationID, op.RequestHash, raw); err != nil {
		return zero, err
	}
	return out, nil
}

type Replacement[T any] struct {
	Identity
	Recover           func() (Recovery[T], error)
	Effect            func() (T, error)
	PersistCredential func() error
	Codec             Codec[T]
}

// RunReplacement is the snapshot fork/restore protocol: durable claim, replay
// or inventory recovery, fence check, durable fence advance, credential
// persistence, then completion. Recovery must advance too: a crash after the
// runtime replacement but before Advance must not leave the recovered execution
// protected only by an older on-disk high-water mark.
func RunReplacement[T any](p *Protocol, op Replacement[T]) (T, error) {
	claimed := ClaimedMutation[T]{Identity: op.Identity, Recover: op.Recover, Effect: op.Effect, Codec: op.Codec}
	advance := func() error { return p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID) }
	return runClaimed(p, claimed, true, advance, op.PersistCredential)
}

type DeleteMutation struct {
	Identity
	SerializationKey string
	AlreadyDeleted   func() (bool, error)
	Effect           func() error
}

func RunResourceDelete(p *Protocol, op DeleteMutation) error {
	return runDelete(p, op, false)
}

func RunMachineDelete(p *Protocol, op DeleteMutation) error {
	return runDelete(p, op, true)
}

func runDelete(p *Protocol, op DeleteMutation, checkFence bool) error {
	key := op.SerializationKey
	if key == "" {
		key = op.MachineID
	}
	unlock := p.fences.LockMachine(key)
	defer unlock()
	rec, existed, err := p.ledger.Begin(state.Record{OperationID: op.OperationID, MachineID: op.MachineID,
		ExecutionID: op.ExecutionID, Generation: op.Generation, Kind: op.Kind, Identity: op.Coordinates, RequestHash: op.RequestHash})
	if err != nil || rec.Completed() {
		return err
	}
	if checkFence {
		if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
			return err
		}
	}
	if existed && op.AlreadyDeleted != nil {
		deleted, err := op.AlreadyDeleted()
		if err != nil {
			return err
		}
		if deleted {
			return p.ledger.Complete(op.OperationID, op.RequestHash, []byte(`{}`))
		}
	}
	if err := op.Effect(); err != nil {
		return err
	}
	return p.ledger.Complete(op.OperationID, op.RequestHash, []byte(`{}`))
}

type CopyTo[T any] struct{ Lifecycle[T] }

// RunCopyTo preserves content-sensitive replay and holds the machine lock over
// fence check, guest write, fence advance, and result persistence.
func RunCopyTo[T any](p *Protocol, op CopyTo[T]) (T, error) {
	var zero T
	unlock := p.fences.LockMachine(op.MachineID)
	defer unlock()
	if raw, ok, err := p.ledger.Check(op.OperationID, op.RequestHash); err != nil {
		return zero, err
	} else if ok {
		return op.Codec.Decode(raw)
	}
	if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
		return zero, err
	}
	out, err := op.Effect()
	if err != nil {
		return zero, err
	}
	if err := p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID); err != nil {
		return zero, err
	}
	raw, err := op.Codec.Encode(out)
	if err != nil {
		return zero, err
	}
	if err := p.ledger.Put(op.OperationID, op.MachineID, op.RequestHash, raw); err != nil {
		return zero, err
	}
	return out, nil
}

func ClaimExec(p *Protocol, id Identity, tombstone json.RawMessage) (bool, error) {
	return p.ledger.Claim(id.OperationID, id.MachineID, id.RequestHash, tombstone)
}
