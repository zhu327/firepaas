// Package mutation owns the agent's durable fenced-mutation protocol. Runtime
// effects and transport error mapping stay at the server/adapter boundary.
package mutation

import (
	"encoding/json"
	"log/slog"

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
	Recover           func() (Recovery[T], error)
	Effect            func() (T, error)
	PersistCredential func() error
	Codec             Codec[T]
}

// RunCreateMachine 是 create 的 durable-claim 协议（与 runClaimed 同一模型）：
// 已完成重放；generation fence；Effect 前持久化 claim（含 request_hash）；
// 未完成 claim 的重试经 hypeman inventory 恢复——实例存在则认领并走正常
// 完成序列，不存在则在同一 claim 下重跑 Effect（不再二次撞同名拒绝）；
// fence advance → credential → durable Complete。重放路径同时补写可能在
// 崩溃窗口丢失的 credential。fence 高水位语义与 legacy 一致：stale 拒绝、
// 同代放行、完成重放不碰 fence、claim 本身不推进高水位。
func RunCreateMachine[T any](p *Protocol, op CreateMachine[T]) (T, error) {
	var zero T
	unlock := p.fences.LockMachine(op.MachineID)
	defer unlock()
	// 已完成重放：按记录返回结果，不碰 fence（幂等性优先）。同时补写
	// credential——首次完成在 ledger 落盘、creds 落盘之前崩溃（或 creds.json
	// 损坏）时，重放必须恢复 :5107 可用，否则该 machine 永久 403。
	if raw, ok, err := p.ledger.Check(op.OperationID, op.RequestHash); err != nil {
		return zero, err
	} else if ok {
		if op.PersistCredential != nil {
			if err := op.PersistCredential(); err != nil {
				return zero, err
			}
		}
		return op.Codec.Decode(raw)
	}
	// generation fence 先于 claim：被拒的 stale 请求不留 ledger 记录。
	if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
		return zero, err
	}
	rec, existed, err := p.ledger.Begin(state.Record{
		OperationID: op.OperationID, MachineID: op.MachineID, ExecutionID: op.ExecutionID,
		Generation: op.Generation, Kind: op.Kind, Identity: op.Coordinates, RequestHash: op.RequestHash,
	})
	if err != nil {
		return zero, err
	}
	// machine 锁下没有并发完成者；Begin 返回 completed 只可能是防御性分支
	//（上面的 Check 已拦截正常重放）。按同一补写语义处理。
	if rec.Completed() {
		if op.PersistCredential != nil {
			if err := op.PersistCredential(); err != nil {
				return zero, err
			}
		}
		return op.Codec.Decode(rec.Result)
	}
	// 崩溃窗口重试（claim 未完成）：先查 hypeman inventory。实例存在 = 上次
	// Effect 已生效只是完成没落盘——认领实例并按正常完成序列收尾（等于幂等
	// 成功，返回真实 machine 状态）；不存在 = Effect 从未生效或实例已被回收，
	// 直接在同一 claim 下重跑 Effect。
	if existed && op.Recover != nil {
		recovered, err := op.Recover()
		if err != nil {
			return zero, err
		}
		if recovered.Found {
			return completeCreate(p, op, recovered.Value)
		}
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
	out, err := op.Effect()
	if err != nil {
		return zero, err
	}
	return completeCreate(p, op, out)
}

// completeCreate 完成 create：fence 推进 → credential → durable 完成。顺序
// 与 legacy 一致——credential 失败时留下可恢复的未完成 claim，完成记录
// 永远最后落盘。
func completeCreate[T any](p *Protocol, op CreateMachine[T], out T) (T, error) {
	var zero T
	if err := p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID); err != nil {
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
	if err := p.ledger.Complete(op.OperationID, op.RequestHash, raw); err != nil {
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
	if _, err := p.ledger.PruneMachineExcept(op.MachineID, op.OperationID); err != nil {
		// prune 失败不阻断 delete 语义：冗余的去重/fence 记录由保留窗口 GC 兜底。
		slog.Warn("ledger prune after delete failed; stale records linger until retention gc",
			"machine_id", op.MachineID, "operation_id", op.OperationID, "error", err)
	}
	if err := p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID); err != nil {
		return false, err
	}
	return false, nil
}

type Lifecycle[T any] struct {
	Identity
	// Recover 为 nil 时等同于无恢复（Effect 总是重跑）。
	Recover func() (Recovery[T], error)
	Effect  func() (T, error)
	Codec   Codec[T]
}

// RunLifecycle 是 pause/resume 的 claimed mutation（R2：不再是“intentionally
// unlocked, non-advancing”）。与 create/delete 共享 LockMachine 串行化：
// Effect 前持久化 claim；未完成 claim 的重试经 Recover 从实例实际状态
// 收敛（已处目标态即认领成功），未收敛则在同一 claim 下重跑幂等 Effect；
// fence 通过后推进高水位（Advance）再 durable Complete。legacy 已完成记录
// （无 status 字段）照常按结果重放，无需迁移。
func RunLifecycle[T any](p *Protocol, op Lifecycle[T]) (T, error) {
	claimed := ClaimedMutation[T]{
		Identity: op.Identity, Recover: op.Recover, Effect: op.Effect, Codec: op.Codec,
	}
	advance := func() error { return p.fences.Advance(op.MachineID, op.Generation, op.ExecutionID) }
	return runClaimed(p, claimed, true, advance, nil)
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

func runClaimed[T any](
	p *Protocol,
	op ClaimedMutation[T],
	checkFence bool,
	beforeResult, beforeComplete func() error,
) (T, error) {
	var zero T
	key := op.SerializationKey
	if key == "" {
		key = op.MachineID
	}
	unlock := p.fences.LockMachine(key)
	defer unlock()
	// 顺序与 RunCreateMachine 一致：已完成重放 → fence → Begin。
	// Begin 前先看已完成记录（重放不受 fence 约束——结果已提交，高水位
	// 可能已推进）；新请求则先过 fence 再写 claim，避免 stale 请求留下
	// 孤儿 PENDING claim（R2 审查 P3）。LockMachine 已串行化同 machine，
	// Get 与 Begin 之间不产生窗口。
	if rec, ok, err := p.ledger.Get(op.OperationID, op.RequestHash); err != nil {
		return zero, err
	} else if ok && rec.Completed() {
		return op.Codec.Decode(rec.Result)
	}
	if checkFence {
		if err := p.fences.Check(op.MachineID, op.Generation); err != nil {
			return zero, err
		}
	}
	rec, existed, err := p.ledger.Begin(state.Record{
		OperationID: op.OperationID, MachineID: op.MachineID, ExecutionID: op.ExecutionID,
		Generation: op.Generation, Kind: op.Kind, Identity: op.Coordinates, RequestHash: op.RequestHash,
	})
	if err != nil {
		return zero, err
	}
	if rec.Completed() {
		// 防御：同 key 已串行化，开始重放检查后不应再有新完成记录。
		return op.Codec.Decode(rec.Result)
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

func completeClaimed[T any](
	p *Protocol,
	op ClaimedMutation[T],
	out T,
	beforeResult, beforeComplete func() error,
) (T, error) {
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
	rec, existed, err := p.ledger.Begin(state.Record{
		OperationID: op.OperationID, MachineID: op.MachineID,
		ExecutionID: op.ExecutionID, Generation: op.Generation, Kind: op.Kind, Identity: op.Coordinates, RequestHash: op.RequestHash,
	})
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
