// guestops.go：v1.2-C（ADR-0025）guest 运维通道的 adapter 接入。
// 数据路径：control-plane → agent gRPC → hypeman lib/guest vsock → guest agent。
// 日志例外：serial console 由 hypeman lib/instances StreamInstanceLogs 提供
// （实时日志不经过对象存储/guest agent）。
package machine

import (
	"context"
	"errors"
	"fmt"

	contracts "github.com/zhu327/firepaas/internal/contracts/agentv1"
	"github.com/kernel/hypeman/lib/hypervisor"
	"github.com/kernel/hypeman/lib/instances"
)

// ErrGuestOpsUnsupported 表示 hypeman instance manager 未提供 guest 运维通道
// 能力（老版本上游 / 测试替身）。控制面应按 capability error 处理。
var ErrGuestOpsUnsupported = errors.New("guest operations unsupported by instance manager")

// ErrStaleExecution 表示请求绑定旧 execution（ADR-0025：立即拒绝）。
var ErrStaleExecution = errors.New("execution mismatch")

// logStreamProvider 是 hypeman instances.Manager 的日志能力子集。
type logStreamProvider interface {
	StreamInstanceLogs(ctx context.Context, id string, tail int, follow bool, source instances.LogSource) (<-chan string, error)
}

// vsockProvider 是 hypeman instances.Manager 的 vsock dialer 能力子集。
type vsockProvider interface {
	GetVsockDialer(ctx context.Context, instanceID string) (hypervisor.VsockDialer, error)
}

// resolveGuest 解析 machine 并校验 execution（tags 比对，与 Delete/GetEndpoint
// 同一纪律；旧 execution 立即拒绝）。
func (a *Adapter) resolveGuest(ctx context.Context, machineID, executionID string) (*instances.Instance, error) {
	inst, err := a.instances.GetInstance(ctx, machineID)
	if err != nil {
		if errors.Is(err, instances.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrMachineNotFound, machineID)
		}
		return nil, fmt.Errorf("get instance %s: %w", machineID, err)
	}
	if executionID != "" && inst.Tags[tagExecution] != executionID {
		return nil, fmt.Errorf("%w: machine %s want %s got %s",
			ErrStaleExecution, machineID, executionID, inst.Tags[tagExecution])
	}
	return inst, nil
}

// StreamLogs 返回 app serial console 日志流（tail 行历史 + 可选 follow）。
func (a *Adapter) StreamLogs(ctx context.Context, machineID, executionID string,
	tail int, follow bool) (<-chan string, error) {
	inst, err := a.resolveGuest(ctx, machineID, executionID)
	if err != nil {
		return nil, err
	}
	lp, ok := a.instances.(logStreamProvider)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	return lp.StreamInstanceLogs(ctx, inst.Id, tail, follow, instances.LogSourceApp)
}

// VerifyExecution re-checks that a live session still belongs to the requested
// execution. Runtime handlers call it periodically so replacement terminates
// already-established sessions rather than only fencing session creation.
func (a *Adapter) VerifyExecution(ctx context.Context, machineID, executionID string) error {
	_, err := a.resolveGuest(ctx, machineID, executionID)
	return err
}

// GuestDialer 返回 machine 当前 execution 的 vsock dialer（exec/cp 通道）。
func (a *Adapter) GuestDialer(ctx context.Context, machineID, executionID string) (hypervisor.VsockDialer, error) {
	inst, err := a.resolveGuest(ctx, machineID, executionID)
	if err != nil {
		return nil, err
	}
	vp, ok := a.instances.(vsockProvider)
	if !ok {
		return nil, ErrGuestOpsUnsupported
	}
	return vp.GetVsockDialer(ctx, inst.Id)
}

// ValidateGuestFilePath 校验单文件 cp 的 guest 路径（复用契约校验，保持
// agent 与控制面两端语义一致）。guest agent 侧仍做最终 chroot 约束与文件
// 类型检查（双保险）。
func ValidateGuestFilePath(p string) error {
	return contracts.ValidateGuestPath(p)
}
