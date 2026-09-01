// Package standby 是 agentd 的 auto-standby 装配层（v1.1，ADR-0017）。
//
// hypeman lib/autostandby.Controller 提供 conntrack 驱动的空闲检测与 standby
// 执行；本包提供 firepaas 侧的两个适配器：
//
//   - instanceStore：InstanceStore 接口的 instances.Manager 适配（参照
//     hypeman lib/providers/auto_standby_linux.go 的装配模式；providers 版
//     import cmd/api/config，不可直接复用）；
//   - filteredSource：ConnectionSource 包装，把 host 侧 readiness 探针连接
//     （probeflow registry 登记）从 conntrack 视图中剔除——探针不清闲，
//     但真实流量（agent proxy 转发）正常计数。
//
// 唤醒路径不变：M4.5 autoresume（proxy GetEndpoint 遇 standby 同步 restore）。
package standby

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/kernel/hypeman/lib/autostandby"
	"github.com/kernel/hypeman/lib/instances"

	"github.com/zhu327/firepaas/internal/agent/probeflow"
)

// runtimeManager 是 hypeman 实例管理器上 auto-standby runtime 持久化的能力
// 子集（具体类型 *instances.manager 满足；接口层未暴露，需类型断言）。
type runtimeManager interface {
	GetAutoStandbyRuntime(ctx context.Context, id string) (*autostandby.Runtime, error)
	SetAutoStandbyRuntime(ctx context.Context, id string, runtime *autostandby.Runtime) error
}

// instanceStore 把 instances.Manager 适配为 autostandby.InstanceStore。
type instanceStore struct {
	manager instances.Manager
	runtime runtimeManager
}

// NewInstanceStore 构造适配器。mgr 必须同时实现 runtimeManager（返回 nil
// 表示不可用，调用方降级关闭 auto-standby）。
func NewInstanceStore(mgr instances.Manager) *instanceStore {
	rm, ok := mgr.(runtimeManager)
	if !ok {
		return nil
	}
	return &instanceStore{manager: mgr, runtime: rm}
}

func (s *instanceStore) ListInstances(ctx context.Context) ([]autostandby.Instance, error) {
	insts, err := s.manager.ListInstances(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]autostandby.Instance, 0, len(insts))
	for i := range insts {
		inst := &insts[i]
		runtime, err := s.runtime.GetAutoStandbyRuntime(ctx, inst.Id)
		if err != nil {
			return nil, err
		}
		out = append(out, autostandby.Instance{
			ID:             inst.Id,
			Name:           inst.Name,
			State:          string(inst.State),
			NetworkEnabled: inst.NetworkEnabled,
			IP:             inst.IP,
			AutoStandby:    inst.AutoStandby,
			Runtime:        runtime,
		})
	}
	return out, nil
}

func (s *instanceStore) StandbyInstance(ctx context.Context, id string) error {
	_, err := s.manager.StandbyInstance(ctx, id, instances.StandbyInstanceRequest{})
	if errors.Is(err, instances.ErrNotFound) {
		return fmt.Errorf("%w: %v", autostandby.ErrInstanceNotFound, err)
	}
	return err
}

func (s *instanceStore) SetRuntime(ctx context.Context, id string, runtime *autostandby.Runtime) error {
	return s.runtime.SetAutoStandbyRuntime(ctx, id, runtime)
}

func (s *instanceStore) SubscribeInstanceEvents() (<-chan autostandby.InstanceEvent, func(), error) {
	src, unsub := s.manager.SubscribeLifecycleEvents(instances.LifecycleEventConsumerAutoStandby)
	dst := make(chan autostandby.InstanceEvent, 32)
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(done)
			unsub()
		})
	}
	go func() {
		defer close(dst)
		for event := range src {
			var inst *autostandby.Instance
			if event.Instance != nil {
				inst = &autostandby.Instance{
					ID:             event.Instance.Id,
					Name:           event.Instance.Name,
					State:          string(event.Instance.State),
					NetworkEnabled: event.Instance.NetworkEnabled,
					IP:             event.Instance.IP,
					AutoStandby:    event.Instance.AutoStandby,
				}
				if runtime, err := s.runtime.GetAutoStandbyRuntime(context.Background(), inst.ID); err == nil {
					inst.Runtime = runtime
				}
			}
			out := autostandby.InstanceEvent{
				Action:     autostandby.InstanceEventAction(event.Action),
				InstanceID: event.InstanceID,
				Instance:   inst,
			}
			select {
			case dst <- out:
			case <-done:
				return
			}
		}
	}()
	return dst, stop, nil
}

// ---------------------------------------------------------------------------
// filteredSource：探针连接过滤
// ---------------------------------------------------------------------------

// filteredSource 包装 ConnectionSource，丢弃 probeflow registry 命中的连接
// （readiness 探针）。真实流量（proxy 转发、外部直连）不受影响。
type filteredSource struct {
	inner autostandby.ConnectionSource
	reg   *probeflow.Registry
}

// NewFilteredSource 构造过滤源。reg 为 nil 时透传。
func NewFilteredSource(inner autostandby.ConnectionSource, reg *probeflow.Registry) autostandby.ConnectionSource {
	if reg == nil {
		return inner
	}
	return &filteredSource{inner: inner, reg: reg}
}

func (f *filteredSource) ListConnections(ctx context.Context) ([]autostandby.Connection, error) {
	conns, err := f.inner.ListConnections(ctx)
	if err != nil {
		return nil, err
	}
	out := conns[:0]
	for _, c := range conns {
		if f.isProbe(c) {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func (f *filteredSource) OpenStream(ctx context.Context) (autostandby.ConnectionStream, error) {
	stream, err := f.inner.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	return &filteredStream{inner: stream, reg: f.reg}, nil
}

func (f *filteredSource) isProbe(c autostandby.Connection) bool {
	return f.reg.Match(
		c.OriginalSourcePort,
		addrPort(c.OriginalDestinationIP, c.OriginalDestinationPort),
	)
}

// addrPort 把 conntrack 连接的地址+端口合成 AddrPort（无 zone）。
func addrPort(ip netip.Addr, port uint16) netip.AddrPort {
	return netip.AddrPortFrom(ip.Unmap(), port)
}

type filteredStream struct {
	inner  autostandby.ConnectionStream
	reg    *probeflow.Registry
	once   sync.Once
	events chan autostandby.ConnectionEvent
}

func (s *filteredStream) Events() <-chan autostandby.ConnectionEvent {
	s.once.Do(func() {
		s.events = make(chan autostandby.ConnectionEvent, 64)
		inner := s.inner
		go func() {
			defer close(s.events)
			for ev := range inner.Events() {
				if s.reg.Match(
					ev.Connection.OriginalSourcePort,
					addrPort(ev.Connection.OriginalDestinationIP, ev.Connection.OriginalDestinationPort),
				) {
					continue
				}
				s.events <- ev
			}
		}()
	})
	return s.events
}

func (s *filteredStream) Errors() <-chan error {
	return s.inner.Errors()
}

func (s *filteredStream) Close() error {
	return s.inner.Close()
}
