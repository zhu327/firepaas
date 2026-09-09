// flow.go：fabric 东西向 flow 事件读取与关联（G3，ADR-0040 §21）。
//
// 链路：tc_ingress 决策点 → flow_events ringbuf（固定尺寸事件，满即丢）
// → 本读取器 → FlowSink（identity_id 关联 project/app/machine/execution
// 之后的结构化记录）。聚合口径：EgressAuditStats（v4 代理路径）不变；
// 本路径新增低基数计数（verdict × proto，域名/IP 不进 label）。
package ebpf

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/ringbuf"
)

// FlowVerdict 是东西向决策结果（与 tc.c FLOW_* 常量对应）。
type FlowVerdict uint8

const (
	FlowAllow      FlowVerdict = 1
	FlowDenyPolicy FlowVerdict = 2
	FlowDenySrc    FlowVerdict = 3
	FlowDenyPort   FlowVerdict = 4
	FlowDenyDst    FlowVerdict = 5
)

func (v FlowVerdict) String() string {
	switch v {
	case FlowAllow:
		return "allow"
	case FlowDenyPolicy:
		return "deny_policy"
	case FlowDenySrc:
		return "deny_src"
	case FlowDenyPort:
		return "deny_port"
	case FlowDenyDst:
		return "deny_dst"
	}
	return "unknown"
}

// FlowEvent 是一条东西向决策事件（ringbuf 原始形态）。
type FlowEvent struct {
	SrcID   uint32
	DstID   uint32
	DPort   uint16
	Proto   uint8
	Verdict FlowVerdict
	PktLen  uint32
}

// FlowRecord 是关联身份后的完整 flow 记录（sink 消费形态）。身份字段
// 在 identity_id 无映射时为空（快照未覆盖的残余流量仍可见 verdict）。
type FlowRecord struct {
	FlowEvent
	SrcProject, SrcApp, SrcMachine, SrcExecution string
	DstProject, DstApp, DstMachine, DstExecution string
}

// FlowIdentity 是 identity_id → 坐标的关联源（fabric 快照投影）。
type FlowIdentity struct {
	ProjectID, AppID, MachineID, ExecutionID string
}

// IdentityResolver 提供 identity_id → 身份坐标（fabric 快照；读侧并发安全）。
type IdentityResolver interface {
	Resolve(identityID uint32) (FlowIdentity, bool)
}

// FlowSink 消费关联后的 flow 记录（agentd 注入实现：日志采样 + 低基数
// 计数）。实现必须非阻塞、永不失败（观测不得反压数据面）。
type FlowSink interface {
	Observe(rec FlowRecord)
}

// StartFlows 启动 ringbuf 读取循环（ctx 取消即停）。返回后 reader 由
// ctx 生命周期管理；错误（ringbuf 设置失败）即时返回。
func (b *Backend) StartFlows(ctx context.Context, resolve IdentityResolver, sink FlowSink) error {
	if b.flows == nil {
		return errors.New("ebpf: flow_events map unavailable")
	}
	rd, err := ringbuf.NewReader(b.flows)
	if err != nil {
		return fmt.Errorf("ebpf: flow ringbuf: %w", err)
	}
	go func() {
		<-ctx.Done()
		_ = rd.Close()
	}()
	go func() {
		for {
			rec, err := rd.Read()
			if err != nil {
				if errors.Is(err, ringbuf.ErrClosed) || ctx.Err() != nil {
					return
				}
				slog.Warn("ebpf flow read", "error", err)
				continue
			}
			ev, ok := decodeFlowEvent(rec.RawSample)
			if !ok {
				continue
			}
			sink.Observe(b.correlate(ev, resolve))
		}
	}()
	return nil
}

// decodeFlowEvent 按固定布局解码（与 tc.c struct flow_event 对齐：
// src u32, dst u32, dport u16, proto u8, verdict u8, pkt_len u32, pad u64）。
func decodeFlowEvent(raw []byte) (FlowEvent, bool) {
	const size = 4 + 4 + 2 + 1 + 1 + 4 + 8
	if len(raw) != size {
		return FlowEvent{}, false
	}
	return FlowEvent{
		SrcID:   binary.LittleEndian.Uint32(raw[0:4]),
		DstID:   binary.LittleEndian.Uint32(raw[4:8]),
		DPort:   binary.LittleEndian.Uint16(raw[8:10]),
		Proto:   raw[10],
		Verdict: FlowVerdict(raw[11]),
		PktLen:  binary.LittleEndian.Uint32(raw[12:16]),
	}, true
}

func (b *Backend) correlate(ev FlowEvent, resolve IdentityResolver) FlowRecord {
	rec := FlowRecord{FlowEvent: ev}
	if src, ok := resolve.Resolve(ev.SrcID); ok {
		rec.SrcProject, rec.SrcApp, rec.SrcMachine, rec.SrcExecution =
			src.ProjectID, src.AppID, src.MachineID, src.ExecutionID
	}
	if dst, ok := resolve.Resolve(ev.DstID); ok {
		rec.DstProject, rec.DstApp, rec.DstMachine, rec.DstExecution =
			dst.ProjectID, dst.AppID, dst.MachineID, dst.ExecutionID
	}
	return rec
}

// compile-time：ringbuf map 存在性由生成代码保证（tc_x86_bpfel.FlowEvents）。
var _ *ebpf.Map = nil
