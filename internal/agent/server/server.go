// Package server 是 agent 的 gRPC 服务实现（M1.4）。
// 依赖方向：server -> machine / image / info / state，反向禁止。
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/example/firepaas/internal/agent/info"
	"github.com/example/firepaas/internal/agent/machine"
	"github.com/example/firepaas/internal/agent/state"
	contracts "github.com/example/firepaas/internal/contracts/agentv1"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"github.com/kernel/hypeman/lib/instances"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server 聚合 InfoService / MachineService。
type Server struct {
	pb.UnimplementedInfoServiceServer
	pb.UnimplementedMachineServiceServer

	machines *machine.Adapter
	ledger   *state.Ledger
	fences   *state.Fences
	info     *info.Provider
	creds    *state.Creds // M4：proxy credential 验证材料（仅摘要，ADR-0006）

	// M4：强制要求 create 携带 proxy credential（兼容开关，默认 true）。
	requireCredential bool

	// P3-7：在途 create 的资源计数。admit 的快照（实例 Size 之和）只能
	// 看到已落地的 machine；并发 create 在“检查→落地”窗口内互相不可见，
	// 会同时通过准入（TOCTOU）。单写者串行派发下不可达，但硬准入是
	// “不越过资源硬上限”的最后防线，不能依赖上游行为。
	inflightVCPU atomic.Int64
	inflightMem  atomic.Int64
}

// New 构造 Server。fences 提供 generation 高水位（P0-2）：
// 变更请求先查 ledger 幂等（重放返回原结果），再校验/推进 generation fence。
// New 构造 Server。fences 提供 generation 高水位（P0-2）；creds 为 M4 proxy
// credential 验证材料（可 nil = 不校验，仅测试用）。opts 可选扩展。
func New(machines *machine.Adapter, ledger *state.Ledger, fences *state.Fences, info *info.Provider, opts ...Option) *Server {
	s := &Server{machines: machines, ledger: ledger, fences: fences, info: info,
		requireCredential: true}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Option 定制 Server（M4：向后兼容的可选参数）。
type Option func(*Server)

// WithCreds 注入验证材料存储（nil 之外）。同时关闭强制校验时用 WithCredentialRequired。
func WithCreds(c *state.Creds) Option { return func(s *Server) { s.creds = c } }

// WithCredentialRequired 控制 create 是否强制携带 proxy credential。
func WithCredentialRequired(v bool) Option { return func(s *Server) { s.requireCredential = v } }

// ServiceInfo 实现 InfoService。
func (s *Server) ServiceInfo(context.Context, *emptypb.Empty) (*pb.ServiceInfoResponse, error) {
	return s.info.Response(), nil
}

// CreateMachine 实现带 fencing/幂等的创建。
func (s *Server) CreateMachine(ctx context.Context, req *pb.CreateMachineRequest) (*pb.CreateMachineResponse, error) {
	if err := contracts.ValidateCreateRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	hash := hashRequest(req)

	// M4（ADR-0006 收口）：execution-bound proxy credential 单向下发。
	// 缺失即拒绝（fail-closed；兼容开关仅限过渡期）。
	if s.creds != nil && s.requireCredential && req.GetProxyCredential() == "" {
		return nil, status.Error(codes.InvalidArgument,
			"missing proxy_credential (execution-bound traffic token required)")
	}

	if raw, ok, err := s.ledger.Check(req.OperationId, hash); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	} else if ok {
		// 幂等重放：返回已记录结果，不再过 generation fence
		// （该操作首次执行时 fence 语义已成立）。
		var resp pb.CreateMachineResponse
		if err := protojson.Unmarshal(raw, &resp); err != nil {
			return nil, status.Errorf(codes.Internal, "replay ledger result: %v", err)
		}
		return &resp, nil
	}

	// generation fencing（P0-2）：拒绝早于已知高水位的请求。
	if err := s.fences.Check(req.MachineId, req.Generation); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// P3-7：把本请求计入在途后再做硬准入；defer 扣回，保证并发 create 互可见。
	wantVCPU, wantMem := admitSize(req)
	s.inflightVCPU.Add(int64(wantVCPU))
	s.inflightMem.Add(int64(wantMem))
	defer s.inflightVCPU.Add(-int64(wantVCPU))
	defer s.inflightMem.Add(-int64(wantMem))

	// M2.2 本机硬准入（双保险）：调度器是软决策，这里按真实容量/承诺量拒绝。
	if err := s.admit(req); err != nil {
		return nil, err
	}

	m, err := s.machines.Create(ctx, req)
	if err != nil {
		// 镜像不可解析/拉取、解包超限是永久性错误：InvalidArgument 让
		// controller 停止无限重派（发布失败自动回滚依赖这一点快速触发）。
		if errors.Is(err, machine.ErrImageNotFound) || errors.Is(err, machine.ErrImageTooBig) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.CreateMachineResponse{Machine: m}

	// M4：create 成功即登记/替换验证材料（execution 替换时旧凭证自动失效）。
	if s.creds != nil && req.GetProxyCredential() != "" {
		if err := s.creds.Set(req.MachineId, req.Spec.GetExecutionId(),
			state.Digest(req.GetProxyCredential())); err != nil {
			return nil, status.Errorf(codes.Internal, "persist credential digest: %v", err)
		}
	}

	raw, err := protojson.Marshal(resp)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal create result: %v", err)
	}
	if err := s.ledger.Put(req.OperationId, req.MachineId, hash, raw); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	if err := s.fences.Advance(req.MachineId, req.Generation, req.Spec.GetExecutionId()); err != nil {
		return nil, status.Errorf(codes.Internal, "advance generation fence: %v", err)
	}
	return resp, nil
}

// ListMachines 实现列表（M1 无分页）。
func (s *Server) ListMachines(ctx context.Context, req *pb.ListMachinesRequest) (*pb.ListMachinesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	machines, err := s.machines.List(ctx, req.ProjectId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ListMachinesResponse{Machines: machines}, nil
}

// DeleteMachine 实现带 fencing/幂等的删除。
func (s *Server) DeleteMachine(ctx context.Context, req *pb.DeleteMachineRequest) (*emptypb.Empty, error) {
	if err := contracts.ValidateDeleteRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	hash := hashRequest(req)

	if _, ok, err := s.ledger.Check(req.OperationId, hash); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	} else if ok {
		return &emptypb.Empty{}, nil
	}

	// generation fencing（P0-2）：拒绝早于已知高水位的删除。
	if err := s.fences.Check(req.MachineId, req.Generation); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	if err := s.machines.Delete(ctx, req.MachineId); err != nil {
		// NotFound 单独映射：控制面把“agent 侧已不存在”当作幂等成功收敛，
		// 其余错误保持 Internal（M1 评审 P2-3 配套）。
		if errors.Is(err, instances.ErrNotFound) {
			// M4：实例已不在也照样撤销验证材料（fail-closed 优先）。
			_ = s.creds.Drop(req.MachineId)
			return nil, status.Errorf(codes.NotFound, "machine %s not found at agent", req.MachineId)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	// M4：删除成功 → 立即撤销验证材料，stale 流量 fail-closed。
	_ = s.creds.Drop(req.MachineId)
	if err := s.ledger.Put(req.OperationId, req.MachineId, hash, []byte(`{}`)); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	// machine 已删除：清掉同 machine 的历史记录（保留 delete 自身的去重记录），
	// 避免重放旧 create 时返回已删除 VM 的“成功”结果（M1 评审 P2-5）。
	// 清理失败不影响删除结果（残留记录会在年龄 GC 窗口内自然过期）。
	_, _ = s.ledger.PruneMachineExcept(req.MachineId, req.OperationId)
	// 高水位保留：更旧 generation 的 re-create 仍被拒绝（P0-2）。
	if err := s.fences.Advance(req.MachineId, req.Generation, req.ExecutionId); err != nil {
		return nil, status.Errorf(codes.Internal, "advance generation fence: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// admit 硬准入：allocated + inflight ≤ R·capacity（CPU R=4，内存 R=1.0，
// 与 scheduler 同一语义；容量未知时拒绝，保守不破坏硬上限承诺）。
// inflight 含本请求（P3-7 先加后查）；memTotal 已含 host 保留扣减（P3-8）。
func (s *Server) admit(req *pb.CreateMachineRequest) error {
	if req.GetSpec() == nil {
		return status.Error(codes.InvalidArgument, "spec is required")
	}
	vcpuTotal, memTotal, vcpuAllocated, memAllocated := s.info.AdmissionSnapshot()
	if vcpuTotal == 0 || memTotal == 0 {
		return status.Error(codes.ResourceExhausted, "node capacity unknown, admission denied")
	}
	inflVCPU := uint64(s.inflightVCPU.Load())
	inflMem := uint64(s.inflightMem.Load())
	if float64(vcpuAllocated+inflVCPU) > float64(vcpuTotal)*4 {
		return status.Errorf(codes.ResourceExhausted,
			"cpu admission: allocated %d + inflight %d exceeds 4x%d", vcpuAllocated, inflVCPU, vcpuTotal)
	}
	if memAllocated+inflMem > memTotal {
		return status.Errorf(codes.ResourceExhausted,
			"mem admission: allocated %dMiB + inflight %dMiB exceeds %dMiB", memAllocated, inflMem, memTotal)
	}
	return nil
}

// admitSize 返回请求的归一化资源需求（与 controller 默认值一致）。
func admitSize(req *pb.CreateMachineRequest) (vcpu, memMib uint64) {
	vcpu = req.GetSpec().GetVcpu()
	memMib = req.GetSpec().GetMemMib()
	if vcpu == 0 {
		vcpu = 1
	}
	if memMib == 0 {
		memMib = 512
	}
	return vcpu, memMib
}

// hashRequest 生成 request hash。只用于幂等比较，不持久化敏感字段。
// CreateMachineRequest 的 secret_env / proxy_credential 是单向下发的一次性
// 字段（ADR-0006/0010）：重派时控制面会重新解析引用值、凭证按需现算，
// 二者不参与幂等比较——首次执行的成功结果仍按 ledger 原样重放。
func hashRequest(msg proto.Message) string {
	if cr, ok := msg.(*pb.CreateMachineRequest); ok {
		cp := proto.Clone(cr).(*pb.CreateMachineRequest)
		cp.SecretEnv = nil
		cp.ProxyCredential = ""
		msg = cp
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("hash-error-%v", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// PauseMachine / ResumeMachine（M4.5 scale-to-zero，mvp-plan §8.4）：
// generation fencing + ledger 幂等，与其它变更 RPC 同一纪律。suspend 期间
// observed PAUSED 不摘路由？——由 controller 决策：proxy 端首个请求触发
// 同步 restore（autoresume），路由保留但请求有唤醒延迟。
func (s *Server) PauseMachine(ctx context.Context, req *pb.PauseMachineRequest) (*pb.Machine, error) {
	op := req.GetOperation()
	if op == nil || op.MachineId == "" || op.ExecutionId == "" || op.OperationId == "" {
		return nil, status.Error(codes.InvalidArgument, "operation with machine_id/execution_id/operation_id is required")
	}
	hash := hashRequest(req)
	if raw, ok, err := s.ledger.Check(op.OperationId, hash); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	} else if ok {
		var resp pb.Machine
		if err := protojson.Unmarshal(raw, &resp); err != nil {
			return nil, status.Errorf(codes.Internal, "replay ledger result: %v", err)
		}
		return &resp, nil
	}
	if err := s.fences.Check(op.MachineId, op.Generation); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	// execution 绑定校验（P3-18）：暂停/恢复必须针对当前 execution，旧代
	// 操作不得误停/误启新代实例（与 GetEndpoint/Delete 同一纪律；adapter 内
	// 实现 tags 比对，mismatch 返回错误）。
	m, err := s.machines.Pause(ctx, op.MachineId, op.ExecutionId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	raw, merr := protojson.Marshal(m)
	if merr != nil {
		return nil, status.Errorf(codes.Internal, "marshal pause result: %v", merr)
	}
	if err := s.ledger.Put(op.OperationId, op.MachineId, hash, raw); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	return m, nil
}

func (s *Server) ResumeMachine(ctx context.Context, req *pb.ResumeMachineRequest) (*pb.Machine, error) {
	op := req.GetOperation()
	if op == nil || op.MachineId == "" || op.ExecutionId == "" || op.OperationId == "" {
		return nil, status.Error(codes.InvalidArgument, "operation with machine_id/execution_id/operation_id is required")
	}
	hash := hashRequest(req)
	if raw, ok, err := s.ledger.Check(op.OperationId, hash); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	} else if ok {
		var resp pb.Machine
		if err := protojson.Unmarshal(raw, &resp); err != nil {
			return nil, status.Errorf(codes.Internal, "replay ledger result: %v", err)
		}
		return &resp, nil
	}
	if err := s.fences.Check(op.MachineId, op.Generation); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	// execution 绑定校验（P3-18）：同 Pause。
	m, err := s.machines.Resume(ctx, op.MachineId, op.ExecutionId)
	if err != nil {
		// 无快照等恢复失败：让上层决定 cold-start 重建。
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	raw, merr := protojson.Marshal(m)
	if merr != nil {
		return nil, status.Errorf(codes.Internal, "marshal resume result: %v", merr)
	}
	if err := s.ledger.Put(op.OperationId, op.MachineId, hash, raw); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	return m, nil
}
