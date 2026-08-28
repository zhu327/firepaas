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
	"syscall"

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

// Server 聚合 InfoService / MachineService / ImageService。
type Server struct {
	pb.UnimplementedInfoServiceServer
	pb.UnimplementedMachineServiceServer
	pb.UnimplementedImageServiceServer

	machines *machine.Adapter
	ledger   *state.Ledger
	fences   *state.Fences
	info     *info.Provider
	creds    *state.Creds // M4：proxy credential 验证材料（仅摘要，ADR-0006）

	// M4：强制要求 create 携带 proxy credential（兼容开关，默认 true）。
	requireCredential bool

	// diskWatermark（v1.1，ADR-0018）：PullImage（部署预取）的磁盘水位守护，
	// 已用比例 ≥ 该值时拒绝预取（0 = 不启用；create 路径不受影响）。
	diskWatermark float64

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

// WithDiskWatermark 设置 PullImage（部署预取）的磁盘水位守护
// （已用比例 ≥ v 时拒绝预取；0 = 不启用）。
func WithDiskWatermark(v float64) Option { return func(s *Server) { s.diskWatermark = v } }

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

	// 与 Delete 共用 machine 临界区，避免新代 create 与旧代 delete 在
	// "读取实例→验证 execution→删除"窗口交错。
	var m *pb.Machine
	var createErr error
	err := s.fences.WithMachine(req.MachineId, func() error {
		if err := s.fences.Check(req.MachineId, req.Generation); err != nil {
			return err
		}
		m, createErr = s.machines.Create(ctx, req)
		if createErr != nil {
			return createErr
		}
		// 在释放 machine 锁之前推进高水位。否则旧代 Delete 可在新实例
		// 创建完成与 Advance 之间通过 fence，继而删除新 execution。
		return s.fences.Advance(req.MachineId, req.Generation, req.Spec.GetExecutionId())
	})
	if err != nil {
		if errors.Is(err, state.ErrStaleGeneration) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		// 镜像不可解析/拉取、解包超限和不安全的 secret 注入均为永久错误：
		// InvalidArgument 让控制面停止重试并避免降级泄露。
		if errors.Is(err, machine.ErrImageNotFound) || errors.Is(err, machine.ErrImageTooBig) ||
			errors.Is(err, machine.ErrSecretEnvInjectionUnsupported) {
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

	// 对同一 machine 串行化 fence 验证、execution 验证与删除。否则并发的
	// 新代 create 可在 Check 后替换实例，旧 execution 的 Delete 会误删新代。
	var deleteErr error
	if err := s.fences.WithMachine(req.MachineId, func() error {
		if err := s.fences.Check(req.MachineId, req.Generation); err != nil {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		if err := s.machines.Delete(ctx, req.MachineId, req.ExecutionId); err != nil {
			if errors.Is(err, instances.ErrNotFound) {
				_ = s.creds.Drop(req.MachineId)
				return status.Errorf(codes.NotFound, "machine %s not found at agent", req.MachineId)
			}
			return status.Error(codes.Internal, err.Error())
		}
		_ = s.creds.Drop(req.MachineId)
		if err := s.ledger.Put(req.OperationId, req.MachineId, hash, []byte(`{}`)); err != nil {
			return status.Error(codes.AlreadyExists, err.Error())
		}
		_, _ = s.ledger.PruneMachineExcept(req.MachineId, req.OperationId)
		if err := s.fences.Advance(req.MachineId, req.Generation, req.ExecutionId); err != nil {
			return status.Errorf(codes.Internal, "advance generation fence: %v", err)
		}
		return nil
	}); err != nil {
		deleteErr = err
	}
	if deleteErr != nil {
		return nil, deleteErr
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

// ---------------------------------------------------------------------------
// ImageService（v1.1，ADR-0018 部署预取）
// ---------------------------------------------------------------------------

// diskStatsFraction 返回 dataDir 所在文件系统已用比例（0-1）；不可用时返回 0。
func diskStatsFraction(dataDir string) float64 {
	total, used := diskStats(dataDir)
	if total == 0 {
		return 0
	}
	return float64(used) / float64(total)
}

// diskStats 复用 info 包的 statfs 逻辑（避免重复实现）。
func diskStats(dataDir string) (totalMib, usedMib uint64) {
	if dataDir == "" {
		dataDir = "/"
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &stat); err != nil {
		return 0, 0
	}
	bsize := uint64(stat.Bsize)
	total := stat.Blocks * bsize
	free := stat.Bfree * bsize
	return total / mib, (total - free) / mib
}

const mib = 1024 * 1024

// PullImage 确保镜像就绪（幂等）。v1.1 部署预取的 agent 侧入口：
// 控制面在 rollout PREPARING 前向 top-K 候选节点异步下发，失败/超时不
// 阻塞 rollout（尽力而为）。磁盘水位守护：已用比例 ≥ watermark 时拒绝
// （capacity-model 既有约束；imageretention GC 为兜底）。
func (s *Server) PullImage(ctx context.Context, req *pb.PullImageRequest) (*pb.PullImageResponse, error) {
	if req.GetImageRef() == "" {
		return nil, status.Error(codes.InvalidArgument, "image_ref is required")
	}
	if s.diskWatermark > 0 && s.info != nil {
		if frac := diskStatsFraction(s.info.DataDir()); frac >= s.diskWatermark {
			return nil, status.Errorf(codes.ResourceExhausted,
				"prefetch rejected: disk usage %.1f%% >= watermark %.1f%%", frac*100, s.diskWatermark*100)
		}
	}
	if err := s.machines.EnsureImage(ctx, req.GetImageRef()); err != nil {
		if errors.Is(err, machine.ErrImageNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, machine.ErrImageTooBig) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	digest, sizeMib := s.machines.ImageInfo(ctx, req.GetImageRef())
	return &pb.PullImageResponse{ImageRef: req.GetImageRef(), Digest: digest, SizeMib: sizeMib}, nil
}

// ListImages 返回节点本地镜像列表（ready 镜像）。
func (s *Server) ListImages(ctx context.Context, _ *pb.ListImagesRequest) (*pb.ListImagesResponse, error) {
	digests := s.machines.CachedImageDigests(ctx, 512)
	// CachedImageDigests 只含 digest 形态；ListImages 语义要求完整条目。
	// 用 adapter 的 ImageInfo 无法批量取，这里直接返回 digest 列表条目
	//（控制面当前只用 digest 集合，不为 Name 字段扩展契约）。
	out := make([]*pb.PullImageResponse, 0, len(digests))
	for _, d := range digests {
		out = append(out, &pb.PullImageResponse{Digest: d})
	}
	return &pb.ListImagesResponse{Images: out}, nil
}
