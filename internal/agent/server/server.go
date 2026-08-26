// Package server 是 agent 的 gRPC 服务实现（M1.4）。
// 依赖方向：server -> machine / image / info / state，反向禁止。
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/example/firepaas/internal/agent/info"
	"github.com/example/firepaas/internal/agent/machine"
	"github.com/example/firepaas/internal/agent/state"
	contracts "github.com/example/firepaas/internal/contracts/agentv1"
	"github.com/kernel/hypeman/lib/instances"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
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
}

// New 构造 Server。fences 提供 generation 高水位（P0-2）：
// 变更请求先查 ledger 幂等（重放返回原结果），再校验/推进 generation fence。
func New(machines *machine.Adapter, ledger *state.Ledger, fences *state.Fences, info *info.Provider) *Server {
	return &Server{machines: machines, ledger: ledger, fences: fences, info: info}
}

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

	m, err := s.machines.Create(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.CreateMachineResponse{Machine: m}

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
			return nil, status.Errorf(codes.NotFound, "machine %s not found at agent", req.MachineId)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
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

// hashRequest 生成 request hash。只用于幂等比较，不持久化敏感字段。
func hashRequest(msg proto.Message) string {
	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("hash-error-%v", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
