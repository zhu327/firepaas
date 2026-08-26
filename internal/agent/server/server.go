// Package server 是 agent 的 gRPC 服务实现（M1.4）。
// 依赖方向：server -> machine / image / info / state，反向禁止。
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/example/firepaas/internal/agent/info"
	"github.com/example/firepaas/internal/agent/machine"
	"github.com/example/firepaas/internal/agent/state"
	contracts "github.com/example/firepaas/internal/contracts/agentv1"
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
	info     *info.Provider
}

// New 构造 Server。
func New(machines *machine.Adapter, ledger *state.Ledger, info *info.Provider) *Server {
	return &Server{machines: machines, ledger: ledger, info: info}
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
		var resp pb.CreateMachineResponse
		if err := protojson.Unmarshal(raw, &resp); err != nil {
			return nil, status.Errorf(codes.Internal, "replay ledger result: %v", err)
		}
		return &resp, nil
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
	if err := s.ledger.Put(req.OperationId, hash, raw); err != nil {
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

	if err := s.machines.Delete(ctx, req.MachineId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.ledger.Put(req.OperationId, hash, []byte(`{}`)); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
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
