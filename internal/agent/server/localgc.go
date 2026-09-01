package server

import (
	"context"
	"errors"

	"github.com/zhu327/firepaas/internal/agent/machine"
	contracts "github.com/zhu327/firepaas/internal/contracts/agentv1"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func localGCError(err error) error {
	if errors.Is(err, machine.ErrGuestOpsUnsupported) {
		return status.Error(codes.Unimplemented, err.Error())
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}
func (s *Server) QuarantineImage(ctx context.Context, r *pb.QuarantineImageRequest) (*pb.ImageQuarantine, error) {
	if e := contracts.ValidateQuarantineImageRequest(r); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	v, e := s.machines.QuarantineImage(ctx, r)
	if e != nil {
		return nil, localGCError(e)
	}
	return v, nil
}
func (s *Server) ListImageQuarantines(ctx context.Context, _ *pb.ListImageQuarantinesRequest) (*pb.ListImageQuarantinesResponse, error) {
	v, e := s.machines.ListImageQuarantines(ctx)
	if e != nil {
		return nil, localGCError(e)
	}
	return &pb.ListImageQuarantinesResponse{Quarantines: v}, nil
}
func (s *Server) RollbackImageQuarantine(ctx context.Context, r *pb.ImageQuarantineActionRequest) (*emptypb.Empty, error) {
	if e := contracts.ValidateImageQuarantineActionRequest(r); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if e := s.machines.RollbackImageQuarantine(ctx, r.GetClaimId(), r.GetToken()); e != nil {
		return nil, localGCError(e)
	}
	return &emptypb.Empty{}, nil
}
func (s *Server) FinalizeImageQuarantine(ctx context.Context, r *pb.ImageQuarantineActionRequest) (*emptypb.Empty, error) {
	if e := contracts.ValidateImageQuarantineActionRequest(r); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if e := s.machines.FinalizeImageQuarantine(ctx, r.GetClaimId(), r.GetToken()); e != nil {
		return nil, localGCError(e)
	}
	return &emptypb.Empty{}, nil
}
func (s *Server) QuarantineVolume(ctx context.Context, r *pb.QuarantineVolumeRequest) (*pb.VolumeQuarantine, error) {
	if e := contracts.ValidateQuarantineVolumeRequest(r); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	v, e := s.machines.QuarantineVolume(ctx, r)
	if e != nil {
		return nil, localGCError(e)
	}
	return v, nil
}
func (s *Server) ListVolumeQuarantines(ctx context.Context, _ *pb.ListVolumeQuarantinesRequest) (*pb.ListVolumeQuarantinesResponse, error) {
	v, e := s.machines.ListVolumeQuarantines(ctx)
	if e != nil {
		return nil, localGCError(e)
	}
	return &pb.ListVolumeQuarantinesResponse{Quarantines: v}, nil
}
func (s *Server) RollbackVolumeQuarantine(ctx context.Context, r *pb.VolumeQuarantineActionRequest) (*emptypb.Empty, error) {
	if e := contracts.ValidateVolumeQuarantineActionRequest(r); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if e := s.machines.RollbackVolumeQuarantine(ctx, r.GetClaimId(), r.GetToken()); e != nil {
		return nil, localGCError(e)
	}
	return &emptypb.Empty{}, nil
}
func (s *Server) FinalizeVolumeQuarantine(ctx context.Context, r *pb.VolumeQuarantineActionRequest) (*emptypb.Empty, error) {
	if e := contracts.ValidateVolumeQuarantineActionRequest(r); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if e := s.machines.FinalizeVolumeQuarantine(ctx, r.GetClaimId(), r.GetToken()); e != nil {
		return nil, localGCError(e)
	}
	return &emptypb.Empty{}, nil
}
