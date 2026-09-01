package server

import (
	"context"
	"errors"

	"github.com/zhu327/firepaas/internal/agent/machine"
	contracts "github.com/zhu327/firepaas/internal/contracts/agentv1"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ScrubSnapshot(ctx context.Context, req *pb.ScrubSnapshotRequest) (*pb.ScrubSnapshotResponse, error) {
	if err := contracts.ValidateScrubSnapshotRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	out, err := s.machines.ScrubSnapshot(ctx, req.GetSnapshotId(), req.GetExpectedRevision())
	if err == nil {
		return out, nil
	}
	switch {
	case errors.Is(err, machine.ErrSnapshotUnsupported):
		return nil, status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, machine.ErrSnapshotNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, machine.ErrSnapshotIncompatible):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	default:
		if machine.IsSnapshotCorrupt(err) {
			return nil, status.Error(codes.DataLoss, "snapshot content verification failed")
		}
		return nil, status.Error(codes.Internal, "snapshot scrub failed")
	}
}
