// snapshots.go implements the v1.3 snapshot mutation RPCs. Every mutation is
// durably claimed before side effects and reconciles an in-progress retry from
// stable hypeman inventory identity.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/mutation"
	"github.com/zhu327/firepaas/internal/agent/state"
	contracts "github.com/zhu327/firepaas/internal/contracts/agentv1"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func identityJSON(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}

func (s *Server) CreateSnapshot(
	ctx context.Context,
	req *pb.CreateSnapshotRequest,
) (*pb.CreateSnapshotResponse, error) {
	if err := contracts.ValidateCreateSnapshotRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	out, err := mutation.RunMachineMutation(s.mutations, mutation.ClaimedMutation[*pb.CreateSnapshotResponse]{
		Identity: mutation.Identity{
			OperationID: req.GetOperationId(),
			MachineID:   req.GetMachineId(),
			ExecutionID: req.GetExecutionId(),
			Generation:  req.GetGeneration(),
			Kind:        "snapshot.create",
			Coordinates: identityJSON(map[string]string{"snapshot_id": req.GetSnapshotId()}),
			RequestHash: hashRequest(req),
		},
		Recover: func() (mutation.Recovery[*pb.CreateSnapshotResponse], error) {
			info, found, err := s.machines.RecoverSnapshot(ctx, req)
			if err != nil {
				err = mapSnapshotError(err)
			}
			return mutation.Recovery[*pb.CreateSnapshotResponse]{
				Value: &pb.CreateSnapshotResponse{Snapshot: info},
				Found: found,
			}, err
		},
		Effect: func() (*pb.CreateSnapshotResponse, error) {
			info, err := s.machines.CreateSnapshot(ctx, req)
			if err != nil {
				err = mapSnapshotError(err)
			}
			return &pb.CreateSnapshotResponse{Snapshot: info}, err
		},
		Codec: protoCodec(func() *pb.CreateSnapshotResponse { return &pb.CreateSnapshotResponse{} }),
	})
	if err != nil {
		return nil, mapSnapshotError(err)
	}
	return out, nil
}

func mapSnapshotError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, machine.ErrMachineNotFound), errors.Is(err, machine.ErrSnapshotNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, machine.ErrSnapshotUnsupported), errors.Is(err, machine.ErrGuestOpsUnsupported):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, machine.ErrSecretSnapshotForbidden),
		errors.Is(err, machine.ErrSnapshotIncompatible),
		errors.Is(err, state.ErrStaleGeneration):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, mutation.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *Server) ListSnapshots(ctx context.Context, req *pb.ListSnapshotsRequest) (*pb.ListSnapshotsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	list, err := s.machines.ListSnapshots(ctx, req.GetMachineId(), req.GetSnapshotId())
	if err != nil {
		if errors.Is(err, machine.ErrGuestOpsUnsupported) {
			return nil, status.Error(codes.Unimplemented, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	complete := req.GetMachineId() == "" && req.GetSnapshotId() == ""
	generation := s.inventoryGeneration.Add(1)
	observedAt := time.Now().Unix()
	return &pb.ListSnapshotsResponse{
		Snapshots: list,
		// v1.4-B：无过滤参数 = 节点全量 inventory（控制面可推导 MISSING）。
		// 带过滤的查询是子集视图，不得声称完整。
		Complete: complete, ObservationGeneration: generation, ObservedAtUnix: observedAt,
		Observation: &pb.InventoryObservation{
			Complete: complete, Epoch: s.inventoryEpoch,
			Generation: generation, ObservedAtUnix: observedAt,
		},
	}, nil
}

func (s *Server) DeleteSnapshot(ctx context.Context, req *pb.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	if err := contracts.ValidateDeleteSnapshotRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err := mutation.RunMachineDelete(s.mutations, mutation.DeleteMutation{
		Identity: mutation.Identity{
			OperationID: req.GetOperationId(),
			MachineID:   req.GetMachineId(),
			ExecutionID: req.GetExecutionId(),
			Generation:  req.GetGeneration(),
			Kind:        "snapshot.delete",
			Coordinates: identityJSON(map[string]string{"snapshot_id": req.GetSnapshotId()}),
			RequestHash: hashRequest(req),
		},
		AlreadyDeleted: func() (bool, error) {
			list, err := s.machines.ListSnapshots(ctx, req.GetMachineId(), req.GetSnapshotId())
			return len(list) == 0, err
		},
		Effect: func() error { return s.machines.DeleteSnapshot(ctx, req.GetSnapshotId(), req.GetMachineId()) },
	})
	if err != nil {
		return nil, mapSnapshotError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ForkSnapshot(ctx context.Context, req *pb.ForkSnapshotRequest) (*pb.ForkSnapshotResponse, error) {
	if err := contracts.ValidateForkSnapshotRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.creds != nil && s.requireCredential && req.GetProxyCredential() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing proxy_credential for fork")
	}
	out, err := mutation.RunReplacement(s.mutations, mutation.Replacement[*pb.ForkSnapshotResponse]{
		Identity: mutation.Identity{
			OperationID: req.GetOperationId(),
			MachineID:   req.GetMachineId(),
			ExecutionID: req.GetExecutionId(),
			Generation:  req.GetGeneration(),
			Kind:        "snapshot.fork",
			Coordinates: identityJSON(map[string]string{"snapshot_id": req.GetSnapshotId()}),
			RequestHash: hashRequest(req),
		},
		Recover: func() (mutation.Recovery[*pb.ForkSnapshotResponse], error) {
			m, found, err := s.machines.RecoverMachine(
				ctx,
				req.GetMachineId(),
				req.GetExecutionId(),
				req.GetGeneration(),
			)
			return mutation.Recovery[*pb.ForkSnapshotResponse]{
				Value: &pb.ForkSnapshotResponse{Machine: m},
				Found: found,
			}, err
		},
		Effect: func() (*pb.ForkSnapshotResponse, error) {
			m, err := s.machines.ForkSnapshot(ctx, req)
			return &pb.ForkSnapshotResponse{Machine: m}, err
		},
		PersistCredential: func() error {
			if s.creds != nil && req.GetProxyCredential() != "" {
				return s.creds.Set(req.GetMachineId(), req.GetExecutionId(), state.Digest(req.GetProxyCredential()))
			}
			return nil
		},
		Codec: protoCodec(func() *pb.ForkSnapshotResponse { return &pb.ForkSnapshotResponse{} }),
	})
	if err != nil {
		return nil, mapSnapshotError(err)
	}
	return out, nil
}

func (s *Server) RestoreSnapshot(
	ctx context.Context,
	req *pb.RestoreSnapshotRequest,
) (*pb.RestoreSnapshotResponse, error) {
	if err := contracts.ValidateRestoreSnapshotRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.creds != nil && s.requireCredential && req.GetProxyCredential() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing proxy_credential for restore")
	}
	out, err := mutation.RunReplacement(s.mutations, mutation.Replacement[*pb.RestoreSnapshotResponse]{
		Identity: mutation.Identity{
			OperationID: req.GetOperationId(),
			MachineID:   req.GetMachineId(),
			ExecutionID: req.GetExecutionId(),
			Generation:  req.GetGeneration(),
			Kind:        "snapshot.restore",
			Coordinates: identityJSON(map[string]string{"snapshot_id": req.GetSnapshotId()}),
			RequestHash: hashRequest(req),
		},
		Recover: func() (mutation.Recovery[*pb.RestoreSnapshotResponse], error) {
			m, mode, consistency, found, err := s.machines.RecoverRestore(ctx, req)
			return mutation.Recovery[*pb.RestoreSnapshotResponse]{
				Value: &pb.RestoreSnapshotResponse{
					Machine:               m,
					RestoreModeUsed:       mode,
					FilesystemConsistency: consistency,
				},
				Found: found,
			}, err
		},
		Effect: func() (*pb.RestoreSnapshotResponse, error) {
			m, mode, consistency, err := s.machines.RestoreSnapshot(ctx, req)
			return &pb.RestoreSnapshotResponse{
				Machine:               m,
				RestoreModeUsed:       mode,
				FilesystemConsistency: consistency,
			}, err
		},
		PersistCredential: func() error {
			if s.creds != nil && req.GetProxyCredential() != "" {
				return s.creds.Set(req.GetMachineId(), req.GetExecutionId(), state.Digest(req.GetProxyCredential()))
			}
			return nil
		},
		Codec: protoCodec(func() *pb.RestoreSnapshotResponse { return &pb.RestoreSnapshotResponse{} }),
	})
	if err != nil {
		return nil, mapSnapshotError(err)
	}
	return out, nil
}
