// volumes.go implements v1.3 volume mutations with durable pre-effect claims
// and inventory reconciliation for crash/ACK-loss retries.
package server

import (
	"context"
	"errors"
	"time"

	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/mutation"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *Server) CreateVolume(ctx context.Context, req *pb.CreateVolumeRequest) (*pb.CreateVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and operation_id are required")
	}
	return s.createVolumeClaimed(
		ctx,
		req.GetOperationId(),
		req.GetVolumeId(),
		hashRequest(req),
		"volume.create",
		func() (*pb.CreateVolumeResponse, error) {
			if err := s.admitVolume(req.GetSizeBytes()); err != nil {
				return nil, err
			}
			return s.machines.CreateVolume(ctx, req.GetVolumeId(), req.GetSizeBytes())
		},
	)
}

func (s *Server) createVolumeClaimed(
	ctx context.Context,
	operationID, volumeID, hash, kind string,
	create func() (*pb.CreateVolumeResponse, error),
) (*pb.CreateVolumeResponse, error) {
	out, err := mutation.RunResourceMutation(s.mutations, mutation.ClaimedMutation[*pb.CreateVolumeResponse]{
		Identity: mutation.Identity{
			OperationID: operationID,
			MachineID:   volumeID,
			Kind:        kind,
			Coordinates: identityJSON(map[string]string{"volume_id": volumeID}),
			RequestHash: hash,
		},
		SerializationKey: volumeID,
		Recover: func() (mutation.Recovery[*pb.CreateVolumeResponse], error) {
			resp, found, err := s.machines.RecoverVolume(ctx, volumeID)
			return mutation.Recovery[*pb.CreateVolumeResponse]{Value: resp, Found: found}, err
		},
		Effect: create,
		Codec:  protoCodec(func() *pb.CreateVolumeResponse { return &pb.CreateVolumeResponse{} }),
	})
	if err != nil {
		return nil, mutationError(err)
	}
	return out, nil
}

func (s *Server) admitVolume(sizeBytes uint64) error {
	diskTotal, diskAllocated := s.info.DiskAdmissionSnapshot()
	wantMib := (sizeBytes + 1024*1024 - 1) / (1024 * 1024)
	if diskTotal == 0 {
		return status.Error(codes.Unavailable, "node disk capacity not available yet")
	}
	if diskAllocated+wantMib > diskTotal {
		return status.Errorf(
			codes.ResourceExhausted,
			"volume disk admission: allocated %dMiB + requested %dMiB exceeds %dMiB",
			diskAllocated,
			wantMib,
			diskTotal,
		)
	}
	if s.admissionDiskWatermark > 0 {
		if frac := diskStatsFraction(s.info.DataDir()); frac >= s.admissionDiskWatermark {
			return status.Errorf(
				codes.ResourceExhausted,
				"volume disk admission: watermark %.2f reached (%.2f)",
				s.admissionDiskWatermark,
				frac,
			)
		}
	}
	return nil
}

func (s *Server) ImportDataset(ctx context.Context, req *pb.ImportDatasetRequest) (*pb.CreateVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetOperationId() == "" || req.GetExpectedDigest() == "" ||
		req.GetExpiresAtUnix() <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"volume_id/operation_id/expected_digest/expires_at are required",
		)
	}
	return s.createVolumeClaimed(
		ctx,
		req.GetOperationId(),
		req.GetVolumeId(),
		hashRequest(req),
		"volume.import",
		func() (*pb.CreateVolumeResponse, error) {
			if req.GetExpiresAtUnix() < time.Now().Unix() ||
				req.GetExpiresAtUnix() > time.Now().Add(15*time.Minute).Unix() {
				return nil, status.Error(
					codes.PermissionDenied,
					"dataset import authorization expired or exceeds maximum TTL",
				)
			}
			if err := s.admitVolume(req.GetMaxExpandedBytes()); err != nil {
				return nil, err
			}
			return s.machines.ImportDataset(ctx, req)
		},
	)
}

// datasetOverlayEnabled is the agent enforcement truth source. It must only be
// enabled together with the advertised capability after genuine per-execution
// CoW support is available.
const datasetOverlayEnabled = false

func (s *Server) AttachVolume(ctx context.Context, req *pb.AttachVolumeRequest) (*pb.Machine, error) {
	if req.GetVolumeId() == "" || req.GetMachineId() == "" || req.GetExecutionId() == "" || req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id/machine_id/execution_id/operation_id are required")
	}
	if (req.GetOverlay() || req.GetOverlaySizeBytes() > 0) && !datasetOverlayEnabled {
		return nil, status.Error(codes.FailedPrecondition, "dataset overlay capability is disabled")
	}
	if req.GetOverlaySizeBytes() > 0 {
		if !req.GetOverlay() || !req.GetReadonly() {
			return nil, status.Error(codes.InvalidArgument, "dataset overlay requires overlay=true and readonly=true")
		}
		if err := s.admitVolume(req.GetOverlaySizeBytes()); err != nil {
			return nil, err
		}
	}
	return s.mutateAttachment(
		ctx,
		req.GetOperationId(),
		req.GetMachineId(),
		req.GetExecutionId(),
		req.GetVolumeId(),
		req.GetGeneration(),
		hashRequest(req),
		true,
		func() (*pb.Machine, error) { return s.machines.AttachVolume(ctx, req) },
	)
}

func (s *Server) DetachVolume(ctx context.Context, req *pb.DetachVolumeRequest) (*pb.Machine, error) {
	if req.GetVolumeId() == "" || req.GetMachineId() == "" || req.GetExecutionId() == "" || req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id/machine_id/execution_id/operation_id are required")
	}
	return s.mutateAttachment(
		ctx,
		req.GetOperationId(),
		req.GetMachineId(),
		req.GetExecutionId(),
		req.GetVolumeId(),
		req.GetGeneration(),
		hashRequest(req),
		false,
		func() (*pb.Machine, error) { return s.machines.DetachVolume(ctx, req) },
	)
}

func (s *Server) mutateAttachment(
	ctx context.Context,
	operationID, machineID, executionID, volumeID string,
	generation uint64,
	hash string,
	wantAttached bool,
	mutate func() (*pb.Machine, error),
) (*pb.Machine, error) {
	kind := "volume.detach"
	if wantAttached {
		kind = "volume.attach"
	}
	out, err := mutation.RunMachineMutation(s.mutations, mutation.ClaimedMutation[*pb.Machine]{
		Identity: mutation.Identity{
			OperationID: operationID,
			MachineID:   machineID,
			ExecutionID: executionID,
			Generation:  generation,
			Kind:        kind,
			Coordinates: identityJSON(map[string]string{"volume_id": volumeID}),
			RequestHash: hash,
		},
		Recover: func() (mutation.Recovery[*pb.Machine], error) {
			m, attached, err := s.machines.VolumeAttachmentState(ctx, machineID, executionID, volumeID)
			return mutation.Recovery[*pb.Machine]{Value: m, Found: attached == wantAttached}, err
		},
		Effect: mutate,
		Codec:  protoCodec(func() *pb.Machine { return &pb.Machine{} }),
	})
	if err != nil {
		return nil, mapVolumeMutationError(err)
	}
	return out, nil
}

func mapVolumeMutationError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, machine.ErrMachineNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, state.ErrStaleGeneration) || errors.Is(err, machine.ErrStaleExecution) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, mutation.ErrConflict) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Server) DeleteVolume(ctx context.Context, req *pb.DeleteVolumeRequest) (*emptypb.Empty, error) {
	if req.GetVolumeId() == "" || req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and operation_id are required")
	}
	err := mutation.RunResourceDelete(s.mutations, mutation.DeleteMutation{
		Identity: mutation.Identity{
			OperationID: req.GetOperationId(),
			MachineID:   req.GetVolumeId(),
			Kind:        "volume.delete",
			Coordinates: identityJSON(map[string]string{"volume_id": req.GetVolumeId()}),
			RequestHash: hashRequest(req),
		}, SerializationKey: req.GetVolumeId(),
		AlreadyDeleted: func() (bool, error) {
			_, found, err := s.machines.RecoverVolume(ctx, req.GetVolumeId())
			return !found, err
		},
		Effect: func() error { return s.machines.DeleteVolume(ctx, req.GetVolumeId()) },
	})
	if err != nil {
		return nil, mutationError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ListVolumes(ctx context.Context, _ *pb.ListVolumesRequest) (*pb.ListVolumesResponse, error) {
	list, err := s.machines.ListVolumes(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// v1.4-B：ListVolumes 无过滤语义，响应恒为节点全量 volume inventory。
	generation := s.inventoryGeneration.Add(1)
	observedAt := time.Now().Unix()
	return &pb.ListVolumesResponse{
		Volumes:  list,
		Complete: true, ObservationGeneration: generation, ObservedAtUnix: observedAt,
		Observation: &pb.InventoryObservation{
			Complete: true, Epoch: s.inventoryEpoch,
			Generation: generation, ObservedAtUnix: observedAt,
		},
	}, nil
}
