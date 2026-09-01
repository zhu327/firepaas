package server

import (
	"context"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/info"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAttachVolumeRejectsDisabledOverlayAtRPCBoundary(t *testing.T) {
	srv, _, _ := newTestServer(t)
	_, err := srv.AttachVolume(context.Background(), &pb.AttachVolumeRequest{
		VolumeId: "v", MachineId: "m", ExecutionId: "e", OperationId: "op",
		Readonly: true, Overlay: true,
	})
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("overlay attach code = %s, want %s (err=%v)", code, codes.FailedPrecondition, err)
	}
}

func TestAdmitVolumeUnknownCapacityIsRetryable(t *testing.T) {
	provider := info.New("node", "test", "test", "compute", "v1", "", t.TempDir()+"/not-created", nil, nil)
	srv := &Server{info: provider}
	if code := status.Code(srv.admitVolume(1 << 20)); code != codes.Unavailable {
		t.Fatalf("admitVolume unknown capacity code = %s, want %s", code, codes.Unavailable)
	}
}
