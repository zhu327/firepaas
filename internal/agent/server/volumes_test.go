package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
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

func TestCreateVolumeClaimedMapsUnsafeDatasetArchiveToInvalidArgument(t *testing.T) {
	srv, _, _ := newTestServer(t)
	_, err := srv.createVolumeClaimed(
		context.Background(),
		"op-import",
		"vol-import",
		"hash",
		"volume.import",
		func() (*pb.CreateVolumeResponse, error) {
			return nil, fmt.Errorf("%w: invalid path", machine.ErrDatasetArchive)
		},
	)
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("unsafe dataset archive code = %s, want %s (err=%v)", code, codes.InvalidArgument, err)
	}
}

func TestAdmitVolumeUnknownCapacityIsRetryable(t *testing.T) {
	provider := info.New("node", "test", "test", "compute", "v1", "", t.TempDir()+"/not-created", nil, nil)
	srv := &Server{info: provider}
	if code := status.Code(srv.admitVolume(1 << 20)); code != codes.Unavailable {
		t.Fatalf("admitVolume unknown capacity code = %s, want %s", code, codes.Unavailable)
	}
}

// TestVolumeAdmissionSerializesConcurrentCreates：R2-8——并发 volume 创建
// （bytes 各超过半盘）在 inflight 预算核算下只有第一个能过准入；rpc 层
// （CreateVolume/ImportDataset/AttachOverlay）都在 admit 前 register 同一个
// inflight 计数。“检查→落地”窗口不再容许同时越过水位。
func TestVolumeAdmissionSerializesConcurrentCreates(t *testing.T) {
	provider := info.New("node", "test", "test", "compute", "v1", "", t.TempDir(), nil, nil)
	diskTotal, _ := provider.DiskAdmissionSnapshot()
	if diskTotal < 4 {
		t.Skip("test filesystem too small")
	}
	s := &Server{info: provider}
	want := (diskTotal/2 + 1) << 20

	admitted := make(chan struct{})
	holdRelease := make(chan struct{})
	firstDone := make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		defer close(firstDone)
		release := s.registerVolumeInflight(want)
		defer release()
		firstErr <- s.admitVolume(want)
		close(admitted)
		<-holdRelease
	}()
	<-admitted
	if err := <-firstErr; err != nil {
		t.Fatalf("first volume create must pass admission: %v", err)
	}
	release2 := s.registerVolumeInflight(want)
	if code := status.Code(s.admitVolume(want)); code != codes.ResourceExhausted {
		t.Fatalf("concurrent second create over-admitted: code = %s, want %s",
			code, codes.ResourceExhausted)
	}
	release2()
	// 首个请求落地（release）后预算重新可用。
	close(holdRelease)
	<-firstDone
	release3 := s.registerVolumeInflight(want)
	defer release3()
	if err := s.admitVolume(want); err != nil {
		t.Fatalf("create after inflight release must pass: %v", err)
	}
}
