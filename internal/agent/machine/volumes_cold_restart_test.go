package machine

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/tags"
	"github.com/kernel/hypeman/lib/volumes"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

type coldVolumeInstances struct {
	*fakeInstances
	calls      []string
	startFails int
}

func (f *coldVolumeInstances) StopInstance(_ context.Context, id string) (*instances.Instance, error) {
	f.calls = append(f.calls, "stop")
	if f.created == nil || f.created.Id != id {
		return nil, instances.ErrNotFound
	}
	f.created.State = instances.StateStopped
	return f.created, nil
}

func (f *coldVolumeInstances) StartInstance(
	_ context.Context,
	id string,
	_ instances.StartInstanceRequest,
) (*instances.Instance, error) {
	f.calls = append(f.calls, "start")
	if f.created == nil || f.created.Id != id {
		return nil, instances.ErrNotFound
	}
	if f.startFails > 0 {
		f.startFails--
		return nil, errors.New("start failed")
	}
	f.created.State = instances.StateRunning
	return f.created, nil
}

func (f *coldVolumeInstances) AttachVolume(
	_ context.Context,
	id, volumeID string,
	req instances.AttachVolumeRequest,
) (*instances.Instance, error) {
	f.calls = append(f.calls, "attach")
	if f.created.State != instances.StateStopped {
		return nil, instances.ErrInvalidState
	}
	f.created.Volumes = append(f.created.Volumes, instances.VolumeAttachment{
		VolumeID: volumeID, MountPath: req.MountPath, Readonly: req.Readonly,
		Overlay: req.Overlay, OverlaySize: req.OverlaySize,
	})
	return f.created, nil
}

func (f *coldVolumeInstances) DetachVolume(_ context.Context, id, volumeID string) (*instances.Instance, error) {
	f.calls = append(f.calls, "detach")
	if f.created.State != instances.StateStopped {
		return nil, instances.ErrInvalidState
	}
	next := f.created.Volumes[:0]
	for _, attachment := range f.created.Volumes {
		if attachment.VolumeID != volumeID {
			next = append(next, attachment)
		}
	}
	f.created.Volumes = next
	return f.created, nil
}

type coldVolumeProvider struct{ volume volumes.Volume }

func (p *coldVolumeProvider) CreateVolume(context.Context, volumes.CreateVolumeRequest) (*volumes.Volume, error) {
	return &p.volume, nil
}

func (p *coldVolumeProvider) CreateVolumeFromArchive(
	context.Context,
	volumes.CreateVolumeFromArchiveRequest,
	io.Reader,
) (*volumes.Volume, error) {
	return &p.volume, nil
}

func (p *coldVolumeProvider) GetVolume(context.Context, string) (*volumes.Volume, error) {
	return &p.volume, nil
}
func (p *coldVolumeProvider) DeleteVolume(context.Context, string) error { return nil }
func (p *coldVolumeProvider) ListVolumes(context.Context) ([]volumes.Volume, error) {
	return []volumes.Volume{p.volume}, nil
}
func (p *coldVolumeProvider) TotalVolumeBytes(context.Context) (int64, error) { return 0, nil }

func newColdVolumeAdapter(state instances.State) (*Adapter, *coldVolumeInstances) {
	inst := &instances.Instance{StoredMetadata: instances.StoredMetadata{
		Id: "internal-1", Name: "machine-1", Tags: tags.Tags{tagExecution: "execution-1"},
	}, State: state}
	manager := &coldVolumeInstances{fakeInstances: &fakeInstances{created: inst}}
	adapter := New(manager, &fakeImages{}, nil, nil)
	adapter.SetVolumes(&coldVolumeProvider{volume: volumes.Volume{Id: "volume-1", Tags: tags.Tags{}}})
	return adapter, manager
}

func TestAttachVolumeColdRestartsRunningMachine(t *testing.T) {
	adapter, manager := newColdVolumeAdapter(instances.StateRunning)
	machine, err := adapter.AttachVolume(context.Background(), &pb.AttachVolumeRequest{
		MachineId: "machine-1", ExecutionId: "execution-1", VolumeId: "volume-1", MountPath: "/data",
	})
	if err != nil {
		t.Fatal(err)
	}
	if machine.GetExecutionId() != "execution-1" || manager.created.State != instances.StateRunning {
		t.Fatalf("cold restart changed execution or final state: %#v", machine)
	}
	want := []string{"stop", "attach", "start"}
	if !slices.Equal(manager.calls, want) {
		t.Fatalf("calls = %v, want %v", manager.calls, want)
	}
}

func TestAttachVolumeStartFailureRestoresAttachmentAndRunningState(t *testing.T) {
	adapter, manager := newColdVolumeAdapter(instances.StateRunning)
	manager.startFails = 1
	_, err := adapter.AttachVolume(context.Background(), &pb.AttachVolumeRequest{
		MachineId: "machine-1", ExecutionId: "execution-1", VolumeId: "volume-1", MountPath: "/data",
	})
	if err == nil {
		t.Fatal("expected the failed restart to be reported")
	}
	if manager.created.State != instances.StateRunning || len(manager.created.Volumes) != 0 {
		t.Fatalf(
			"compensation did not restore source: state=%s volumes=%v",
			manager.created.State,
			manager.created.Volumes,
		)
	}
	want := []string{"stop", "attach", "start", "stop", "detach", "start"}
	if !slices.Equal(manager.calls, want) {
		t.Fatalf("calls = %v, want %v", manager.calls, want)
	}
}

func TestDetachVolumeStartFailureRestoresAttachmentAndRunningState(t *testing.T) {
	adapter, manager := newColdVolumeAdapter(instances.StateRunning)
	manager.created.Volumes = []instances.VolumeAttachment{{
		VolumeID: "volume-1", MountPath: "/dataset", Readonly: true, Overlay: true, OverlaySize: 4096,
	}}
	manager.startFails = 1
	_, err := adapter.DetachVolume(context.Background(), &pb.DetachVolumeRequest{
		MachineId: "machine-1", ExecutionId: "execution-1", VolumeId: "volume-1",
	})
	if err == nil {
		t.Fatal("expected the failed restart to be reported")
	}
	if manager.created.State != instances.StateRunning || len(manager.created.Volumes) != 1 {
		t.Fatalf(
			"compensation did not restore source: state=%s volumes=%v",
			manager.created.State,
			manager.created.Volumes,
		)
	}
	got := manager.created.Volumes[0]
	if got.MountPath != "/dataset" || !got.Readonly || !got.Overlay || got.OverlaySize != 4096 {
		t.Fatalf("restored attachment = %#v", got)
	}
	want := []string{"stop", "detach", "start", "stop", "attach", "start"}
	if !slices.Equal(manager.calls, want) {
		t.Fatalf("calls = %v, want %v", manager.calls, want)
	}
}
