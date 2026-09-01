package machine

import (
	"context"
	"errors"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/tags"

	"github.com/zhu327/firepaas/internal/agent/network/slot"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

type snapshotSlotFake struct {
	attachErr error
	attached  int
	released  int
}

func (s *snapshotSlotFake) Attach(context.Context, string, string, string) (slot.Slot, error) {
	s.attached++
	return slot.Slot{}, s.attachErr
}
func (s *snapshotSlotFake) Release(context.Context, string) error { s.released++; return nil }
func (s *snapshotSlotFake) SlotFor(string) (slot.Slot, bool)      { return slot.Slot{}, false }

type restoreSnapshotFake struct {
	fakeInstances
	snapshots   []instances.Snapshot
	requests    []instances.RestoreSnapshotRequest
	forkRequest *instances.ForkSnapshotRequest
	restoreErr  error
	deleted     int
	stopped     int
}

func (f *restoreSnapshotFake) CreateSnapshot(context.Context, string, instances.CreateSnapshotRequest) (*instances.Snapshot, error) {
	return nil, instances.ErrNotSupported
}
func (f *restoreSnapshotFake) ListSnapshots(context.Context, *instances.ListSnapshotsFilter) ([]instances.Snapshot, error) {
	return f.snapshots, nil
}
func (f *restoreSnapshotFake) GetSnapshot(context.Context, string) (*instances.Snapshot, error) {
	return nil, instances.ErrNotFound
}
func (f *restoreSnapshotFake) DeleteSnapshot(context.Context, string) error { return nil }
func (f *restoreSnapshotFake) StopInstance(_ context.Context, _ string) (*instances.Instance, error) {
	f.stopped++
	if f.got == nil {
		return &instances.Instance{StoredMetadata: instances.StoredMetadata{Id: "internal"}, State: instances.StateStopped}, nil
	}
	f.got.State = instances.StateStopped
	return f.got, nil
}
func (f *restoreSnapshotFake) DeleteInstance(context.Context, string) error { f.deleted++; return nil }
func (f *restoreSnapshotFake) StartInstance(context.Context, string, instances.StartInstanceRequest) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}
func (f *restoreSnapshotFake) ForkSnapshot(_ context.Context, _ string, req instances.ForkSnapshotRequest) (*instances.Instance, error) {
	f.forkRequest = &req
	return &instances.Instance{StoredMetadata: instances.StoredMetadata{Id: "forked", Name: req.Name, Tags: req.Tags}, State: instances.StateRunning}, nil
}
func (f *restoreSnapshotFake) RestoreSnapshot(_ context.Context, _ string, _ string, req instances.RestoreSnapshotRequest) (*instances.Instance, error) {
	f.requests = append(f.requests, req)
	if f.restoreErr != nil {
		return nil, f.restoreErr
	}
	return &instances.Instance{StoredMetadata: instances.StoredMetadata{Id: "internal", Name: "m1", Tags: req.Tags}, State: instances.StateRunning}, nil
}

func TestForkSnapshotExplicitlyClearsInheritedVolumes(t *testing.T) {
	f := &restoreSnapshotFake{snapshots: []instances.Snapshot{{Id: "artifact", Tags: tags.Tags{tagSnapshotID: "snap"}}}}
	a := New(f, &fakeImages{}, nil, nil)
	_, err := a.ForkSnapshot(context.Background(), &pb.ForkSnapshotRequest{
		SnapshotId: "snap", MachineId: "fork", ExecutionId: "exec", Generation: 1,
		SpecJson: `{"projectId":"p","appId":"a"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.forkRequest == nil || !f.forkRequest.ClearVolumes {
		t.Fatalf("fork request did not clear inherited volume metadata: %+v", f.forkRequest)
	}
}

func TestForkSnapshotReattachesSlotAndCleansUpOnFailure(t *testing.T) {
	f := &restoreSnapshotFake{snapshots: []instances.Snapshot{{Id: "artifact"}}}
	slots := &snapshotSlotFake{attachErr: errors.New("attach failed")}
	a := New(f, &fakeImages{}, slots, nil)
	_, err := a.ForkSnapshot(context.Background(), &pb.ForkSnapshotRequest{
		SnapshotId: "snap", MachineId: "fork", ExecutionId: "exec", Generation: 1,
		SpecJson: `{"projectId":"p","appId":"a"}`,
	})
	if err == nil || slots.attached != 1 || slots.released != 1 || f.deleted != 1 {
		t.Fatalf("err=%v attached=%d released=%d deleted=%d", err, slots.attached, slots.released, f.deleted)
	}
}

func TestRestoreSnapshotReattachesSlotAndStopsReplacementOnFailure(t *testing.T) {
	f := &restoreSnapshotFake{
		fakeInstances: fakeInstances{got: &instances.Instance{StoredMetadata: instances.StoredMetadata{Id: "internal", Name: "m1", Tags: tags.Tags{tagExecution: "old"}}, State: instances.StateStopped}},
		snapshots:     []instances.Snapshot{{Id: "artifact", CompatibilityKey: "same"}},
	}
	slots := &snapshotSlotFake{attachErr: errors.New("attach failed")}
	a := New(f, &fakeImages{}, slots, nil)
	_, _, _, err := a.RestoreSnapshot(context.Background(), &pb.RestoreSnapshotRequest{
		SnapshotId: "snap", MachineId: "m1", ExecutionId: "new", Generation: 2,
		OperationId: "op", RestoreMode: "memory", CompatibilityKey: "same",
	})
	if err == nil || slots.attached != 1 || slots.released != 1 || f.stopped != 1 {
		t.Fatalf("err=%v attached=%d released=%d stopped=%d", err, slots.attached, slots.released, f.stopped)
	}
}

func TestRestoreSnapshotAutoFallsBackOnlyForCompatibilityAndReplacesIdentity(t *testing.T) {
	f := &restoreSnapshotFake{
		fakeInstances: fakeInstances{got: &instances.Instance{StoredMetadata: instances.StoredMetadata{Id: "internal", Name: "m1", Tags: tags.Tags{tagExecution: "old", tagGeneration: "3"}}, State: instances.StateRunning}},
		snapshots:     []instances.Snapshot{{Id: "artifact", CompatibilityKey: "source-key"}},
	}
	a := New(f, &fakeImages{}, nil, nil)
	m, mode, _, err := a.RestoreSnapshot(context.Background(), &pb.RestoreSnapshotRequest{
		SnapshotId: "snap", MachineId: "m1", ExecutionId: "new", Generation: 4,
		OperationId: "op", RestoreMode: "auto", CompatibilityKey: "target-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if mode != "filesystem" || len(f.requests) != 1 || !f.requests[0].FilesystemOnly {
		t.Fatalf("mode=%q requests=%+v", mode, f.requests)
	}
	if m.GetExecutionId() != "new" || m.GetGeneration() != 4 {
		t.Fatalf("restored identity = %q/%d", m.GetExecutionId(), m.GetGeneration())
	}
}

func TestRestoreSnapshotAutoDoesNotFallbackOnRuntimeError(t *testing.T) {
	f := &restoreSnapshotFake{
		fakeInstances: fakeInstances{got: &instances.Instance{StoredMetadata: instances.StoredMetadata{Id: "internal", Name: "m1", Tags: tags.Tags{tagExecution: "old"}}, State: instances.StateStopped}},
		snapshots:     []instances.Snapshot{{Id: "artifact", CompatibilityKey: "same"}},
		restoreErr:    errors.New("checksum corrupt"),
	}
	a := New(f, &fakeImages{}, nil, nil)
	_, _, _, err := a.RestoreSnapshot(context.Background(), &pb.RestoreSnapshotRequest{
		SnapshotId: "snap", MachineId: "m1", ExecutionId: "new", Generation: 2,
		OperationId: "op", RestoreMode: "auto", CompatibilityKey: "same",
	})
	if err == nil || len(f.requests) != 1 || f.requests[0].FilesystemOnly {
		t.Fatalf("err=%v requests=%+v", err, f.requests)
	}
}
