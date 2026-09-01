// snapshots_v14_test.go：v1.4-B agent 侧 inventory 观测元数据回归。
package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/kernel/hypeman/lib/volumes"
	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// inventoryFake 实现 snapshot/volume inventory 所需的最小 hypeman 能力。
type inventoryFake struct {
	fakeInstances
	snapshots []instances.Snapshot
	volumes   []volumes.Volume
}

func (f *inventoryFake) ListSnapshots(context.Context, *instances.ListSnapshotsFilter) ([]instances.Snapshot, error) {
	return f.snapshots, nil
}

func (f *inventoryFake) CreateSnapshot(
	context.Context,
	string,
	instances.CreateSnapshotRequest,
) (*instances.Snapshot, error) {
	return nil, instances.ErrNotSupported
}

func (f *inventoryFake) GetSnapshot(context.Context, string) (*instances.Snapshot, error) {
	return nil, instances.ErrNotFound
}

func (f *inventoryFake) DeleteSnapshot(context.Context, string) error { return nil }

func (f *inventoryFake) StopInstance(_ context.Context, _ string) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (f *inventoryFake) StartInstance(
	_ context.Context,
	_ string,
	_ instances.StartInstanceRequest,
) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (f *inventoryFake) ForkSnapshot(
	context.Context,
	string,
	instances.ForkSnapshotRequest,
) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (f *inventoryFake) RestoreSnapshot(
	context.Context,
	string,
	string,
	instances.RestoreSnapshotRequest,
) (*instances.Instance, error) {
	return nil, instances.ErrNotSupported
}

func (f *inventoryFake) ListVolumes(context.Context) ([]volumes.Volume, error) {
	return f.volumes, nil
}

func newInventoryServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	ledger, err := state.Open(filepath.Join(dir, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	fences, err := state.OpenFences(filepath.Join(dir, "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := machine.New(
		&inventoryFake{fakeInstances: fakeInstances{byName: map[string]*instances.Instance{}}},
		fakeImages{},
		nil,
		nil,
	)
	provider := info.New("node-inv", "test", "test", "compute", "v1.14.2", "10.100.0.0/16", dir, nil, nil)
	return New(adapter, ledger, fences, provider)
}

// v1.4-B：无过滤参数的 ListSnapshots 是全量 inventory（complete=true）；带
// 过滤是子集视图（complete=false）。
func TestListSnapshotsInventoryMetadata(t *testing.T) {
	srv := newInventoryServer(t)
	ctx := context.Background()

	full, err := srv.ListSnapshots(ctx, &pb.ListSnapshotsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !full.GetComplete() || full.GetObservedAtUnix() == 0 || full.GetObservationGeneration() == 0 {
		t.Fatalf("full inventory metadata missing: %+v", full)
	}
	filtered, err := srv.ListSnapshots(ctx, &pb.ListSnapshotsRequest{SnapshotId: "snap-1"})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.GetComplete() {
		t.Fatal("filtered listing must not claim completeness")
	}
	if filtered.GetObservationGeneration() <= full.GetObservationGeneration() {
		t.Fatal("observation generation must be monotonic")
	}
}
