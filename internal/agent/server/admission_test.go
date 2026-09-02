package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDiskAdmissionCountsCurrentRequestOnce(t *testing.T) {
	provider := info.New("node", "test", "test", "compute", "v1", "", t.TempDir(), nil, nil)
	total, _ := provider.DiskAdmissionSnapshot()
	if total < 2 {
		t.Skip("test filesystem too small")
	}
	provider.SetDiskAllocatedFunc(func() uint64 { return total - 1 })
	s := &Server{info: provider}
	s.inflightDisk.Store(1) // CreateMachine registers the current request before admit.
	req := &pb.CreateMachineRequest{Spec: &pb.MachineSpec{Vcpu: 1, MemMib: 1, DiskMib: 1}}
	if err := s.admit(req); err != nil {
		t.Fatalf("request fitting exact remaining disk must pass: %v", err)
	}
}

// TestCreateAdmissionFailClosedOnInvalidInventory：R2（契约 D-1）——资源采集
// 无效（inventory 故障且无 ≤60s last-known-good）时新 create 必须被拒
// （Unavailable），不得把“采集失败”当 0 占用放行 ghost 超售；非 create
// RPC（List/生命周期等）不经准入，不受影响。
func TestCreateAdmissionFailClosedOnInvalidInventory(t *testing.T) {
	dir := t.TempDir()
	ledger, err := state.Open(filepath.Join(dir, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	fences, err := state.OpenFences(filepath.Join(dir, "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	creds, err := state.OpenCreds(filepath.Join(dir, "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := machine.New(&fakeInstances{byName: map[string]*instances.Instance{}}, fakeImages{}, nil, nil)
	provider := info.New("node", "test", "test", "compute", "v1", "", dir, nil, nil)
	provider.SetResourcesValidFunc(func() bool { return false })
	srv := New(adapter, ledger, fences, provider, WithCreds(creds), WithCredentialRequired(true))

	if _, err := srv.CreateMachine(context.Background(), createReq("m-ghost", 1, "op-ghost")); status.Code(
		err,
	) != codes.Unavailable {
		t.Fatalf("create with invalid inventory: code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
	// 非 create RPC 不受影响。
	if _, err := srv.ListMachines(context.Background(), &pb.ListMachinesRequest{}); err != nil {
		t.Fatalf("list machines must not depend on admission snapshot: %v", err)
	}
}
