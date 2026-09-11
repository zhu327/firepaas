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

// TestDiskAdmissionCrossPathVisibility（review 2026-09-10）：create overlay 与
// volume/import/overlay attach 的在途磁盘承诺必须共享同一硬上限。修复前两侧
// 各查各的 counter，并发的 create 与 volume create 可同时越过 diskTotal。
func TestDiskAdmissionCrossPathVisibility(t *testing.T) {
	provider := info.New("node", "test", "test", "compute", "v1", "", t.TempDir(), nil, nil)
	total, _ := provider.DiskAdmissionSnapshot()
	if total < 4 {
		t.Skip("test filesystem too small")
	}
	s := &Server{info: provider}

	// 场景 A：volume 在途 T-1，create 请求 2（inflightDisk 已含本请求）→ 拒。
	s.inflightVolumeDisk.Store(int64(total - 1))
	s.inflightDisk.Store(2)
	req := &pb.CreateMachineRequest{Spec: &pb.MachineSpec{Vcpu: 1, MemMib: 1, DiskMib: 2}}
	if code := status.Code(s.admit(req)); code != codes.ResourceExhausted {
		t.Fatalf("create must see in-flight volume disk: code = %s, want %s", code, codes.ResourceExhausted)
	}

	// 场景 B：create 在途 T-1，volume 请求 2（已 register）→ 拒。
	s.inflightDisk.Store(int64(total - 1))
	s.inflightVolumeDisk.Store(2)
	if code := status.Code(s.admitVolume(2 << 20)); code != codes.ResourceExhausted {
		t.Fatalf("volume must see in-flight create disk: code = %s, want %s", code, codes.ResourceExhausted)
	}

	// 清零后两条路径都恢复可准入。
	s.inflightDisk.Store(0)
	s.inflightVolumeDisk.Store(0)
	if err := s.admit(req); err != nil {
		t.Fatalf("create after inflight release must pass: %v", err)
	}
	s.inflightVolumeDisk.Store(1)
	if err := s.admitVolume(1 << 20); err != nil {
		t.Fatalf("volume after inflight release must pass: %v", err)
	}
}

// review L1：在途计数 double-release 变负时，uint64 回绕会放大而不是缩小
// 已承诺量，硬准入必须 fail closed（Unavailable），不得放行。
func TestDiskAdmissionCounterUnderflowFailsClosed(t *testing.T) {
	provider := info.New("node", "test", "test", "compute", "v1", "", t.TempDir(), nil, nil)
	s := &Server{info: provider}
	req := &pb.CreateMachineRequest{Spec: &pb.MachineSpec{Vcpu: 1, MemMib: 1, DiskMib: 1}}

	s.inflightDisk.Store(-1)
	if code := status.Code(s.admit(req)); code != codes.Unavailable {
		t.Fatalf("negative create inflight: code = %s, want %s", code, codes.Unavailable)
	}
	s.inflightDisk.Store(0)

	s.inflightVolumeDisk.Store(-1)
	if code := status.Code(s.admitVolume(1 << 20)); code != codes.Unavailable {
		t.Fatalf("negative volume inflight: code = %s, want %s", code, codes.Unavailable)
	}
}
