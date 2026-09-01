package server

import (
	"testing"

	"github.com/zhu327/firepaas/internal/agent/info"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
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
