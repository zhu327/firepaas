package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func TestIsNoCandidates(t *testing.T) {
	if !isNoCandidates(scheduler.ErrNoCandidates{Reason: "x"}) {
		t.Fatal("wrapped value must match")
	}
	if isNoCandidates(errors.New("boom")) {
		t.Fatal("generic error must not match")
	}
	if isNoCandidates(nil) {
		t.Fatal("nil must not match")
	}
}

func TestScaleUpSignalFor(t *testing.T) {
	op := store.Operation{ID: "op-1", ProjectID: "p", MachineID: "m-1"}
	req := &pb.CreateMachineRequest{
		Spec: &pb.MachineSpec{
			AppId: "app-1", Vcpu: 2, MemMib: 1024, DiskMib: 10240,
			Placement: &pb.PlacementConstraints{NodePool: "gpu"},
		},
	}
	sig := scaleUpSignalFor(op, req, scheduler.ErrNoCandidates{Reason: "all filters exhausted"})
	if sig.Pool != "gpu" || sig.VCPU != 2 || sig.MemMib != 1024 || sig.DiskMib != 10240 ||
		sig.AppID != "app-1" || sig.OperationID != "op-1" || sig.MachineID != "m-1" || sig.ProjectID != "p" {
		t.Fatalf("signal = %+v", sig)
	}
	// 空 pool → compute（与调度默认值一致）。
	req.Spec.Placement = nil
	if sig := scaleUpSignalFor(op, req, nil); sig.Pool != "compute" || sig.Reason == "" {
		t.Fatalf("default pool signal = %+v", sig)
	}
}

func TestScaleUpThrottle(t *testing.T) {
	var th scaleUpThrottle
	now := time.Now()
	if !th.allow("compute", now) {
		t.Fatal("first signal must pass")
	}
	if th.allow("compute", now.Add(time.Minute)) {
		t.Fatal("same pool within cooldown must be throttled")
	}
	if !th.allow("gpu", now.Add(time.Minute)) {
		t.Fatal("different pool must pass")
	}
	if !th.allow("compute", now.Add(scaleUpCooldown)) {
		t.Fatal("pool past cooldown must pass again")
	}
}
