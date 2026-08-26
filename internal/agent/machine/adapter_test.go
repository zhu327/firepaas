package machine

import (
	"context"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
)

type fakeInstances struct {
	created *instances.Instance
	listed  []instances.Instance
	deleted string
	err     error
}

func (f *fakeInstances) CreateInstance(_ context.Context, req instances.CreateInstanceRequest) (*instances.Instance, error) {
	if f.err != nil {
		return nil, f.err
	}
	inst := &instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id:          "internal-1",
			Name:        req.Name,
			Image:       req.Image,
			Size:        req.Size,
			OverlaySize: req.OverlaySize,
			Vcpus:       req.Vcpus,
			Env:         req.Env,
			Tags:        req.Tags,
			IP:          "10.100.0.2",
			CreatedAt:   time.Now(),
		},
		State: instances.StateInitializing,
	}
	f.created = inst
	return inst, nil
}

func (f *fakeInstances) ListInstances(_ context.Context, _ *instances.ListInstancesFilter) ([]instances.Instance, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.listed != nil {
		return f.listed, nil
	}
	if f.created != nil {
		return []instances.Instance{*f.created}, nil
	}
	return nil, nil
}

func (f *fakeInstances) GetInstance(_ context.Context, idOrName string) (*instances.Instance, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.created != nil && (f.created.Id == idOrName || f.created.Name == idOrName) {
		return f.created, nil
	}
	return nil, instances.ErrNotFound
}

func (f *fakeInstances) DeleteInstance(_ context.Context, id string) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = id
	return nil
}

type fakeImages struct{ err error }

func (f *fakeImages) CreateImage(context.Context, images.CreateImageRequest) (*images.Image, error) {
	return nil, f.err
}
func (f *fakeImages) WaitForReady(context.Context, string) error { return f.err }

func validCreateRequest() *pb.CreateMachineRequest {
	return &pb.CreateMachineRequest{
		MachineId:   "m1-test",
		Generation:  1,
		OperationId: "op-1",
		SecretEnv:   map[string]string{"SECRET_TOKEN": "s3cr3t"},
		Spec: &pb.MachineSpec{
			ProjectId:    "p1",
			AppId:        "a1",
			DeploymentId: "d1",
			ExecutionId:  "e1",
			ImageRef:     "docker.io/library/nginx:alpine",
			Vcpu:         1,
			MemMib:       512,
			Env:          map[string]string{"PORT": "8080"},
		},
	}
}

func TestAdapterCreateMapping(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{})
	m, err := a.Create(context.Background(), validCreateRequest())
	if err != nil {
		t.Fatal(err)
	}
	if m.MachineId != "m1-test" {
		t.Fatalf("machine_id = %s, want m1-test", m.MachineId)
	}
	if m.Spec.ExecutionId != "e1" || m.Readiness != pb.MachineReadiness_UNCONFIGURED {
		t.Fatalf("unexpected machine: %+v", m)
	}
	if got := im.created.Env["SECRET_TOKEN"]; got != "s3cr3t" {
		t.Fatalf("secret env not merged: %q", got)
	}
	if im.created.Name != "m1-test" || im.created.Vcpus != 1 || im.created.Size != 512*1024*1024 {
		t.Fatalf("unexpected hypeman request mapping: %+v", im.created)
	}
}

func TestAdapterListProjectFilter(t *testing.T) {
	im := &fakeInstances{listed: []instances.Instance{
		{StoredMetadata: instances.StoredMetadata{Id: "i1", Name: "m1", Image: "img", Tags: map[string]string{tagProject: "p1", tagExecution: "e1"}}, State: instances.StateRunning},
		{StoredMetadata: instances.StoredMetadata{Id: "i2", Name: "m2", Image: "img", Tags: map[string]string{tagProject: "p2", tagExecution: "e2"}}, State: instances.StateStopped},
	}}
	a := New(im, &fakeImages{})
	got, err := a.List(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MachineId != "m1" {
		t.Fatalf("unexpected filter result: %+v", got)
	}
}

func TestAdapterDeleteResolvesNameToInternalID(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{})
	if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(context.Background(), "m1-test"); err != nil {
		t.Fatal(err)
	}
	if im.deleted != "internal-1" {
		t.Fatalf("deleted %q, want internal-1", im.deleted)
	}
}
