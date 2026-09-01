package appcommand

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type fakeStore struct {
	app       *store.App
	active    *store.Deployment
	secretErr error
	deployErr error
	deployed  *store.Deployment
	rollout   *store.Rollout
}

func (f *fakeStore) GetApp(context.Context, string) (*store.App, error) { return f.app, nil }
func (f *fakeStore) ActiveDeploymentForApp(context.Context, string) (*store.Deployment, error) {
	return f.active, nil
}

func (f *fakeStore) GetSecretMeta(context.Context, string, string, *int64) (*store.SecretMeta, error) {
	if f.secretErr != nil {
		return nil, f.secretErr
	}
	return &store.SecretMeta{}, nil
}

func (f *fakeStore) DeployApp(_ context.Context, d store.Deployment, r store.Rollout, _ int64) error {
	f.deployed, f.rollout = &d, &r
	return f.deployErr
}

type fakeImages struct{ err error }

func (f fakeImages) Validate(image string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return "normalized:" + image, nil
}

func TestExecuteInheritsAndInitiatesDeployment(t *testing.T) {
	hc, _ := protojson.Marshal(&pb.HealthCheckSpec{Type: pb.HealthCheckSpec_HTTP, Target: "/ready"})
	auto, _ := protojson.Marshal(&pb.AutoStandbyPolicy{Enabled: false, IdleTimeoutSeconds: 60})
	egress, err := marshalEgress(&EgressPolicy{Mode: "allowlist", AllowedDomains: []string{"API.EXAMPLE.COM."}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	refs := map[string]store.SecretRef{"TOKEN": {Secret: "api-token"}}
	active := &store.Deployment{
		Generation: 4, ImageRef: "old", VCPU: 2, MemMIB: 1024, Port: 8080,
		Services: []store.ServiceSpec{{Name: "http", InternalPort: 8080}, {Name: "grpc", InternalPort: 9090}},
		Env: map[string]string{
			"MODE": "prod",
		}, SecretRefs: refs, Placement: json.RawMessage(`{"nodePool":"critical"}`),
		HealthCheck: hc, AutoStandby: auto, Strategy: "rolling", EgressPolicy: egress,
	}
	st := &fakeStore{app: &store.App{ID: "app-1", Generation: 4}, active: active}
	cmd := New(st, fakeImages{})
	cmd.newID = func() string { return "fixed" }

	got, err := cmd.Execute(
		context.Background(),
		Intent{AppID: "app-1", ProjectID: "dev", SecretRefs: refs, InheritAll: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.DeploymentID != "dep-app-1-5" || got.RolloutID != "rollout-fixed" || got.Status != "PREPARING" {
		t.Fatalf("result = %+v", got)
	}
	d := st.deployed
	if d.ImageRef != "old" || d.VCPU != 2 || d.MemMIB != 1024 || d.Port != 8080 || d.Strategy != "rolling" {
		t.Fatalf("scalars = %+v", d)
	}
	if !reflect.DeepEqual(d.Services, active.Services) || !reflect.DeepEqual(d.Env, active.Env) ||
		!reflect.DeepEqual(d.SecretRefs, refs) {
		t.Fatalf("collections = %+v", d)
	}
	if string(d.Placement) != string(active.Placement) || string(d.HealthCheck) != string(active.HealthCheck) ||
		string(d.AutoStandby) != string(active.AutoStandby) {
		t.Fatalf("policies = %+v", d)
	}
	if !reflect.DeepEqual(d.RequiredFeatures, []string{capabilities.SecretOneShotV1}) {
		t.Fatalf("features = %v", d.RequiredFeatures)
	}
	var policy pb.EgressPolicySpec
	if err := protojson.Unmarshal(d.EgressPolicy, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.GetPolicyGeneration() != 5 ||
		!reflect.DeepEqual(policy.GetAllowedDomains(), []string{"api.example.com"}) {
		t.Fatalf("egress = %s", d.EgressPolicy)
	}
}

func TestExecuteRegularDeployPreservesDefaults(t *testing.T) {
	active := &store.Deployment{
		Generation: 1, ImageRef: "old", VCPU: 2, MemMIB: 1024, Port: 8080,
		Env: map[string]string{"A": "1"}, Placement: json.RawMessage(`{"nodePool":"old"}`), Strategy: "rolling",
	}
	st := &fakeStore{app: &store.App{ID: "app-1", Generation: 1}, active: active}
	cmd := New(st, fakeImages{})
	cmd.newID = func() string { return "fixed" }
	if _, err := cmd.Execute(context.Background(), Intent{AppID: "app-1", ProjectID: "dev"}); err != nil {
		t.Fatal(err)
	}
	if st.deployed.Strategy != "bluegreen" || string(st.deployed.Placement) == string(active.Placement) {
		t.Fatalf("deployment = %+v", st.deployed)
	}
}

func TestExecuteMapsDomainErrors(t *testing.T) {
	cmd := New(&fakeStore{}, fakeImages{})
	if _, err := cmd.Execute(context.Background(), Intent{AppID: "missing"}); !errors.Is(err, ErrAppNotFound) {
		t.Fatalf("missing app: %v", err)
	}
	st := &fakeStore{app: &store.App{ID: "app"}}
	if _, err := New(st, fakeImages{}).Execute(context.Background(), Intent{AppID: "app"}); !errors.Is(
		err,
		ErrNoActiveDeployment,
	) {
		t.Fatalf("missing active: %v", err)
	}
	st.active = &store.Deployment{Generation: 1, ImageRef: "old"}
	st.app.Generation = 1
	st.deployErr = store.ErrRolloutBusy
	if _, err := New(st, fakeImages{}).Execute(context.Background(), Intent{AppID: "app"}); !errors.Is(
		err,
		ErrRolloutBusy,
	) {
		t.Fatalf("busy: %v", err)
	}
}
