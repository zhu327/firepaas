package routepublisher

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

func TestDeriveRollingMultiportRoutesDeterministically(t *testing.T) {
	machines := []store.Machine{
		{ID: "old-1", AppID: "app", DeploymentID: "old", ReplicaOrdinal: 1, Hostname: "app.test", CurrentExecutionID: "exec-old-1", NodeID: "node-a", ObservedState: "RUNNING", ObservedReadiness: "READY"},
		{ID: "new-0", AppID: "app", DeploymentID: "new", ReplicaOrdinal: 0, Hostname: "app.test", CurrentExecutionID: "exec-new-0", NodeID: "node-b", ObservedState: "RUNNING", ObservedReadiness: "READY"},
		{ID: "old-0", AppID: "app", DeploymentID: "old", ReplicaOrdinal: 0, Hostname: "app.test", CurrentExecutionID: "exec-old-0", NodeID: "node-a", ObservedState: "PAUSED", ObservedReadiness: "UNCONFIGURED"},
		{ID: "new-1", AppID: "app", DeploymentID: "new", ReplicaOrdinal: 1, Hostname: "app.test", CurrentExecutionID: "exec-new-1", NodeID: "node-b", ObservedState: "RUNNING", ObservedReadiness: "NOT_READY"},
	}
	input := Input{
		Machines: machines,
		Deployments: []store.Deployment{
			{ID: "new", AppID: "app", Generation: 2, Strategy: "rolling", Services: []store.ServiceSpec{{Name: "http", InternalPort: 8080}, {Name: "admin", InternalPort: 9090}}},
			{ID: "old", AppID: "app", Generation: 1, Services: []store.ServiceSpec{{Name: "http", InternalPort: 8080}, {Name: "admin", InternalPort: 9090}}},
		},
		Rollouts:       []store.Rollout{{AppID: "app", FromGeneration: 1, ToGeneration: 2, Status: "PREPARING"}},
		ProxyByNode:    map[string]string{"node-a": "proxy-a", "node-b": "proxy-b"},
		DefaultAppPort: 8080,
	}

	first := Derive(input)
	input.Machines[0], input.Machines[3] = input.Machines[3], input.Machines[0]
	second := Derive(input)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("derivation depends on input order:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if len(first.Routes) != 2 || first.Routes[0].Port != 8080 || first.Routes[1].Port != 9090 {
		t.Fatalf("routes are not deterministically ordered: %+v", first.Routes)
	}
	for _, route := range first.Routes {
		if route.Generation != 2 || len(route.Backends) != 2 {
			t.Fatalf("unexpected route: %+v", route)
		}
		if route.Backends[0].MachineID != "new-0" || route.Backends[1].MachineID != "old-1" {
			t.Fatalf("only ready, non-draining rollout backends may be published: %+v", route.Backends)
		}
		for _, backend := range route.Backends {
			if !machineServing(store.Machine{ObservedState: "RUNNING", ObservedReadiness: backend.Readiness}) || backend.Draining {
				t.Fatalf("ineligible backend was published: %+v", backend)
			}
		}
	}
	if first.PrimaryPorts["app.test"] != 8080 {
		t.Fatalf("primary port = %d", first.PrimaryPorts["app.test"])
	}
}

func TestDeriveExcludesNotReadyExecutionFromPostgresProjection(t *testing.T) {
	projection := Derive(Input{
		Machines: []store.Machine{
			{ID: "ready", AppID: "app", DeploymentID: "dep", Hostname: "app.test", CurrentExecutionID: "exec-ready", NodeID: "node", ObservedState: "RUNNING", ObservedReadiness: "READY"},
			{ID: "not-ready", AppID: "app", DeploymentID: "dep", Hostname: "app.test", CurrentExecutionID: "exec-not-ready", NodeID: "node", ObservedState: "RUNNING", ObservedReadiness: "NOT_READY"},
		},
		Deployments: []store.Deployment{{ID: "dep", AppID: "app", Generation: 7, Port: 8080}},
		ProxyByNode: map[string]string{"node": "proxy"}, DefaultAppPort: 8080,
	})

	if len(projection.Routes) != 1 || projection.Routes[0].Generation != 7 {
		t.Fatalf("route generation changed while filtering readiness: %+v", projection.Routes)
	}
	backends := projection.Routes[0].Backends
	if len(backends) != 1 || backends[0].MachineID != "ready" {
		t.Fatalf("NOT_READY execution reached PostgreSQL projection: %+v", backends)
	}
}

func TestDeriveNotReadyTargetKeepsServingGeneration(t *testing.T) {
	projection := Derive(Input{
		Machines: []store.Machine{
			{ID: "old", AppID: "app", DeploymentID: "old-dep", ReplicaOrdinal: 0, Hostname: "app.test", CurrentExecutionID: "old-exec", NodeID: "node", ObservedState: "RUNNING", ObservedReadiness: "READY"},
			{ID: "new", AppID: "app", DeploymentID: "new-dep", ReplicaOrdinal: 0, Hostname: "app.test", CurrentExecutionID: "new-exec", NodeID: "node", ObservedState: "RUNNING", ObservedReadiness: "NOT_READY"},
		},
		Deployments: []store.Deployment{
			{ID: "old-dep", AppID: "app", Generation: 1, Port: 8080},
			{ID: "new-dep", AppID: "app", Generation: 2, Port: 8080, Strategy: "rolling"},
		},
		Rollouts:    []store.Rollout{{AppID: "app", FromGeneration: 1, ToGeneration: 2, Status: "PREPARING"}},
		ProxyByNode: map[string]string{"node": "proxy"}, DefaultAppPort: 8080,
	})

	if len(projection.Routes) != 1 || projection.Routes[0].Generation != 1 {
		t.Fatalf("NOT_READY target changed serving generation: %+v", projection.Routes)
	}
	backends := projection.Routes[0].Backends
	if len(backends) != 1 || backends[0].MachineID != "old" {
		t.Fatalf("NOT_READY target changed serving backend: %+v", backends)
	}
}

func TestDeriveUsesReadyTargetToCutButExcludesDrainingExecution(t *testing.T) {
	projection := Derive(Input{
		Machines: []store.Machine{
			{ID: "old", AppID: "app", DeploymentID: "old-dep", ReplicaOrdinal: 0, Hostname: "app.test", CurrentExecutionID: "old-exec", NodeID: "node", ObservedState: "RUNNING", ObservedReadiness: "READY"},
			{ID: "new", AppID: "app", DeploymentID: "new-dep", ReplicaOrdinal: 0, Hostname: "app.test", CurrentExecutionID: "new-exec", NodeID: "node", ObservedState: "RUNNING", ObservedReadiness: "READY"},
		},
		Deployments: []store.Deployment{
			{ID: "old-dep", AppID: "app", Generation: 1, Port: 8080},
			{ID: "new-dep", AppID: "app", Generation: 2, Port: 8080, Strategy: "rolling"},
		},
		Rollouts:    []store.Rollout{{AppID: "app", FromGeneration: 1, ToGeneration: 2, Status: "PREPARING"}},
		ProxyByNode: map[string]string{"node": "proxy"}, DefaultAppPort: 8080,
	})

	if len(projection.Routes) != 1 || projection.Routes[0].Generation != 2 {
		t.Fatalf("ready target did not advance route generation: %+v", projection.Routes)
	}
	backends := projection.Routes[0].Backends
	if len(backends) != 1 || backends[0].MachineID != "new" || backends[0].Draining {
		t.Fatalf("draining source execution reached PostgreSQL projection: %+v", backends)
	}
}

func TestPublisherExcludesIneligibleExecutionsFromPostgresAndRedis(t *testing.T) {
	calls := []string{}
	st := &fakeStore{
		machines: []store.Machine{
			{ID: "old-draining", AppID: "app", DeploymentID: "old", ReplicaOrdinal: 0, Hostname: "app.test", NodeID: "node", CurrentExecutionID: "exec-old", ObservedState: "RUNNING", ObservedReadiness: "READY"},
			{ID: "new-ready", AppID: "app", DeploymentID: "new", ReplicaOrdinal: 0, Hostname: "app.test", NodeID: "node", CurrentExecutionID: "exec-new", ObservedState: "RUNNING", ObservedReadiness: "READY"},
			{ID: "new-not-ready", AppID: "app", DeploymentID: "new", ReplicaOrdinal: 1, Hostname: "app.test", NodeID: "node", CurrentExecutionID: "exec-not-ready", ObservedState: "RUNNING", ObservedReadiness: "NOT_READY"},
		},
		rollouts: []store.Rollout{{AppID: "app", FromGeneration: 2, ToGeneration: 3, Status: "PREPARING"}},
		deployments: map[string][]store.Deployment{"app": {
			{ID: "old", AppID: "app", Generation: 2, Port: 8080},
			{ID: "new", AppID: "app", Generation: 3, Port: 8080, Strategy: "rolling"},
		}},
		calls: &calls,
	}
	cat := &fakeCatalog{calls: &calls}
	if err := New(st, cat, 8080, "").Rebuild(context.Background(), map[string]string{"node": "proxy"}); err != nil {
		t.Fatal(err)
	}
	if len(st.synced) != 1 || len(st.synced[0].Backends) != 1 || st.synced[0].Backends[0].MachineID != "new-ready" {
		t.Fatalf("PostgreSQL received ineligible backend: %+v", st.synced)
	}
	if len(cat.replaced) != 1 || len(cat.replaced[0].Route.Backends) != 1 || cat.replaced[0].Route.Backends[0].MachineID != "new-ready" {
		t.Fatalf("Redis received ineligible backend: %+v", cat.replaced)
	}
	if cat.replaced[0].Route.RouteGeneration != 3 {
		t.Fatalf("Redis route generation = %d, want 3", cat.replaced[0].Route.RouteGeneration)
	}
}

func TestPublisherStopsBeforeRedisWhenPostgresSyncFails(t *testing.T) {
	pgErr := errors.New("postgres unavailable")
	calls := []string{}
	st := &fakeStore{calls: &calls, syncErr: pgErr}
	cat := &fakeCatalog{calls: &calls}

	err := New(st, cat, 8080, "").Rebuild(context.Background(), nil)
	if !errors.Is(err, pgErr) {
		t.Fatalf("Rebuild error = %v, want PostgreSQL failure", err)
	}
	if !reflect.DeepEqual(calls, []string{"pg"}) {
		t.Fatalf("publication calls = %v, want PostgreSQL only", calls)
	}
	if cat.pruned {
		t.Fatal("Redis must remain untouched after PostgreSQL failure")
	}
}

func TestPublisherPersistsPostgresBeforeRedisAndStopsOnRedisFailure(t *testing.T) {
	redisErr := errors.New("redis unavailable")
	calls := []string{}
	st := &fakeStore{
		machines:    []store.Machine{{ID: "m", AppID: "app", DeploymentID: "dep", Hostname: "app.test", NodeID: "node", CurrentExecutionID: "exec", ObservedState: "RUNNING", ObservedReadiness: "READY"}},
		deployments: map[string][]store.Deployment{"app": {{ID: "dep", AppID: "app", Generation: 1, Port: 8080}}},
		calls:       &calls,
	}
	cat := &fakeCatalog{calls: &calls, replaceErr: redisErr}
	p := New(st, cat, 8080, "")

	err := p.Rebuild(context.Background(), map[string]string{"node": "proxy"})
	if !errors.Is(err, redisErr) {
		t.Fatalf("Rebuild error = %v, want Redis failure", err)
	}
	want := []string{"pg", "redis:app.test"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("publication order = %v, want %v", calls, want)
	}
	if len(st.synced) != 1 || st.synced[0].Hostname != "app.test" {
		t.Fatalf("Postgres route facts were not committed first: %+v", st.synced)
	}
	if cat.pruned {
		t.Fatal("prune must not run after host replacement fails")
	}
}

type fakeStore struct {
	machines    []store.Machine
	rollouts    []store.Rollout
	deployments map[string][]store.Deployment
	synced      []store.RouteRow
	calls       *[]string
	syncErr     error
}

func (f *fakeStore) ActiveRouteMachines(context.Context) ([]store.Machine, error) {
	return f.machines, nil
}
func (f *fakeStore) ListActiveRollouts(context.Context) ([]store.Rollout, error) {
	return f.rollouts, nil
}
func (f *fakeStore) ListDeployments(_ context.Context, appID string) ([]store.Deployment, error) {
	return f.deployments[appID], nil
}
func (f *fakeStore) SyncRoutes(_ context.Context, routes []store.RouteRow) error {
	*f.calls = append(*f.calls, "pg")
	f.synced = routes
	return f.syncErr
}

type fakeCatalog struct {
	calls      *[]string
	replaceErr error
	pruned     bool
	replaced   []catalog.HostRoute
}

func (f *fakeCatalog) ReplaceHostRoutes(_ context.Context, hostname string, routes []catalog.HostRoute, _ int) error {
	*f.calls = append(*f.calls, "redis:"+hostname)
	f.replaced = append(f.replaced, routes...)
	return f.replaceErr
}
func (f *fakeCatalog) PruneRoutes(context.Context, map[string]bool, map[string]bool) error {
	f.pruned = true
	*f.calls = append(*f.calls, "prune")
	return nil
}
