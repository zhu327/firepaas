package routepublisher

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

func TestDeriveRollingMultiportRoutesDeterministically(t *testing.T) {
	machines := []store.Machine{
		{
			ID:                 "old-1",
			AppID:              "app",
			DeploymentID:       "old",
			ReplicaOrdinal:     1,
			Hostname:           "app.test",
			CurrentExecutionID: "exec-old-1",
			NodeID:             "node-a",
			ObservedState:      "RUNNING",
			ObservedReadiness:  "READY",
		},
		{
			ID:                 "new-0",
			AppID:              "app",
			DeploymentID:       "new",
			ReplicaOrdinal:     0,
			Hostname:           "app.test",
			CurrentExecutionID: "exec-new-0",
			NodeID:             "node-b",
			ObservedState:      "RUNNING",
			ObservedReadiness:  "READY",
		},
		{
			ID:                 "old-0",
			AppID:              "app",
			DeploymentID:       "old",
			ReplicaOrdinal:     0,
			Hostname:           "app.test",
			CurrentExecutionID: "exec-old-0",
			NodeID:             "node-a",
			ObservedState:      "PAUSED",
			ObservedReadiness:  "UNCONFIGURED",
		},
		{
			ID:                 "new-1",
			AppID:              "app",
			DeploymentID:       "new",
			ReplicaOrdinal:     1,
			Hostname:           "app.test",
			CurrentExecutionID: "exec-new-1",
			NodeID:             "node-b",
			ObservedState:      "RUNNING",
			ObservedReadiness:  "NOT_READY",
		},
	}
	input := Input{
		Machines: machines,
		Deployments: []store.Deployment{
			{
				ID:         "new",
				AppID:      "app",
				Generation: 2,
				Strategy:   "rolling",
				Services: []store.ServiceSpec{
					{Name: "http", InternalPort: 8080},
					{Name: "admin", InternalPort: 9090},
				},
			},
			{
				ID:         "old",
				AppID:      "app",
				Generation: 1,
				Services: []store.ServiceSpec{
					{Name: "http", InternalPort: 8080},
					{Name: "admin", InternalPort: 9090},
				},
			},
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
			if !machineServing(store.Machine{ObservedState: "RUNNING", ObservedReadiness: backend.Readiness}) ||
				backend.Draining {
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
			{
				ID:                 "ready",
				AppID:              "app",
				DeploymentID:       "dep",
				Hostname:           "app.test",
				CurrentExecutionID: "exec-ready",
				NodeID:             "node",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
			{
				ID:                 "not-ready",
				AppID:              "app",
				DeploymentID:       "dep",
				Hostname:           "app.test",
				CurrentExecutionID: "exec-not-ready",
				NodeID:             "node",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "NOT_READY",
			},
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
			{
				ID:                 "old",
				AppID:              "app",
				DeploymentID:       "old-dep",
				ReplicaOrdinal:     0,
				Hostname:           "app.test",
				CurrentExecutionID: "old-exec",
				NodeID:             "node",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
			{
				ID:                 "new",
				AppID:              "app",
				DeploymentID:       "new-dep",
				ReplicaOrdinal:     0,
				Hostname:           "app.test",
				CurrentExecutionID: "new-exec",
				NodeID:             "node",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "NOT_READY",
			},
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
			{
				ID:                 "old",
				AppID:              "app",
				DeploymentID:       "old-dep",
				ReplicaOrdinal:     0,
				Hostname:           "app.test",
				CurrentExecutionID: "old-exec",
				NodeID:             "node",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
			{
				ID:                 "new",
				AppID:              "app",
				DeploymentID:       "new-dep",
				ReplicaOrdinal:     0,
				Hostname:           "app.test",
				CurrentExecutionID: "new-exec",
				NodeID:             "node",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
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
			{
				ID:                 "old-draining",
				AppID:              "app",
				DeploymentID:       "old",
				ReplicaOrdinal:     0,
				Hostname:           "app.test",
				NodeID:             "node",
				CurrentExecutionID: "exec-old",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
			{
				ID:                 "new-ready",
				AppID:              "app",
				DeploymentID:       "new",
				ReplicaOrdinal:     0,
				Hostname:           "app.test",
				NodeID:             "node",
				CurrentExecutionID: "exec-new",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
			{
				ID:                 "new-not-ready",
				AppID:              "app",
				DeploymentID:       "new",
				ReplicaOrdinal:     1,
				Hostname:           "app.test",
				NodeID:             "node",
				CurrentExecutionID: "exec-not-ready",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "NOT_READY",
			},
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
	if len(cat.replaced) != 1 || len(cat.replaced[0].Route.Backends) != 1 ||
		cat.replaced[0].Route.Backends[0].MachineID != "new-ready" {
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
		machines: []store.Machine{
			{
				ID:                 "m",
				AppID:              "app",
				DeploymentID:       "dep",
				Hostname:           "app.test",
				NodeID:             "node",
				CurrentExecutionID: "exec",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
		},
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
	// identities（G2b）：FabricIdentities 返回值（nil = 空集）。
	identities []store.FabricIdentityRow
	synced     []store.RouteRow
	calls      *[]string
	syncErr    error
	// revs 是 SynnRoutes 返回的 hostname → revision（缺省按序递增分配）。
	revs map[string]int64
	mu   sync.Mutex
	seq  int64
}

func (f *fakeStore) ActiveRouteMachines(context.Context) ([]store.Machine, error) {
	return f.machines, nil
}

func (f *fakeStore) FabricIdentities(context.Context) ([]store.FabricIdentityRow, error) {
	return f.identities, nil
}

func (f *fakeStore) ListActiveRollouts(context.Context) ([]store.Rollout, error) {
	return f.rollouts, nil
}

func (f *fakeStore) ListDeployments(_ context.Context, appID string) ([]store.Deployment, error) {
	return f.deployments[appID], nil
}

func (f *fakeStore) SyncRoutes(_ context.Context, routes []store.RouteRow) (map[string]int64, error) {
	*f.calls = append(*f.calls, "pg")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced = routes
	if f.syncErr != nil {
		return nil, f.syncErr
	}
	if f.revs != nil {
		return f.revs, nil
	}
	revs := make(map[string]int64)
	for _, r := range routes {
		if _, ok := revs[r.Hostname]; !ok {
			f.seq++
			revs[r.Hostname] = f.seq
		}
	}
	return revs, nil
}

type fakeCatalog struct {
	calls      *[]string
	replaceErr error
	pruned     bool
	replaced   []catalog.HostRoute
	revisions  map[string]int64
	// dnsRecords（G2c）：ReplaceInternalDNS 收到的全量（nil 后无断言则空）。
	dnsRecords []catalog.InternalDNSRecord
	dnsTTL     time.Duration
}

func (f *fakeCatalog) ReplaceInternalDNS(
	_ context.Context,
	records []catalog.InternalDNSRecord,
	ttl time.Duration,
) error {
	if f.replaceErr != nil {
		return f.replaceErr
	}
	f.dnsRecords = records
	f.dnsTTL = ttl
	return nil
}

func (f *fakeCatalog) ReplaceHostRoutes(
	_ context.Context,
	hostname string,
	revision int64,
	routes []catalog.HostRoute,
	_ int,
) (bool, error) {
	*f.calls = append(*f.calls, "redis:"+hostname)
	if f.revisions == nil {
		f.revisions = map[string]int64{}
	}
	f.revisions[hostname] = revision
	f.replaced = append(f.replaced, routes...)
	return f.replaceErr == nil, f.replaceErr
}

func (f *fakeCatalog) PruneRoutes(context.Context, map[string]bool, map[string]bool) error {
	f.pruned = true
	*f.calls = append(*f.calls, "prune")
	return nil
}

// D-2：Rebuild 必须进程内串行——周期 sync 与显式 KickRouteRebuild 并发时
// 只有单一执行流（PG 分配 revision 与 Redis 高水位写入都不允许交错成
// 新 revision 先写、旧 revision 后到的乱序）。
func TestRebuildSerializedAcrossConcurrentCallers(t *testing.T) {
	calls := []string{}
	var inFlight, maxInFlight atomic.Int32
	st := &blockingStore{
		fakeStore: fakeStore{calls: &calls},
		enter: func() {
			n := inFlight.Add(1)
			for {
				old := maxInFlight.Load()
				if n <= old || maxInFlight.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond) // 拉长窗口：未串行化必然撞车
			inFlight.Add(-1)
		},
	}
	cat := &fakeCatalog{calls: &calls}
	p := New(st, cat, 8080, "")

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- p.Rebuild(context.Background(), nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := maxInFlight.Load(); got != 1 {
		t.Fatalf("rebuild must be serialized in-process, observed %d concurrent writers", got)
	}
}

type blockingStore struct {
	fakeStore
	enter func()
}

func (b *blockingStore) SyncRoutes(ctx context.Context, routes []store.RouteRow) (map[string]int64, error) {
	b.enter()
	return b.fakeStore.SyncRoutes(ctx, routes)
}

// D-2：publisher 把 SyncRoutes 同事务分配的 revision 透传给 catalog 发布。
func TestRebuildPassesAllocatedRevisionToCatalog(t *testing.T) {
	calls := []string{}
	st := &fakeStore{
		machines: []store.Machine{
			{
				ID:                 "m",
				AppID:              "app",
				DeploymentID:       "dep",
				Hostname:           "app.test",
				NodeID:             "node",
				CurrentExecutionID: "exec",
				ObservedState:      "RUNNING",
				ObservedReadiness:  "READY",
			},
		},
		deployments: map[string][]store.Deployment{"app": {{ID: "dep", AppID: "app", Generation: 1, Port: 8080}}},
		calls:       &calls,
		revs:        map[string]int64{"app.test": 42},
	}
	cat := &fakeCatalog{calls: &calls}
	if err := New(st, cat, 8080, "").Rebuild(context.Background(), map[string]string{"node": "proxy"}); err != nil {
		t.Fatal(err)
	}
	if cat.revisions["app.test"] != 42 {
		t.Fatalf("catalog revision = %d, want 42 (allocated by SyncRoutes)", cat.revisions["app.test"])
	}
}

// TestDeriveMeshDirectULAHints（G2b，ADR-0040 §16）：mesh_direct 服务的
// backend 携带在役 execution 的 ULA/identity/generation 提示；非直连服务、
// 无身份（未入 mesh）与旧行为一致为零值。
func TestDeriveMeshDirectULAHints(t *testing.T) {
	machines := []store.Machine{{
		ID: "m1", AppID: "app", DeploymentID: "dep", Hostname: "app.test",
		CurrentExecutionID: "exec-1", NodeID: "node-a",
		ObservedState: "RUNNING", ObservedReadiness: "READY",
	}}
	deployments := map[string][]store.Deployment{
		"app": {{
			ID: "dep", AppID: "app", Generation: 1,
			Services: []store.ServiceSpec{
				{Name: "direct", InternalPort: 8080, MeshDirect: true},
				{Name: "proxy-only", InternalPort: 9090},
			},
		}},
	}
	ula := netip.MustParseAddr("fd7a:9a55:0:1::7")
	identities := map[string]store.FabricIdentityRow{
		"m1\x00exec-1": {
			IdentityID: 42, ProjectID: "p", AppID: "app",
			Service: "direct", ULA: ula, MachineID: "m1", ExecutionID: "exec-1", Generation: 3,
		},
	}

	proj := Derive(Input{
		Machines: machines, Deployments: flattenDeployments(deployments),
		ProxyByNode:    map[string]string{"node-a": "10.0.0.1:5107"},
		DefaultAppPort: 8080, LegacyProxyAddr: "legacy:5107",
		FabricByIdentity: identities,
	})

	byPort := map[int]store.RouteRow{}
	for _, r := range proj.Routes {
		byPort[r.Port] = r
	}
	direct := byPort[8080]
	if len(direct.Backends) != 1 {
		t.Fatalf("direct backends = %+v", direct.Backends)
	}
	b := direct.Backends[0]
	if b.ULA != "fd7a:9a55:0:1::7" || b.IdentityID != 42 || b.Generation != 3 {
		t.Fatalf("mesh_direct hint missing: ula=%q id=%d gen=%d", b.ULA, b.IdentityID, b.Generation)
	}
	proxyOnly := byPort[9090]
	if len(proxyOnly.Backends) != 1 {
		t.Fatalf("proxy-only backends = %+v", proxyOnly.Backends)
	}
	b2 := proxyOnly.Backends[0]
	if b2.ULA != "" || b2.IdentityID != 0 || b2.Generation != 0 {
		t.Fatalf("non-mesh_direct service must not carry hints: %+v", b2)
	}

	// 无身份（未入 mesh / 分配未落）：mesh_direct 服务也无提示（零回归）。
	proj2 := Derive(Input{
		Machines: machines, Deployments: flattenDeployments(deployments),
		ProxyByNode:    map[string]string{"node-a": "10.0.0.1:5107"},
		DefaultAppPort: 8080, LegacyProxyAddr: "legacy:5107",
	})
	for _, r := range proj2.Routes {
		for _, b := range r.Backends {
			if b.ULA != "" || b.IdentityID != 0 || b.Generation != 0 {
				t.Fatalf("no-identity input must yield zero hints: %+v", b)
			}
		}
	}
}

func flattenDeployments(m map[string][]store.Deployment) []store.Deployment {
	var out []store.Deployment
	for _, deps := range m {
		out = append(out, deps...)
	}
	return out
}

// TestDeriveInternalDNSGating（G2c，ADR-0040 §16）：.internal 记录与 backend
// ULA 提示同源——非 READY / draining / 非 mesh_direct / 无身份的 execution
// 一律不发布 AAAA。
func TestDeriveInternalDNSGating(t *testing.T) {
	ula := func(s string) netip.Addr { return netip.MustParseAddr(s) }
	deployments := []store.Deployment{{
		ID: "dep", AppID: "app", Generation: 1,
		Services: []store.ServiceSpec{{Name: "svc", InternalPort: 8080, MeshDirect: true}},
	}}
	identities := map[string]store.FabricIdentityRow{
		"ready\x00e-ready": {
			IdentityID:  1,
			ProjectID:   "p",
			AppID:       "app",
			ULA:         ula("fd7a:9a55:0:1::1"),
			MachineID:   "ready",
			ExecutionID: "e-ready",
			Generation:  2,
		},
		"booting\x00e-boot": {
			IdentityID:  2,
			ProjectID:   "p",
			AppID:       "app",
			ULA:         ula("fd7a:9a55:0:1::2"),
			MachineID:   "booting",
			ExecutionID: "e-boot",
			Generation:  2,
		},
		"draining\x00e-dr": {
			IdentityID:  3,
			ProjectID:   "p",
			AppID:       "app",
			ULA:         ula("fd7a:9a55:0:1::3"),
			MachineID:   "draining",
			ExecutionID: "e-dr",
			Generation:  2,
		},
		"noident\x00e-noid": {
			IdentityID:  4,
			ProjectID:   "p",
			AppID:       "app",
			ULA:         ula("fd7a:9a55:0:1::4"),
			MachineID:   "noident",
			ExecutionID: "e-noid",
			Generation:  2,
		},
	}
	mk := func(id, exec, state, readiness string) store.Machine {
		return store.Machine{
			ID: id, AppID: "app", DeploymentID: "dep", Hostname: "app.test",
			CurrentExecutionID: exec, NodeID: "node-a", ObservedState: state, ObservedReadiness: readiness,
		}
	}
	machines := []store.Machine{
		mk("ready", "e-ready", "RUNNING", "READY"),
		mk("booting", "e-boot", "RUNNING", "UNCONFIGURED"), // 未 READY
		mk("stopped", "e-stop", "STOPPED", "READY"),        // 非 serving
	}
	proj := Derive(Input{
		Machines: machines, Deployments: deployments,
		ProxyByNode:    map[string]string{"node-a": "p:5107"},
		DefaultAppPort: 8080, LegacyProxyAddr: "legacy:5107",
		FabricByIdentity: identities,
	})
	if len(proj.InternalDNS) != 1 {
		t.Fatalf("internal dns = %+v, want single record", proj.InternalDNS)
	}
	rec := proj.InternalDNS[0]
	if rec.Name != "app.p.internal" || len(rec.AAAA) != 1 || rec.AAAA[0] != "fd7a:9a55:0:1::1" || rec.Generation != 2 {
		t.Fatalf("record = %+v", rec)
	}

	// 非 mesh_direct 服务（同 app）→ 无记录。
	depsNoDirect := []store.Deployment{{
		ID: "dep", AppID: "app", Generation: 1,
		Services: []store.ServiceSpec{{Name: "svc", InternalPort: 8080}},
	}}
	proj2 := Derive(Input{
		Machines:       []store.Machine{mk("ready", "e-ready", "RUNNING", "READY")},
		Deployments:    depsNoDirect,
		ProxyByNode:    map[string]string{"node-a": "p:5107"},
		DefaultAppPort: 8080, LegacyProxyAddr: "legacy:5107",
		FabricByIdentity: identities,
	})
	if len(proj2.InternalDNS) != 0 {
		t.Fatalf("non-mesh_direct must yield no dns records: %+v", proj2.InternalDNS)
	}

	// 多副本：去重聚合（两个 READY execution 同 app → 2 AAAA）。
	identities["ready2\x00e-ready2"] = store.FabricIdentityRow{
		IdentityID: 5, ProjectID: "p",
		AppID: "app", ULA: ula("fd7a:9a55:0:1::9"), MachineID: "ready2", ExecutionID: "e-ready2", Generation: 4,
	}
	proj3 := Derive(Input{
		Machines: []store.Machine{
			mk("ready", "e-ready", "RUNNING", "READY"),
			mk("ready2", "e-ready2", "RUNNING", "READY"),
		},
		Deployments:    deployments,
		ProxyByNode:    map[string]string{"node-a": "p:5107"},
		DefaultAppPort: 8080, LegacyProxyAddr: "legacy:5107",
		FabricByIdentity: identities,
	})
	if len(proj3.InternalDNS) != 1 || len(proj3.InternalDNS[0].AAAA) != 2 ||
		proj3.InternalDNS[0].AAAA[0] != "fd7a:9a55:0:1::1" || proj3.InternalDNS[0].AAAA[1] != "fd7a:9a55:0:1::9" ||
		proj3.InternalDNS[0].Generation != 4 {
		t.Fatalf("aggregate record = %+v", proj3.InternalDNS)
	}
}

// TestRebuildPublishesBackendHints（G2b 回归）：Derive 产出的 ULA 提示必须
// 经 publishRedis 落进 catalog.Backend（曾因转换层漏字段而丢失——真机
// G2d 验收抓到：DNS 记录有 ULA 而 route backend 没有）。
func TestRebuildPublishesBackendHints(t *testing.T) {
	ula := netip.MustParseAddr("fd7a:9a55:0:1::7")
	st := &fakeStore{
		machines: []store.Machine{{
			ID: "m1", AppID: "app", DeploymentID: "dep", Hostname: "app.test",
			CurrentExecutionID: "e1", NodeID: "node-a",
			ObservedState: "RUNNING", ObservedReadiness: "READY",
		}},
		deployments: map[string][]store.Deployment{
			"app": {{
				ID: "dep", AppID: "app", Generation: 1,
				Services: []store.ServiceSpec{{Name: "svc", InternalPort: 8080, MeshDirect: true}},
			}},
		},
		identities: []store.FabricIdentityRow{{
			IdentityID: 42, ProjectID: "p", AppID: "app", Service: "svc",
			ULA: ula, MachineID: "m1", ExecutionID: "e1", Generation: 5,
		}},
	}
	calls := []string{}
	st.calls = &calls
	cat := &fakeCatalog{calls: &calls}
	p := New(st, cat, 8080, "legacy:5107")
	if err := p.Rebuild(context.Background(), map[string]string{"node-a": "10.0.0.1:5107"}); err != nil {
		t.Fatal(err)
	}
	if len(cat.replaced) != 1 || len(cat.replaced[0].Route.Backends) != 1 {
		t.Fatalf("replaced = %+v", cat.replaced)
	}
	b := cat.replaced[0].Route.Backends[0]
	if b.ULA != "fd7a:9a55:0:1::7" || b.IdentityID != 42 || b.Generation != 5 {
		t.Fatalf("published backend hint missing: %+v", b)
	}
}

// TestDNSStaleWindowDefaultsMatchEdge（P1 独立评审）：dns:internal 投影 TTL
// 默认必须与 edge FIREPAAS_EDGE_STALE_WINDOW 默认同源（ADR-0040 §16，均为
// 120s）；运维侧两端 env（FIREPAAS_DNS_STALE_WINDOW / FIREPAAS_EDGE_STALE_WINDOW）
// 分别接线，默认值分叉即本测试失败。
func TestDNSStaleWindowDefaultsMatchEdge(t *testing.T) {
	const edgeStaleDefault = 120 * time.Second // cmd/edge-proxy staleDefault 同值
	p := New(nil, nil, 8080, "")
	if p.dnsStaleWindow != edgeStaleDefault {
		t.Fatalf("publisher default TTL = %v, want edge default %v", p.dnsStaleWindow, edgeStaleDefault)
	}
	p.SetDNSStaleWindow(0)
	if p.dnsStaleWindow != edgeStaleDefault {
		t.Fatalf("SetDNSStaleWindow(0) must restore default, got %v", p.dnsStaleWindow)
	}
	p.SetDNSStaleWindow(60 * time.Second)
	if p.dnsStaleWindow != 60*time.Second {
		t.Fatalf("SetDNSStaleWindow(60s) = %v", p.dnsStaleWindow)
	}
}
