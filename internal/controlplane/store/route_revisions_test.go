package store

import (
	"context"
	"testing"
)

// D-2：route 发布 revision 由 route_publication_revisions 表在 SyncRoutes
// 同事务内分配，hostname 级单调递增；leader 换届（新进程，无任何内存态）
// 后仍从 PG 表继续分配，绝不回退——catalog 高水位守卫依赖这一性质。
func TestSyncRoutesAllocatesMonotoneRevisions(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-r2-routerev"
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM route_publication_revisions WHERE hostname IN ('routerev-a.test','routerev-b.test')`); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-routerev-a", "routerev-a.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-routerev-b", "routerev-b.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	routeA := RouteRow{
		Hostname: "routerev-a.test", Port: 80, AppID: "app-routerev-a", Generation: 1,
		Backends: []RouteBackendRow{{
			MachineID: "m1", ExecutionID: "e1", NodeProxyEndpoint: "10.0.0.1:5107", AppPort: 80,
			Weight: 100, Readiness: "READY",
		}},
	}

	// 首次发布：从 1 开始（PG 是新 leader 换届后唯一 revision 来源，不存在
	// 任何进程内存态——单调性完全由表保证）。
	revs, err := s.SyncRoutes(ctx, []RouteRow{routeA})
	if err != nil {
		t.Fatal(err)
	}
	if revs["routerev-a.test"] != 1 {
		t.Fatalf("first allocation = %d, want 1", revs["routerev-a.test"])
	}
	// 第二次“leader”发布（等价于新进程重放）：revision +1，无内容变化要求。
	revs, err = s.SyncRoutes(ctx, []RouteRow{routeA})
	if err != nil {
		t.Fatal(err)
	}
	if revs["routerev-a.test"] != 2 {
		t.Fatalf("replay allocation = %d, want 2", revs["routerev-a.test"])
	}
	// hostname 粒度独立：另一 hostname 首次发布也从 1 起。
	routeB := routeA
	routeB.Hostname = "routerev-b.test"
	routeB.AppID = "app-routerev-b"
	revs, err = s.SyncRoutes(ctx, []RouteRow{routeA, routeB})
	if err != nil {
		t.Fatal(err)
	}
	if revs["routerev-a.test"] != 3 || revs["routerev-b.test"] != 1 {
		t.Fatalf("per-hostname revisions = %v, want a=3 b=1", revs)
	}
	// 删后重建不回退：route 整体删除（空集合）再重建，revision 继续递增——
	// 这是 catalog 高水位拒绝“删后低 revision 乱序重建”的权威依据。
	if _, err := s.SyncRoutes(ctx, nil); err != nil {
		t.Fatal(err)
	}
	revs, err = s.SyncRoutes(ctx, []RouteRow{routeA})
	if err != nil {
		t.Fatal(err)
	}
	if revs["routerev-a.test"] != 4 {
		t.Fatalf("delete-then-recreate must continue, not regress: got %d, want 4", revs["routerev-a.test"])
	}
	// 表中记录保留已删除 hostname 的历史水位（不随 route 删除消失）。
	var rev int64
	if err := s.pool.QueryRow(ctx,
		`SELECT revision FROM route_publication_revisions WHERE hostname='routerev-b.test'`).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("high-water row for deleted hostname = %d, want 1", rev)
	}
}

// W3 §17：SyncRoutes 同事务持久化 mesh 直连提示（ULA/identity/generation），
// PG 成为已发布 backend set 的完整权威（审计/重建可见）。
func TestSyncRoutesPersistsMeshHints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := "test-r2-meshhints"
	cleanupProject(t, s, project)
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if err := s.EnsureProject(ctx, project, "t"); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureApp(ctx, project, "app-meshhints", "meshhints.test", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	route := RouteRow{
		Hostname: "meshhints.test", Port: 80, AppID: "app-meshhints", Generation: 1,
		Backends: []RouteBackendRow{{
			MachineID: "m1", ExecutionID: "e1", NodeProxyEndpoint: "10.0.0.1:5107", AppPort: 80,
			Weight: 100, Readiness: "READY",
			ULA: "fd7a:9a55:0:1::5", IdentityID: 42, Generation: 7,
		}, {
			MachineID: "m2", ExecutionID: "e2", NodeProxyEndpoint: "10.0.0.2:5107", AppPort: 80,
			Weight: 100, Readiness: "READY",
		}},
	}
	if _, err := s.SyncRoutes(ctx, []RouteRow{route}); err != nil {
		t.Fatal(err)
	}
	var ula string
	var iid uint32
	var gen int64
	if err := s.pool.QueryRow(ctx, `
		SELECT mesh_ula, mesh_identity_id, mesh_generation FROM route_backends
		WHERE machine_id='m1' AND execution_id='e1'`).Scan(&ula, &iid, &gen); err != nil {
		t.Fatal(err)
	}
	if ula != "fd7a:9a55:0:1::5" || iid != 42 || gen != 7 {
		t.Fatalf("mesh hints = (%q, %d, %d)", ula, iid, gen)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT mesh_ula, mesh_identity_id, mesh_generation FROM route_backends
		WHERE machine_id='m2' AND execution_id='e2'`).Scan(&ula, &iid, &gen); err != nil {
		t.Fatal(err)
	}
	if ula != "" || iid != 0 || gen != 0 {
		t.Fatalf("non-mesh backend hints must be zero: (%q, %d, %d)", ula, iid, gen)
	}
}
