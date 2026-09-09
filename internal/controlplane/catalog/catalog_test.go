package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS=127.0.0.1:6379 to run catalog tests")
	}
	// 独立 DB：并行包测试（store/reservations 也用同一 Redis）会互相
	// SCAN/DEL 投影键——DB 1 隔离 catalog 测试的键空间。
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 1})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func cleanHost(t *testing.T, c *Catalog, hostname string) {
	t.Helper()
	ctx := context.Background()
	// 连 routerev 高水位一起清——本键特意不随投影删除（见 ReplaceHostRoutes
	// 删除路径注释），测试隔离必须显式处理。
	_ = c.rdb.Del(ctx, hostIndexKey(hostname), routeRevisionKey(hostname)).Err()
	iter := c.rdb.Scan(ctx, 0, "route:"+hostname+":*", 100).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if len(keys) > 0 {
		_ = c.rdb.Del(ctx, keys...).Err()
	}
}

// v1.1（ADR-0022）：hostidx 端口集合——多 service 发布后按端口可查，
// 未声明端口权威 miss；主端口（首元素）兼容 GetRouteForHostname。
func TestMultiportHostIndex(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "multi.test")

	// 一次原子发布完整 service 集：主 80 + 附加 8081。
	applied, err := c.ReplaceHostRoutes(ctx, "multi.test", 1, []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "m1", AppPort: 80}}}},
		{Port: 8081, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "m1", AppPort: 8081}}}},
	}, 80)
	if err != nil || !applied {
		t.Fatalf("replace: applied=%v err=%v", applied, err)
	}

	ports, err := c.HostPorts(ctx, "multi.test")
	if err != nil || len(ports) != 2 || ports[0] != 80 || ports[1] != 8081 {
		t.Fatalf("host ports = %v %v", ports, err)
	}

	// 主端口查询（M1 兼容）。
	route, err := c.GetRouteForHostname(ctx, "multi.test")
	if err != nil || route == nil || route.Backends[0].AppPort != 80 {
		t.Fatalf("primary route: %+v %v", route, err)
	}

	// 按端口查询：声明端口命中，backend AppPort = internal_port。
	route, declared, err := c.GetRouteForPort(ctx, "multi.test", 8081)
	if err != nil || !declared || route == nil || route.Backends[0].AppPort != 8081 {
		t.Fatalf("port 8081: route=%+v declared=%v err=%v", route, declared, err)
	}

	// 未声明端口：权威 miss（declared=false）。
	route, declared, err = c.GetRouteForPort(ctx, "multi.test", 9999)
	if err != nil || declared || route != nil {
		t.Fatalf("undeclared port: route=%+v declared=%v err=%v", route, declared, err)
	}

	// 旧单端口值向后兼容读。
	if err := c.rdb.Set(ctx, hostIndexKey("legacy.test"), "8080", 0).Err(); err != nil {
		t.Fatal(err)
	}
	ports, err = c.HostPorts(ctx, "legacy.test")
	if err != nil || len(ports) != 1 || ports[0] != 8080 {
		t.Fatalf("legacy hostidx: %v %v", ports, err)
	}
	cleanHost(t, c, "legacy.test")
}

// ReplaceHostRoutes 的并发调用各自携带完整集合，不能因为 hostidx 的 RMW
// 竞争而丢失附加端口；最终集合须仍精确包含全部声明端口。
func TestReplaceHostRoutesConcurrentCompleteSet(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "concurrent.test")
	t.Cleanup(func() { cleanHost(t, c, "concurrent.test") })

	routes := []HostRoute{{Port: 80}, {Port: 8081}, {Port: 9090}}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		// 每次并发发布携带递增 revision（与 publisher 行为一致：同 revision
		// 的并行重放会被高水位守卫拒绝，只剩单次生效——那是 D-2 的特性）。
		go func(rev int64) {
			defer wg.Done()
			_, err := c.ReplaceHostRoutes(ctx, "concurrent.test", rev, routes, 80)
			errs <- err
		}(int64(i + 1))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	ports, err := c.HostPorts(ctx, "concurrent.test")
	if err != nil || len(ports) != 3 || ports[0] != 80 {
		t.Fatalf("concurrent host ports = %v, %v", ports, err)
	}
	for _, port := range []int{80, 8081, 9090} {
		if _, declared, err := c.GetRouteForPort(ctx, "concurrent.test", port); err != nil || !declared {
			t.Fatalf("port %d declared=%v err=%v", port, declared, err)
		}
	}
}

// ReplaceHostRoutes 以完整集合替换，而非读-改-写：service 删除必须同时从
// hostidx 和 route 投影消失，避免 edge 把已删除端口当作声明端口。
func TestReplaceHostRoutesRemovesDeletedService(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "replace.test")
	t.Cleanup(func() { cleanHost(t, c, "replace.test") })

	if _, err := c.ReplaceHostRoutes(ctx, "replace.test", 1, []HostRoute{{Port: 80}, {Port: 8081}}, 80); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReplaceHostRoutes(ctx, "replace.test", 2, []HostRoute{{Port: 80}}, 80); err != nil {
		t.Fatal(err)
	}
	ports, err := c.HostPorts(ctx, "replace.test")
	if err != nil || len(ports) != 1 || ports[0] != 80 {
		t.Fatalf("host ports after removal = %v, %v", ports, err)
	}
	if route, declared, err := c.GetRouteForPort(ctx, "replace.test", 8081); err != nil || declared || route != nil {
		t.Fatalf("removed port: route=%+v declared=%v err=%v", route, declared, err)
	}
	if exists, err := c.rdb.Exists(ctx, routeKey("replace.test", 8081)).Result(); err != nil || exists != 0 {
		t.Fatalf("removed route exists=%d err=%v", exists, err)
	}
}

// PruneRoutes 语义回归：keepHosts 保留 hostidx（多端口 app 不被误删）。
func TestPruneKeepsMultiportHostIndex(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "prune.test")

	if _, err := c.ReplaceHostRoutes(ctx, "prune.test", 1, []HostRoute{{Port: 80}, {Port: 8081}}, 80); err != nil {
		t.Fatal(err)
	}
	keepRoutes := map[string]bool{"route:prune.test:80": true, "route:prune.test:8081": true}
	keepHosts := map[string]bool{"prune.test": true}
	if err := c.PruneRoutes(ctx, keepRoutes, keepHosts); err != nil {
		t.Fatal(err)
	}
	ports, err := c.HostPorts(ctx, "prune.test")
	if err != nil || len(ports) != 2 {
		t.Fatalf("prune must keep both ports, got %v %v", ports, err)
	}
	cleanHost(t, c, "prune.test")
}

// D-2：乱序发布——旧 revision 快照整体拒绝（含删除意图），高 revision 生效；
// revision 写入条目 JSON 供 edge 端守卫读取。
func TestReplaceHostRoutesRejectsOutOfOrderRevision(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "ooo.test")
	t.Cleanup(func() { cleanHost(t, c, "ooo.test") })

	// rev 2 发布：两端口。
	applied, err := c.ReplaceHostRoutes(ctx, "ooo.test", 2, []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "new", AppPort: 80}}}},
		{Port: 8081, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "new", AppPort: 8081}}}},
	}, 80)
	if err != nil || !applied {
		t.Fatalf("rev 2: applied=%v err=%v", applied, err)
	}
	// rev 1 的旧快照迟到：试图复活已被淘汰的 backend / 缩减端口集 → 整体拒绝。
	applied, err = c.ReplaceHostRoutes(ctx, "ooo.test", 1, []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "stale", AppPort: 80}}}},
	}, 80)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("lower revision must be rejected by the high-water guard")
	}
	route, err := c.GetRoute(ctx, "ooo.test", 80)
	if err != nil || route == nil || route.Backends[0].MachineID != "new" || route.Revision != 2 {
		t.Fatalf("rejected redraw must leave rev-2 projection intact: %+v %v", route, err)
	}
	if ports, _ := c.HostPorts(ctx, "ooo.test"); len(ports) != 2 {
		t.Fatalf("rejected redraw must not shrink port set: %v", ports)
	}
	// 同 revision 重放：不高于高水位，同样拒绝（幂等：同 rev 发布本就应去重）。
	applied, err = c.ReplaceHostRoutes(ctx, "ooo.test", 2, []HostRoute{{Port: 80}}, 80)
	if err != nil || applied {
		t.Fatalf("same revision replay: applied=%v err=%v", applied, err)
	}
	// rev 3：新发布生效，高水位前移。
	applied, err = c.ReplaceHostRoutes(ctx, "ooo.test", 3, []HostRoute{{Port: 80}}, 80)
	if err != nil || !applied {
		t.Fatalf("rev 3: applied=%v err=%v", applied, err)
	}
	if ports, _ := c.HostPorts(ctx, "ooo.test"); len(ports) != 1 {
		t.Fatalf("rev 3 must apply: %v", ports)
	}
}

// D-2 删除路径：合法删除（PruneRoutes 清掉 route/hostidx）后，乱序重放的
// 低 revision 重建不得复活陈旧 backend——高水位键就是墓碑记忆，不随投影删除。
func TestRevisionHighWaterSurvivesDeleteThenRecreate(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "gone.test")
	t.Cleanup(func() { cleanHost(t, c, "gone.test") })

	if _, err := c.ReplaceHostRoutes(ctx, "gone.test", 5, []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "m1", AppPort: 80}}}},
	}, 80); err != nil {
		t.Fatal(err)
	}
	// 合法删除全量投影（route 从 PG 删除后的 prune）。
	if err := c.PruneRoutes(ctx, map[string]bool{}, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if route, _ := c.GetRoute(ctx, "gone.test", 80); route != nil {
		t.Fatalf("prune must delete projection, got %+v", route)
	}
	// 低 revision 重放试图重建（delete-then-recreate 竞态）→ 拒绝。
	applied, err := c.ReplaceHostRoutes(ctx, "gone.test", 3, []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "stale", AppPort: 80}}}},
	}, 80)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale recreate after delete must be rejected by surviving high-water")
	}
	if route, _ := c.GetRoute(ctx, "gone.test", 80); route != nil {
		t.Fatalf("stale backend resurrected: %+v", route)
	}
	// PG revision 单调保证合法重建必然 rev > 5 → 生效。
	applied, err = c.ReplaceHostRoutes(ctx, "gone.test", 6, []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "m2", AppPort: 80}}}},
	}, 80)
	if err != nil || !applied {
		t.Fatalf("legit recreate: applied=%v err=%v", applied, err)
	}
}

// D-2 升级路径：旧发布器写出的条目（无 revision 字段）反序列化为 0，
// 首个带 revision 的发布（≥1）可无门槛覆盖旧投影。
func TestLegacyEntryWithoutRevisionUpgrades(t *testing.T) {
	c := New(testRedis(t))
	ctx := context.Background()
	cleanHost(t, c, "legacyup.test")
	t.Cleanup(func() { cleanHost(t, c, "legacyup.test") })

	// 手写旧形态投影：hostidx 单数字 + 无 revision 字段的 route JSON。
	if err := c.rdb.Set(ctx, hostIndexKey("legacyup.test"), "8080", 0).Err(); err != nil {
		t.Fatal(err)
	}
	legacy := `{"route_generation":7,"backends":[{"machine_id":"m1","app_port":8080}]}`
	if err := c.rdb.Set(ctx, routeKey("legacyup.test", 8080), legacy, 0).Err(); err != nil {
		t.Fatal(err)
	}
	route, err := c.GetRouteForHostname(ctx, "legacyup.test")
	if err != nil || route == nil {
		t.Fatalf("legacy read: %+v %v", route, err)
	}
	if route.Revision != 0 {
		t.Fatalf("legacy entry must deserialize with revision=0, got %d", route.Revision)
	}
	if route.RouteGeneration != 7 {
		t.Fatalf("legacy payload must parse: %+v", route)
	}
	// 首个 revision 发布生效（miss → 直接生效）。
	applied, err := c.ReplaceHostRoutes(ctx, "legacyup.test", 1, []HostRoute{
		{Port: 8080, Route: Route{RouteGeneration: 8, Backends: []Backend{{MachineID: "m2", AppPort: 8080}}}},
	}, 8080)
	if err != nil || !applied {
		t.Fatalf("first revisioned publish: applied=%v err=%v", applied, err)
	}
	route, _ = c.GetRoute(ctx, "legacyup.test", 8080)
	if route == nil || route.Revision != 1 || route.Backends[0].MachineID != "m2" {
		t.Fatalf("post-upgrade entry: %+v", route)
	}
}

// TestBackendULAHintJSONCompat（G2b，ADR-0040 §16）：
//   - 无提示 backend 的 JSON 与旧形态字节一致（新增字段全部 omitempty）；
//   - 带提示 backend 往返无损；
//   - 提示经 ReplaceHostRoutes 全链路（序列化进 route 条目、revision 高
//     水位守卫不受影响）。
func TestBackendULAHintJSONCompat(t *testing.T) {
	old := Backend{
		MachineID: "m1", ExecutionID: "e1", NodeProxyEndpoint: "p:5107",
		AppPort: 8080, Readiness: "READY", Weight: 100,
	}
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	// 旧形态基准（draining 无 omitempty，本就在旧 JSON 中）。
	const wantOld = `{"machine_id":"m1","execution_id":"e1","node_proxy_endpoint":"p:5107","app_port":8080,"readiness":"READY","weight":100,"draining":false}`
	if string(raw) != wantOld {
		t.Fatalf("old-form JSON changed: %s", raw)
	}
	hinted := old
	hinted.ULA = "fd7a:9a55:0:1::7"
	hinted.IdentityID = 42
	hinted.Generation = 3
	raw2, err := json.Marshal(hinted)
	if err != nil {
		t.Fatal(err)
	}
	var back Backend
	if err := json.Unmarshal(raw2, &back); err != nil {
		t.Fatal(err)
	}
	if back.ULA != hinted.ULA || back.IdentityID != 42 || back.Generation != 3 {
		t.Fatalf("hint roundtrip lost: %+v", back)
	}

	rdb := testRedis(t)
	cat := New(rdb)
	ctx := context.Background()
	// 共享 lab Redis：hostname 带运行标识，避免上次运行的高水位键挡住本次。
	hostname := fmt.Sprintf("hints-%d.test", time.Now().UnixNano())
	route := Route{RouteGeneration: 1, Backends: []Backend{hinted}}
	applied, err := cat.ReplaceHostRoutes(ctx, hostname, 1,
		[]HostRoute{{Port: 8080, Route: route}}, 8080)
	if err != nil || !applied {
		t.Fatalf("replace = (%v, %v)", applied, err)
	}
	got, err := cat.GetRoute(ctx, hostname, 8080)
	if err != nil || got == nil {
		t.Fatalf("get route = (%+v, %v)", got, err)
	}
	b := got.Backends[0]
	if b.ULA != "fd7a:9a55:0:1::7" || b.IdentityID != 42 || b.Generation != 3 {
		t.Fatalf("hint lost through projection: %+v", b)
	}
	// 旧 revision 重放（即便带提示）被高水位拒。
	stale := Route{RouteGeneration: 1, Backends: []Backend{hinted}}
	applied2, err := cat.ReplaceHostRoutes(ctx, hostname, 1,
		[]HostRoute{{Port: 8080, Route: stale}}, 8080)
	if err != nil || applied2 {
		t.Fatalf("stale revision accepted: (%v, %v)", applied2, err)
	}
}

// TestInternalDNSProjectionLifecycle（G2c，ADR-0040 §16/§18）：全量替换、
// 缺席键删除、TTL = serve-stale 预算（键过期自然消失——ListInternalDNS
// 不再返回，reconciler 快照随之摘除）。
func TestInternalDNSProjectionLifecycle(t *testing.T) {
	rdb := testRedis(t)
	cat := New(rdb)
	ctx := context.Background()

	recs := []InternalDNSRecord{
		{Name: "a.p.internal", AAAA: []string{"fd7a:9a55:0:1::1"}, Generation: 1},
		{Name: "b.p.internal", AAAA: []string{"fd7a:9a55:0:1::2", "fd7a:9a55:0:1::3"}, Generation: 5},
	}
	if err := cat.ReplaceInternalDNS(ctx, recs, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := cat.ListInternalDNS(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("list = (%+v, %v)", got, err)
	}
	if got[0].Name != "a.p.internal" || got[1].Name != "b.p.internal" || got[1].Generation != 5 {
		t.Fatalf("list content = %+v", got)
	}

	// 全量替换：a 下线（缺席即删）、b 更新。
	if err := cat.ReplaceInternalDNS(ctx, []InternalDNSRecord{
		{Name: "b.p.internal", AAAA: []string{"fd7a:9a55:0:1::9"}, Generation: 6},
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err = cat.ListInternalDNS(ctx)
	if err != nil || len(got) != 1 || got[0].Name != "b.p.internal" || got[0].AAAA[0] != "fd7a:9a55:0:1::9" {
		t.Fatalf("after replace = (%+v, %v)", got, err)
	}

	// TTL 过期（stale 预算）：短 TTL 写入后等待过期 → 键消失。
	if err := cat.ReplaceInternalDNS(ctx, []InternalDNSRecord{
		{Name: "c.p.internal", AAAA: []string{"fd7a:9a55:0:2::1"}, Generation: 7},
	}, 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	got, err = cat.ListInternalDNS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range got {
		if r.Name == "c.p.internal" {
			t.Fatal("expired key must disappear (stale budget)")
		}
	}

	// 空集发布（全部下线）= 只清不写。
	if err := cat.ReplaceInternalDNS(ctx, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, err = cat.ListInternalDNS(ctx); err != nil || len(got) != 0 {
		t.Fatalf("empty publish must clear: (%+v, %v)", got, err)
	}
}

// TestMeshProjectionLifecycle（G2d，ADR-0040 §18）：mesh:peer / mesh:endpoint
// 全量替换、缺席删除、TTL 预算（键过期消失）、edge hub 自身不发布 endpoint。
func TestMeshProjectionLifecycle(t *testing.T) {
	rdb := testRedis(t)
	cat := New(rdb)
	ctx := context.Background()

	if err := cat.ReplaceMeshProjection(ctx,
		[]MeshPeerRecord{
			{NodeID: "n1", Pubkey: "PUB1", Endpoint: "10.0.0.1:51820", NodePrefix: "fd7a:9a55:0:1::/64"},
			{NodeID: "edge-hub", Pubkey: "PUB-E", Endpoint: "10.9.9.9:51821", NodePrefix: "fd7a:9a55:0:9::/64"},
		},
		[]MeshEndpointRecord{
			{
				MachineID: "m1", ExecutionID: "e1", NodeID: "n1", NodeULA: "fd7a:9a55:0:1::",
				IngressPort: 5109, WorkloadULA: "fd7a:9a55:0:1::5", Generation: 3,
			},
		},
		time.Minute); err != nil {
		t.Fatal(err)
	}
	peers, err := cat.ListMeshPeers(ctx)
	if err != nil || len(peers) != 2 {
		t.Fatalf("peers = (%+v, %v)", peers, err)
	}
	if peers[0].NodeID != "edge-hub" || peers[1].NodeID != "n1" {
		t.Fatalf("peer order = %+v", peers)
	}
	ep, err := cat.GetMeshEndpoint(ctx, "m1", "e1")
	if err != nil || ep == nil || ep.NodeULA != "fd7a:9a55:0:1::" || ep.IngressPort != 5109 {
		t.Fatalf("endpoint = (%+v, %v)", ep, err)
	}
	if miss, err := cat.GetMeshEndpoint(ctx, "m-none", "e-none"); err != nil || miss != nil {
		t.Fatalf("miss = (%+v, %v)", miss, err)
	}

	// 全量替换：peer 下线 + endpoint 摘除。
	if err := cat.ReplaceMeshProjection(ctx,
		[]MeshPeerRecord{{NodeID: "n2", Pubkey: "PUB2", Endpoint: "10.0.0.2:51820", NodePrefix: "fd7a:9a55:0:2::/64"}},
		nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	if peers, err = cat.ListMeshPeers(ctx); err != nil || len(peers) != 1 || peers[0].NodeID != "n2" {
		t.Fatalf("after replace peers = (%+v, %v)", peers, err)
	}
	if ep, err = cat.GetMeshEndpoint(ctx, "m1", "e1"); err != nil || ep != nil {
		t.Fatalf("endpoint must be pruned: (%+v, %v)", ep, err)
	}

	// TTL 预算：短 TTL 过期 → 键消失。
	if err := cat.ReplaceMeshProjection(ctx, nil,
		[]MeshEndpointRecord{{MachineID: "m2", ExecutionID: "e2", NodeID: "n2", NodeULA: "fd7a:9a55:0:2::", IngressPort: 5109}},
		150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if ep, err = cat.GetMeshEndpoint(ctx, "m2", "e2"); err != nil || ep != nil {
		t.Fatalf("expired endpoint must disappear: (%+v, %v)", ep, err)
	}
}
