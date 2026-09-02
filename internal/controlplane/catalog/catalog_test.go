package catalog

import (
	"context"
	"os"
	"sync"
	"testing"

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
