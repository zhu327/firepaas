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
	_ = c.rdb.Del(ctx, hostIndexKey(hostname)).Err()
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
	if err := c.ReplaceHostRoutes(ctx, "multi.test", []HostRoute{
		{Port: 80, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "m1", AppPort: 80}}}},
		{Port: 8081, Route: Route{RouteGeneration: 1, Backends: []Backend{{MachineID: "m1", AppPort: 8081}}}},
	}, 80); err != nil {
		t.Fatal(err)
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
		go func() {
			defer wg.Done()
			errs <- c.ReplaceHostRoutes(ctx, "concurrent.test", routes, 80)
		}()
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

	if err := c.ReplaceHostRoutes(ctx, "replace.test", []HostRoute{{Port: 80}, {Port: 8081}}, 80); err != nil {
		t.Fatal(err)
	}
	if err := c.ReplaceHostRoutes(ctx, "replace.test", []HostRoute{{Port: 80}}, 80); err != nil {
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

	if err := c.ReplaceHostRoutes(ctx, "prune.test", []HostRoute{{Port: 80}, {Port: 8081}}, 80); err != nil {
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
