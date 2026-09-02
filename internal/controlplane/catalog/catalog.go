// Package catalog 维护 Redis 路由投影（可重建，ADR-0005）。
// PG 是 route/backend 生命周期权威；本包只写 edge 查询投影，
// 绝不包含 slot_ip / netns / TAP 等 agent 内部信息。
//
// v1.1（ADR-0022）：hostidx 从"每 hostname 单活跃端口"演进为 hostname →
// 端口集合。值是 JSON 数组（首元素 = 主 service 端口，继承单端口语义——
// 80/443 请求按首元素查路由）。旧值形态（纯数字字符串）向后兼容读。
//
// D-2（R2 加固）：route 条目携带 publisher 分配的单调 revision；
// ReplaceHostRoutes 以 routerev:{hostname} 高水位键拒绝旧 revision 的乱序/
// 重放快照。旧形态条目（无 revision 字段）反序列化为 revision=0，任何
// 新发布（revision≥1）都可覆盖。
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Backend 是 edge 查询到的单个后端。
type Backend struct {
	MachineID         string `json:"machine_id"`
	ExecutionID       string `json:"execution_id"`
	NodeProxyEndpoint string `json:"node_proxy_endpoint"`
	AppPort           int    `json:"app_port"`
	Readiness         string `json:"readiness"`
	Weight            int    `json:"weight"`
	Draining          bool   `json:"draining"`
}

// Route 是 hostname+port 对应的版本化 backend set。
type Route struct {
	RouteGeneration int64 `json:"route_generation"`
	// Revision 是 publisher 分配的 hostname 级发布 revision（D-2 单调高水位；
	// 	旧条目缺省为 0）。omitempty 保持旧发布器写出的字节形态可逆。
	Revision int64     `json:"revision,omitempty"`
	Backends []Backend `json:"backends"`
}

// RouteRevision 实现 edge RouteCache 的 revision 守卫取值接口（nil 安全；
// 旧客户端/非 route 值视为 0）。
func (r *Route) RouteRevision() int64 {
	if r == nil {
		return 0
	}
	return r.Revision
}

// Catalog 是 Redis 客户端封装。
type Catalog struct {
	rdb *redis.Client
}

// New 构造 Catalog。
func New(rdb *redis.Client) *Catalog { return &Catalog{rdb: rdb} }

// HostRoute 是一个 hostname 的完整 route 投影项。
type HostRoute struct {
	Port  int
	Route Route
}

// ReplaceHostRoutes 原子地以 hostname 的完整端口集替换 route 投影和 hostidx。
// 它绝不读改写 hostidx：controller 每个 sync 已知道权威完整集合，因此 Lua
// 脚本在一次 Redis 原子操作中写入新 routes、删除已移除 service 的旧 route，
// 并替换索引。这避免多 route 发布并发时丢端口，也不会在 service 删除后留下
// hostidx/route 残留。旧的单数字 hostidx 也在脚本中兼容解析。
//
// D-2 乱序/重放守卫：revision 来自 PG route_publication_revisions（与 route
// 发布同一事务分配，hostname 级单调）。Lua 仅当 incoming revision 大于
// routerev:{hostname} 高水位时整体生效（miss=0 → 任何 revision≥1 生效），
// 并在生效时提升高水位。返回 applied=false 表示被高水位拒绝（旧快照被丢弃）。
//
// 删除路径语义（重点）：删除投影的 PruneRoutes/WipeProjections 只删
// route:*/hostidx:*，绝不删 routerev:*——高水位键就是"墓碑记忆"。否则
// “合法删除（rev N）→ 乱序重放的旧发布（rev < N）因键缺失直接生效而复活
// 陈旧 backend”无法防御；保留高水位后旧重放恒被拒绝，而 PG 表 revision
// 单调不回退保证任何合法重建必然带更大 revision、总能生效。代价是每个
// 历史 hostname 常驻一个小字符串键（基数有界，与历史 app 数同阶）。
func (c *Catalog) ReplaceHostRoutes(
	ctx context.Context,
	hostname string,
	revision int64,
	routes []HostRoute,
	primaryPort int,
) (bool, error) {
	if len(routes) == 0 {
		return false, fmt.Errorf("replace host routes %q: empty route set", hostname)
	}
	args := make([]interface{}, 0, 4+len(routes)*2)
	args = append(args, hostname, primaryPort, revision, len(routes))
	seen := make(map[int]bool, len(routes))
	for _, item := range routes {
		if item.Port <= 0 || seen[item.Port] {
			return false, fmt.Errorf("replace host routes %q: invalid or duplicate port %d", hostname, item.Port)
		}
		seen[item.Port] = true
		// revision 单一来源是参数：覆盖写入条目 JSON，供 edge 端守卫读取。
		item.Route.Revision = revision
		raw, err := json.Marshal(item.Route)
		if err != nil {
			return false, fmt.Errorf("marshal route: %w", err)
		}
		args = append(args, item.Port, raw)
	}
	if primaryPort <= 0 || !seen[primaryPort] {
		return false, fmt.Errorf("replace host routes %q: primary port %d not declared", hostname, primaryPort)
	}
	// KEYS[1] is the hostidx key, KEYS[2] the revision high-water key.
	// ARGV: hostname, primary, revision, count, then port/json.
	const replaceHostRoutes = `
local index = KEYS[1]
local revkey = KEYS[2]
local hostname = ARGV[1]
local primary = tonumber(ARGV[2])
local revision = tonumber(ARGV[3])
local count = tonumber(ARGV[4])
local current = tonumber(redis.call('GET', revkey) or '0')
if revision <= current then
  return 0
end
local wanted = {}
local ordered = {primary}
for i = 1, count do
  local port = tonumber(ARGV[4 + (i-1)*2 + 1])
  local raw = ARGV[4 + (i-1)*2 + 2]
  wanted[port] = true
  if port ~= primary then table.insert(ordered, port) end
  redis.call('SET', 'route:' .. hostname .. ':' .. port, raw)
end
local old = redis.call('GET', index)
if old then
  local ok, oldPorts = pcall(cjson.decode, old)
  if not ok or type(oldPorts) ~= 'table' then oldPorts = {tonumber(old)} end
  for _, port in ipairs(oldPorts) do
    if port and not wanted[port] then redis.call('DEL', 'route:' .. hostname .. ':' .. port) end
  end
end
redis.call('SET', index, cjson.encode(ordered))
redis.call('SET', revkey, revision)
return 1`
	applied, err := c.rdb.Eval(ctx, replaceHostRoutes, []string{hostIndexKey(hostname), routeRevisionKey(hostname)}, args...).
		Int()
	if err != nil {
		return false, fmt.Errorf("replace host routes: %w", err)
	}
	return applied == 1, nil
}

// HostPorts 读取 hostname 的端口集合（v1.1 hostidx；旧单端口值兼容）。
// hostidx 缺失返回 nil。
func (c *Catalog) HostPorts(ctx context.Context, hostname string) ([]int, error) {
	val, err := c.rdb.Get(ctx, hostIndexKey(hostname)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get host index: %w", err)
	}
	return parsePorts(val)
}

// parsePorts 解析 hostidx 值：JSON 数组（v1.1）或单数字（M1 旧形态）。
func parsePorts(val string) ([]int, error) {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil, nil
	}
	if strings.HasPrefix(val, "[") {
		var ports []int
		if err := json.Unmarshal([]byte(val), &ports); err != nil {
			return nil, fmt.Errorf("bad host index: %w", err)
		}
		return ports, nil
	}
	port, err := strconv.Atoi(val)
	if err != nil {
		return nil, fmt.Errorf("bad host index: %w", err)
	}
	return []int{port}, nil
}

// GetRouteForHostname 查询 hostname 主 service 端口对应的 route 投影（经
// hostidx 索引首元素；M1 单端口语义，80/443 请求路径）。
// hostidx 缺失或指向的 route 已不存在时返回 nil；投影在 controller 的
// sync 周期内重建，短暂 miss 是预期行为。
func (c *Catalog) GetRouteForHostname(ctx context.Context, hostname string) (*Route, error) {
	ports, err := c.HostPorts(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return nil, nil
	}
	return c.GetRoute(ctx, hostname, ports[0])
}

// GetRouteForPort 查询 (hostname, 请求端口) 的 route。declared 返回该端口
// 是否在 hostidx 集合内（false = 未声明端口，调用方 404 权威 miss 语义）。
// route 键存在但 hostidx 缺失（投影中间态）视为已声明。
func (c *Catalog) GetRouteForPort(
	ctx context.Context,
	hostname string,
	port int,
) (route *Route, declared bool, err error) {
	ports, err := c.HostPorts(ctx, hostname)
	if err != nil {
		return nil, false, err
	}
	if len(ports) == 0 {
		return nil, false, nil
	}
	declared = port == ports[0] // 主端口恒可查
	if !declared {
		for _, p := range ports {
			if p == port {
				declared = true
				break
			}
		}
	}
	if !declared {
		return nil, false, nil
	}
	route, err = c.GetRoute(ctx, hostname, port)
	return route, true, err
}

// GetRoute 读取 route 投影。
func (c *Catalog) GetRoute(ctx context.Context, hostname string, port int) (*Route, error) {
	raw, err := c.rdb.Get(ctx, routeKey(hostname, port)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get route: %w", err)
	}
	var route Route
	if err := json.Unmarshal(raw, &route); err != nil {
		return nil, fmt.Errorf("parse route %s: %w", routeKey(hostname, port), err)
	}
	return &route, nil
}

// PruneRoutes 删除 keep 之外的 route:* 与 hostidx:* 投影（PG 仍是权威，
// 仅重建投影）。keepRoutes 的键形如 "route:{hostname}:{port}"。
// 注意：刻意不删 routerev:* 高水位键（见 ReplaceHostRoutes 的删除路径注释）。
func (c *Catalog) PruneRoutes(ctx context.Context, keepRoutes, keepHosts map[string]bool) error {
	routeIter := c.rdb.Scan(ctx, 0, "route:*", 100).Iterator()
	var staleRoutes []string
	for routeIter.Next(ctx) {
		if !keepRoutes[routeIter.Val()] {
			staleRoutes = append(staleRoutes, routeIter.Val())
		}
	}
	if err := routeIter.Err(); err != nil {
		return fmt.Errorf("scan routes: %w", err)
	}

	hostIter := c.rdb.Scan(ctx, 0, "hostidx:*", 100).Iterator()
	var staleHosts []string
	for hostIter.Next(ctx) {
		key := hostIter.Val()
		hostname := key[len("hostidx:"):]
		if !keepHosts[hostname] {
			staleHosts = append(staleHosts, key)
		}
	}
	if err := hostIter.Err(); err != nil {
		return fmt.Errorf("scan host indexes: %w", err)
	}

	if len(staleRoutes) > 0 {
		if err := c.rdb.Del(ctx, staleRoutes...).Err(); err != nil {
			return fmt.Errorf("prune routes: %w", err)
		}
	}
	if len(staleHosts) > 0 {
		if err := c.rdb.Del(ctx, staleHosts...).Err(); err != nil {
			return fmt.Errorf("prune host indexes: %w", err)
		}
	}
	return nil
}

func routeKey(hostname string, port int) string {
	return fmt.Sprintf("route:%s:%d", hostname, port)
}

func hostIndexKey(hostname string) string {
	return fmt.Sprintf("hostidx:%s", hostname)
}

// routeRevisionKey 是 ReplaceHostRoutes 高水位键的命名（见删除路径注释：
// 本键永不随 route/hostidx 投影一起删除）。
func routeRevisionKey(hostname string) string {
	return fmt.Sprintf("routerev:%s", hostname)
}

// WipeProjections 删除全部 route/hostidx 投影键（M5.4 显式重投影）。
// PG 仍是权威；controller 下一个 sync 周期（5s routes/30s leases）即重建。
// 返回删除键数。routerev:* 高水位键刻意保留：wipe 窗口内到达的旧 revision
// 重放仍被拒绝，controller 重建从 PG 分配的 revision 必大于高水位。
func (c *Catalog) WipeProjections(ctx context.Context) (int64, error) {
	var total int64
	for _, pattern := range []string{"route:*", "hostidx:*"} {
		it := c.rdb.Scan(ctx, 0, pattern, 200).Iterator()
		var keys []string
		for it.Next(ctx) {
			keys = append(keys, it.Val())
		}
		if err := it.Err(); err != nil {
			return total, fmt.Errorf("scan %s: %w", pattern, err)
		}
		if len(keys) > 0 {
			if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
				return total, fmt.Errorf("wipe %s: %w", pattern, err)
			}
			total += int64(len(keys))
		}
	}
	return total, nil
}
