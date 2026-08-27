// Package catalog 维护 Redis 路由投影（可重建，ADR-0005）。
// PG 是 route/backend 生命周期权威；本包只写 edge 查询投影，
// 绝不包含 slot_ip / netns / TAP 等 agent 内部信息。
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

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
	RouteGeneration int64     `json:"route_generation"`
	Backends        []Backend `json:"backends"`
}

// Catalog 是 Redis 客户端封装。
type Catalog struct {
	rdb *redis.Client
}

// New 构造 Catalog。
func New(rdb *redis.Client) *Catalog { return &Catalog{rdb: rdb} }

// PublishRoute 原子发布 route 投影，并维护 hostname → port 索引
// （评审 P3：edge 按 hostname 直查，不再每请求 SCAN）。
func (c *Catalog) PublishRoute(ctx context.Context, hostname string, port int, route Route) error {
	raw, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("marshal route: %w", err)
	}
	if err := c.rdb.Set(ctx, routeKey(hostname, port), raw, 0).Err(); err != nil {
		return fmt.Errorf("publish route: %w", err)
	}
	// M1 语义：每 hostname 一个活跃端口（与 PG routes 表一致）。
	if err := c.rdb.Set(ctx, hostIndexKey(hostname), strconv.Itoa(port), 0).Err(); err != nil {
		return fmt.Errorf("publish host index: %w", err)
	}
	return nil
}

// PublishLocation 发布 machine location 投影。
func (c *Catalog) PublishLocation(ctx context.Context, machineID, executionID, nodeProxyEndpoint string, appPort int) error {
	loc := map[string]any{
		"node_proxy_endpoint": nodeProxyEndpoint,
		"app_port":            appPort,
	}
	raw, err := json.Marshal(loc)
	if err != nil {
		return fmt.Errorf("marshal location: %w", err)
	}
	if err := c.rdb.Set(ctx, locationKey(machineID, executionID), raw, 0).Err(); err != nil {
		return fmt.Errorf("publish location: %w", err)
	}
	return nil
}

// GetRouteForHostname 查询 hostname 对应的 route 投影（经 hostidx 索引）。
// hostidx 缺失或指向的 route 已不存在时返回 nil；投影在 controller 的
// sync 周期内重建，短暂 miss 是预期行为。
func (c *Catalog) GetRouteForHostname(ctx context.Context, hostname string) (*Route, error) {
	portStr, err := c.rdb.Get(ctx, hostIndexKey(hostname)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get host index: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("bad host index for %s: %q", hostname, portStr)
	}
	return c.GetRoute(ctx, hostname, port)
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

// RemoveLocation 删除 machine location 投影。
func (c *Catalog) RemoveLocation(ctx context.Context, machineID, executionID string) error {
	return c.rdb.Del(ctx, locationKey(machineID, executionID)).Err()
}

// PruneRoutes 删除 keep 之外的 route:* 与 hostidx:* 投影（PG 仍是权威，
// 仅重建投影）。keepRoutes 的键形如 "route:{hostname}:{port}"。
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

func locationKey(machineID, executionID string) string {
	return fmt.Sprintf("machine:location:%s:%s", machineID, executionID)
}

// PublishLocationState 在 location 投影上附加状态位（M4.5）：
// paused=true 时 agent proxy 对该 machine 的请求将同步唤醒（autoresume）。
func (c *Catalog) PublishLocationState(ctx context.Context, machineID, executionID,
	nodeProxyEndpoint string, appPort int, paused bool) error {

	loc := map[string]any{
		"node_proxy_endpoint": nodeProxyEndpoint,
		"app_port":            appPort,
		"paused":              paused,
	}
	raw, err := json.Marshal(loc)
	if err != nil {
		return fmt.Errorf("marshal location: %w", err)
	}
	return c.rdb.Set(ctx, locationKey(machineID, executionID), raw, 0).Err()
}

// WipeProjections 删除全部 route/hostidx/location 投影键（M5.4 显式重投影）。
// PG 仍是权威；controller 下一个 sync 周期（5s routes/30s leases）即重建。
// 返回删除键数。
func (c *Catalog) WipeProjections(ctx context.Context) (int64, error) {
	var total int64
	for _, pattern := range []string{"route:*", "hostidx:*", "machine:location:*"} {
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
