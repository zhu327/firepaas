// Package catalog 维护 Redis 路由投影（可重建，ADR-0005）。
// PG 是 route/backend 生命周期权威；本包只写 edge 查询投影，
// 绝不包含 slot_ip / netns / TAP 等 agent 内部信息。
package catalog

import (
	"context"
	"encoding/json"
	"fmt"

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

// PublishRoute 原子发布 route 投影。
func (c *Catalog) PublishRoute(ctx context.Context, hostname string, port int, route Route) error {
	raw, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("marshal route: %w", err)
	}
	if err := c.rdb.Set(ctx, routeKey(hostname, port), raw, 0).Err(); err != nil {
		return fmt.Errorf("publish route: %w", err)
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

// GetRouteForHostname 扫描 hostname 对应的任一端口 route（M1 单端口场景；
// 多端口在 M3 路由模型里由 edge 显式携带目标端口）。
func (c *Catalog) GetRouteForHostname(ctx context.Context, hostname string) (*Route, error) {
	iter := c.rdb.Scan(ctx, 0, fmt.Sprintf("route:%s:*", hostname), 100).Iterator()
	for iter.Next(ctx) {
		raw, err := c.rdb.Get(ctx, iter.Val()).Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get route %s: %w", iter.Val(), err)
		}
		var route Route
		if err := json.Unmarshal(raw, &route); err != nil {
			return nil, fmt.Errorf("parse route %s: %w", iter.Val(), err)
		}
		return &route, nil
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan hostname routes: %w", err)
	}
	return nil, nil
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
		return nil, fmt.Errorf("parse route: %w", err)
	}
	return &route, nil
}

// RemoveLocation 删除 machine location 投影。
func (c *Catalog) RemoveLocation(ctx context.Context, machineID, executionID string) error {
	return c.rdb.Del(ctx, locationKey(machineID, executionID)).Err()
}

// PruneRoutes 删除 keep 之外的 route:* 投影（PG 仍是权威，仅重建投影）。
func (c *Catalog) PruneRoutes(ctx context.Context, keep map[string]bool) error {
	iter := c.rdb.Scan(ctx, 0, "route:*", 100).Iterator()
	var stale []string
	for iter.Next(ctx) {
		if !keep[iter.Val()] {
			stale = append(stale, iter.Val())
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan routes: %w", err)
	}
	if len(stale) == 0 {
		return nil
	}
	return c.rdb.Del(ctx, stale...).Err()
}

func routeKey(hostname string, port int) string {
	return fmt.Sprintf("route:%s:%d", hostname, port)
}

func locationKey(machineID, executionID string) string {
	return fmt.Sprintf("machine:location:%s:%s", machineID, executionID)
}
