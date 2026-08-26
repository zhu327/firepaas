// Package controller 实现 M1 单实例 reconcile：
//
//	PG operations（desired）→ agent gRPC → PG observed → Redis route 投影
//
// 不直接管理 VM 生命周期之外的逻辑；调度器/多节点在 M2 加入。
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/firepaas/internal/controlplane/agentclient"
	"github.com/example/firepaas/internal/controlplane/catalog"
	"github.com/example/firepaas/internal/controlplane/store"
	pb "github.com/example/firepaas/shared/gen/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Config 是 controller 运行参数。
type Config struct {
	AgentAddr      string // 单节点 agent gRPC 地址
	AgentProxyAddr string // 节点 agent proxy 地址（写入 Redis，edge 使用）
	DefaultAppPort int
	OpPollInterval time.Duration
	SyncInterval   time.Duration
}

// Controller 执行 reconcile。
type Controller struct {
	store   *store.Store
	catalog *catalog.Catalog
	client  *agentclient.Client
	cfg     Config
}

// New 构造 Controller 并连接 agent。
func New(ctx context.Context, st *store.Store, cat *catalog.Catalog, cfg Config) (*Controller, error) {
	client, err := agentclient.Dial(cfg.AgentAddr)
	if err != nil {
		return nil, err
	}
	return &Controller{store: st, catalog: cat, client: client, cfg: cfg}, nil
}

// Run 启动两个循环：操作 reconcile 与 observed 同步。
func (c *Controller) Run(ctx context.Context) error {
	opTicker := time.NewTicker(c.cfg.OpPollInterval)
	syncTicker := time.NewTicker(c.cfg.SyncInterval)
	defer opTicker.Stop()
	defer syncTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-opTicker.C:
			if err := c.reconcileOperations(ctx); err != nil {
				slog.Error("reconcile operations", "error", err)
			}
		case <-syncTicker.C:
			if err := c.syncObserved(ctx); err != nil {
				slog.Error("sync observed", "error", err)
			}
		}
	}
}

// Close 释放 agent 连接。
func (c *Controller) Close() error { return c.client.Close() }

func (c *Controller) reconcileOperations(ctx context.Context) error {
	ops, err := c.store.ClaimPendingOperations(ctx, 10)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if err := c.processOperation(ctx, op); err != nil {
			slog.Error("process operation", "op", op.ID, "kind", op.Kind, "error", err)
			_ = c.store.RequeueOperation(ctx, op.ID, err.Error())
		}
	}
	if len(ops) > 0 {
		return c.buildRoutes(ctx)
	}
	return nil
}

func (c *Controller) processOperation(ctx context.Context, op store.Operation) error {
	switch op.Kind {
	case "create":
		var req pb.CreateMachineRequest
		if err := protojson.Unmarshal(op.Request, &req); err != nil {
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
			return err
		}
		machine, err := c.client.Create(ctx, &req)
		if err != nil {
			return fmt.Errorf("agent create: %w", err)
		}
		if err := c.store.UpdateMachineObserved(ctx, machine.MachineId, machine.ExecutionId,
			machine.State.String(), machine.SlotIp, machine.Readiness.String()); err != nil {
			return err
		}
		result, _ := protojson.Marshal(machine)
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", result, "")

	case "delete":
		var del pb.DeleteMachineRequest
		if err := protojson.Unmarshal(op.Request, &del); err != nil {
			_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
			return err
		}
		if err := c.client.Delete(ctx, &del); err != nil {
			return fmt.Errorf("agent delete: %w", err)
		}
		if err := c.store.MarkMachineDeleted(ctx, del.MachineId); err != nil {
			return err
		}
		_ = c.catalog.RemoveLocation(ctx, del.MachineId, del.ExecutionId)
		return c.store.CompleteOperation(ctx, op.ID, "SUCCEEDED", []byte(`{}`), "")

	default:
		err := fmt.Errorf("unknown operation kind %q", op.Kind)
		_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
		return err
	}
}

func (c *Controller) syncObserved(ctx context.Context) error {
	machines, err := c.client.List(ctx, "")
	if err != nil {
		return err
	}
	for _, m := range machines {
		pg, err := c.store.GetMachine(ctx, m.MachineId)
		if err != nil || pg == nil {
			continue
		}
		if err := c.store.UpdateMachineObserved(ctx, m.MachineId, m.ExecutionId,
			m.State.String(), m.SlotIp, m.Readiness.String()); err != nil {
			return err
		}
		port := c.cfg.DefaultAppPort
		if m.Spec.GetNetwork() != nil && m.Spec.GetNetwork().IngressPort != 0 {
			port = int(m.Spec.GetNetwork().IngressPort)
		}
		_ = c.catalog.PublishLocation(ctx, m.MachineId, m.ExecutionId, c.cfg.AgentProxyAddr, port)
	}
	return c.buildRoutes(ctx)
}

func (c *Controller) buildRoutes(ctx context.Context) error {
	machines, err := c.store.ActiveRouteMachines(ctx)
	if err != nil {
		return err
	}

	type routeKey struct {
		hostname string
		port     int
	}
	grouped := map[routeKey]*catalog.Route{}
	for _, m := range machines {
		port := m.IngressPort
		if port == 0 {
			port = c.cfg.DefaultAppPort
		}
		key := routeKey{hostname: m.Hostname, port: port}
		route := grouped[key]
		if route == nil {
			route = &catalog.Route{RouteGeneration: m.Generation, Backends: []catalog.Backend{}}
			grouped[key] = route
		}
		route.Backends = append(route.Backends, catalog.Backend{
			MachineID:         m.ID,
			ExecutionID:       m.CurrentExecutionID,
			NodeProxyEndpoint: c.cfg.AgentProxyAddr,
			AppPort:           port,
			Readiness:         m.ObservedReadiness,
			Weight:            100,
			Draining:          false,
		})
	}

	for key, route := range grouped {
		if err := c.catalog.PublishRoute(ctx, key.hostname, key.port, *route); err != nil {
			return err
		}
	}

	// 清理已删除/无观测 machine 的 stale route 投影。
	targets, err := c.store.ListRouteTargets(ctx)
	if err != nil {
		return err
	}
	keep := map[string]bool{}
	for _, rt := range targets {
		keep[fmt.Sprintf("route:%s:%d", rt.Hostname, rt.Port)] = true
	}
	return c.catalog.PruneRoutes(ctx, keep)
}
