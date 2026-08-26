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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Config 是 controller 运行参数。
type Config struct {
	AgentAddr      string // 单节点 agent gRPC 地址
	AgentProxyAddr string // 节点 agent proxy 地址（写入 PG/Redis，edge 使用）
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
			// RequeueOperation 只回退仍在 CLAIMED 的操作；已终态
			// （SUCCEEDED/FAILED）的操作不会被复活。
			_ = c.store.RequeueOperation(ctx, op.ID, err.Error())
		}
	}
	if len(ops) > 0 {
		return c.buildRoutes(ctx)
	}
	return nil
}

// isPermanentAgentError 判断 agent 返回的错误是否不可重试：
// 重试不可能改变结果的操作直接标记 FAILED，避免无限 requeue（M1 评审 P2-3）。
func isPermanentAgentError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, // 请求本身不合法
		codes.AlreadyExists,     // 同 operation_id 不同 request hash（幂等冲突）
		codes.FailedPrecondition, // 未来的 stale generation fencing（P0-2 落地后）
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.NotFound:
		return true
	default:
		return false
	}
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
			if isPermanentAgentError(err) {
				// 不可恢复：落终态，machine 期望行保留供排查/清理。
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			}
			return fmt.Errorf("agent create: %w", err) // 暂时性失败，requeue
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
			switch {
			case status.Code(err) == codes.NotFound:
				// agent 侧已不存在（如节点数据被清理）：幂等成功收敛。
				slog.Warn("delete target missing at agent; converging as deleted",
					"machine", del.MachineId, "execution", del.ExecutionId)
			case isPermanentAgentError(err):
				_ = c.store.CompleteOperation(ctx, op.ID, "FAILED", nil, err.Error())
				return err
			default:
				return fmt.Errorf("agent delete: %w", err) // 暂时性失败，requeue
			}
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

// buildRoutes 把观测到的活跃 machine 集合发布为 route：
// 先写 PG（routes/route_backends，ADR-0005 的权威），再写 Redis 投影，
// 最后清理两侧的 stale 条目。edge 永不读取 slot IP（ADR-0013 不变量 4）。
func (c *Controller) buildRoutes(ctx context.Context) error {
	machines, err := c.store.ActiveRouteMachines(ctx)
	if err != nil {
		return err
	}

	type routeKey struct {
		hostname string
		port     int
	}
	grouped := map[routeKey]*store.RouteRow{}
	for _, m := range machines {
		port := m.IngressPort
		if port == 0 {
			port = c.cfg.DefaultAppPort
		}
		key := routeKey{hostname: m.Hostname, port: port}
		route := grouped[key]
		if route == nil {
			route = &store.RouteRow{Hostname: m.Hostname, Port: port, AppID: m.AppID}
			grouped[key] = route
		}
		if m.Generation > route.Generation {
			route.Generation = m.Generation
		}
		route.Backends = append(route.Backends, store.RouteBackendRow{
			MachineID:         m.ID,
			ExecutionID:       m.CurrentExecutionID,
			NodeProxyEndpoint: c.cfg.AgentProxyAddr,
			AppPort:           port,
			Weight:            100,
			Readiness:         m.ObservedReadiness,
			Draining:          false,
		})
	}

	active := make([]store.RouteRow, 0, len(grouped))
	for _, route := range grouped {
		active = append(active, *route)
	}

	// PG 先落权威（包含 stale 清理），失败则本轮不发布 Redis 投影。
	if err := c.store.SyncRoutes(ctx, active); err != nil {
		return err
	}

	keepRoutes := make(map[string]bool, len(active))
	keepHosts := make(map[string]bool, len(active))
	for _, r := range active {
		keepRoutes[fmt.Sprintf("route:%s:%d", r.Hostname, r.Port)] = true
		keepHosts[r.Hostname] = true
		catalogRoute := catalog.Route{
			RouteGeneration: r.Generation,
			Backends:        make([]catalog.Backend, 0, len(r.Backends)),
		}
		for _, b := range r.Backends {
			catalogRoute.Backends = append(catalogRoute.Backends, catalog.Backend{
				MachineID:         b.MachineID,
				ExecutionID:       b.ExecutionID,
				NodeProxyEndpoint: b.NodeProxyEndpoint,
				AppPort:           b.AppPort,
				Readiness:         b.Readiness,
				Weight:            b.Weight,
				Draining:          b.Draining,
			})
		}
		if err := c.catalog.PublishRoute(ctx, r.Hostname, r.Port, catalogRoute); err != nil {
			return err
		}
	}
	return c.catalog.PruneRoutes(ctx, keepRoutes, keepHosts)
}
