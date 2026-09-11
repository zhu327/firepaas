// Package routepublisher owns rollout-aware route derivation and the ordered
// PostgreSQL-to-Redis publication workflow. PostgreSQL is always synchronized
// before the rebuildable Redis projection is replaced or pruned.
package routepublisher

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// Store is the narrow PostgreSQL port needed by a complete route rebuild.
type Store interface {
	ActiveRouteMachines(context.Context) ([]store.Machine, error)
	ListActiveRollouts(context.Context) ([]store.Rollout, error)
	ListDeployments(context.Context, string) ([]store.Deployment, error)
	// FabricIdentities 返回集群在役 execution 的 ULA↔identity 映射（G2b：
	// mesh_direct 服务的 route backend 提示来源；同 fabric 快照权威）。
	FabricIdentities(context.Context) ([]store.FabricIdentityRow, error)
	// SyncRoutes 提交权威 route 集合并同事务分配各 hostname 的发布
	// revision（D-2，单调，leader 换届不回退）。
	SyncRoutes(context.Context, []store.RouteRow) (map[string]int64, error)
}

// Catalog is the narrow Redis projection port needed by a complete rebuild.
type Catalog interface {
	// ReplaceHostRoutes 返回 applied=false 表示被 revision 高水位拒绝
	// （旧乱序快照，安全丢弃）。
	ReplaceHostRoutes(context.Context, string, int64, []catalog.HostRoute, int) (bool, error)
	PruneRoutes(context.Context, map[string]bool, map[string]bool) error
	// ReplaceInternalDNS（G2c）：.internal 投影全量替换（TTL = stale 预算）。
	ReplaceInternalDNS(context.Context, []catalog.InternalDNSRecord, time.Duration) error
}

// Publisher is the controller's sole route writer.
//
// D-2：进程内 serialize 全部 Rebuild（周期 sync 与显式 KickRouteRebuild
// 共用此互斥）。同一进程内两个 rebuild 并发不会并发分配 revision / 乱序
// 写 Redis；跨进程（leader 换届）的乱序由 PG 分配的单调 revision + catalog
// 高水位 Lua 守卫兜底。
type Publisher struct {
	store           Store
	catalog         Catalog
	defaultAppPort  int
	legacyProxyAddr string
	// dnsStaleWindow（G2c）：dns:internal 键 TTL = serve-stale 预算。
	dnsStaleWindow time.Duration
	mu             sync.Mutex
}

func New(st Store, cat Catalog, defaultAppPort int, legacyProxyAddr string) *Publisher {
	return &Publisher{
		store: st, catalog: cat, defaultAppPort: defaultAppPort,
		legacyProxyAddr: legacyProxyAddr, dnsStaleWindow: defaultDNSStaleWindow,
	}
}

// defaultDNSStaleWindow 与 edge stale 窗口同源（ADR-0040 §16：
// FIREPAAS_EDGE_STALE_WINDOW 同值，默认 120s）。
const defaultDNSStaleWindow = 120 * time.Second

// SetDNSStaleWindow 配置 .internal 投影 TTL（<=0 恢复默认）。
func (p *Publisher) SetDNSStaleWindow(d time.Duration) {
	if d <= 0 {
		d = defaultDNSStaleWindow
	}
	p.dnsStaleWindow = d
}

// Input is the complete in-memory snapshot consumed by deterministic derivation.
type Input struct {
	Machines        []store.Machine
	Deployments     []store.Deployment
	Rollouts        []store.Rollout
	ProxyByNode     map[string]string
	DefaultAppPort  int
	LegacyProxyAddr string
	// FabricByIdentity（G2b）：在役 execution 的 fabric 身份，键 =
	// machineID+"\x00"+executionID。nil/缺项 = 无 mesh 提示（零回归）。
	FabricByIdentity map[string]store.FabricIdentityRow
}

// Projection contains PostgreSQL route facts and the primary-port information
// needed to publish the equivalent Redis representation.
type Projection struct {
	Routes       []store.RouteRow
	PrimaryPorts map[string]int
	// InternalDNS（G2c，ADR-0040 §16）：{app}.{project}.internal → AAAA
	// 集，与 backend ULA 提示同源（mesh_direct ∧ serving ∧ 有在役身份）。
	InternalDNS []catalog.InternalDNSRecord
}

// Rebuild loads route inputs, derives one projection without I/O, commits the
// PostgreSQL authority, then replaces and prunes Redis. A Redis failure is
// returned without undoing or reinterpreting the PostgreSQL result.
func (p *Publisher) Rebuild(ctx context.Context, proxyByNode map[string]string) error {
	// D-2：进程内串行化（mutex 而非 singleflight 丢弃：KickRouteRebuild 的
	// 调用方期待自己这次重建真的执行过，排队等先行者结束后仍跑一遍）。
	p.mu.Lock()
	defer p.mu.Unlock()

	machines, err := p.store.ActiveRouteMachines(ctx)
	if err != nil {
		return err
	}
	rollouts, err := p.store.ListActiveRollouts(ctx)
	if err != nil {
		return err
	}

	apps := make(map[string]bool)
	for _, m := range machines {
		apps[m.AppID] = true
	}
	appIDs := make([]string, 0, len(apps))
	for appID := range apps {
		appIDs = append(appIDs, appID)
	}
	sort.Strings(appIDs)
	var deployments []store.Deployment
	for _, appID := range appIDs {
		deps, err := p.store.ListDeployments(ctx, appID)
		if err != nil {
			return err
		}
		deployments = append(deployments, deps...)
	}
	// G2b：在役 fabric 身份（mesh_direct 服务提示；失败不阻断发布——缺
	// 提示只影响未来的 mesh 路径，旧路径不受损，降级记日志）。
	fabricByIdentity := map[string]store.FabricIdentityRow{}
	if ids, err := p.store.FabricIdentities(ctx); err != nil {
		slog.Warn("route rebuild: fabric identities unavailable, mesh hints skipped", "error", err)
	} else {
		for _, id := range ids {
			fabricByIdentity[id.MachineID+"\x00"+id.ExecutionID] = id
		}
	}

	projection := Derive(Input{
		Machines: machines, Deployments: deployments, Rollouts: rollouts,
		ProxyByNode: proxyByNode, DefaultAppPort: p.defaultAppPort,
		LegacyProxyAddr:  p.legacyProxyAddr,
		FabricByIdentity: fabricByIdentity,
	})
	revisions, err := p.store.SyncRoutes(ctx, projection.Routes)
	if err != nil {
		return err
	}
	return p.publishAll(ctx, projection, revisions)
}

// publishAll 追加 G2c .internal 投影发布（route 发布完成后；失败上抛，
// 下轮重建重试——投影可重建，无一致性窗口问题）。
func (p *Publisher) publishAll(ctx context.Context, projection Projection, revisions map[string]int64) error {
	if err := p.publishRedis(ctx, projection, revisions); err != nil {
		return err
	}
	return p.publishInternalDNS(ctx, projection)
}

// Derive applies readiness, rollout, generation, and multiport policy without I/O.
func Derive(in Input) Projection {
	rolloutByApp := make(map[string]*store.Rollout, len(in.Rollouts))
	for i := range in.Rollouts {
		rolloutByApp[in.Rollouts[i].AppID] = &in.Rollouts[i]
	}
	depGen := make(map[string]int64, len(in.Deployments))
	depServices := make(map[string][]store.ServiceSpec, len(in.Deployments))
	depStrategy := make(map[string]string, len(in.Deployments))
	depStatus := make(map[string]string, len(in.Deployments))
	toDepByApp := make(map[string]string)
	for i := range in.Deployments {
		dep := &in.Deployments[i]
		depGen[dep.ID] = dep.Generation
		depServices[dep.ID] = dep.EffectiveServices()
		depStrategy[dep.ID] = dep.EffectiveStrategy()
		depStatus[dep.ID] = dep.Status
		if rollout := rolloutByApp[dep.AppID]; rollout != nil && dep.Generation == rollout.ToGeneration {
			toDepByApp[dep.AppID] = dep.ID
		}
	}

	type appOrdinal struct {
		app     string
		ordinal int
	}
	cutOrdinals := make(map[appOrdinal]bool)
	for _, m := range in.Machines {
		if toDepByApp[m.AppID] == m.DeploymentID && machineServing(m) {
			cutOrdinals[appOrdinal{m.AppID, m.ReplicaOrdinal}] = true
		}
	}

	type routeKey struct {
		hostname string
		port     int
	}
	grouped := make(map[routeKey]*store.RouteRow)
	primaryPorts := make(map[string]int)
	// G2c：.internal 记录聚合（app 粒度）。
	dnsSet := make(map[string][]string)
	dnsGen := make(map[string]int64)
	for _, m := range in.Machines {
		port := m.IngressPort
		if port == 0 {
			port = in.DefaultAppPort
		}
		services := depServices[m.DeploymentID]
		if len(services) == 0 {
			services = []store.ServiceSpec{{Name: "default", InternalPort: port}}
		}
		proxy := in.ProxyByNode[m.NodeID]
		if proxy == "" {
			proxy = in.LegacyProxyAddr
		}
		generation := depGen[m.DeploymentID]
		// review 2026-09-10：rollout 完成（或回滚失败）后旧代 deployment 已
		// SUPERSEDED/FAILED，但它的 machine 在 delete 操作收敛前仍是
		// desired_state=CREATED/RUNNING。旧实现只在“有活跃 rollout”时计算
		// draining，COMPLETE 后旧代 backend 会被重新发布上线。终态的
		// 非活跃代永远不能 serving，与是否有活跃 rollout 无关。
		draining := false
		if s := depStatus[m.DeploymentID]; s == "SUPERSEDED" || s == "FAILED" {
			draining = true
		}
		if rollout := rolloutByApp[m.AppID]; rollout != nil {
			rolling := depStrategy[toDepByApp[m.AppID]] == "rolling" && rollout.Status == "PREPARING"
			switch rollout.Status {
			case "PREPARING":
				if rolling {
					cut := cutOrdinals[appOrdinal{m.AppID, m.ReplicaOrdinal}]
					if generation == rollout.ToGeneration {
						draining = !cut
					} else {
						draining = cut
					}
				} else {
					draining = generation != rollout.FromGeneration
				}
			case "CUTOVER":
				draining = generation != rollout.ToGeneration
			case "ROLLING_BACK":
				draining = generation != rollout.FromGeneration
			}
		}
		// Rollout cut decisions are derived from the complete machine snapshot
		// above, but neither authoritative route rows nor Redis may contain an
		// execution that is unready or draining.
		if !machineServing(m) || draining || proxy == "" {
			continue
		}
		for serviceIndex, service := range services {
			key := routeKey{m.Hostname, service.InternalPort}
			route := grouped[key]
			if route == nil {
				route = &store.RouteRow{Hostname: m.Hostname, Port: service.InternalPort, AppID: m.AppID}
				grouped[key] = route
			}
			if serviceIndex == 0 && (primaryPorts[m.Hostname] == 0 || service.InternalPort < primaryPorts[m.Hostname]) {
				primaryPorts[m.Hostname] = service.InternalPort
			}
			backend := store.RouteBackendRow{
				MachineID: m.ID, ExecutionID: m.CurrentExecutionID,
				NodeProxyEndpoint: proxy, AppPort: service.InternalPort, Weight: 100,
				Readiness: m.ObservedReadiness,
			}
			// G2b/G2c（ADR-0040 §16）：mesh_direct 服务才发布 ULA 提示与
			// .internal AAAA，且仅 READY 非 draining（严格于 route backend 的
			// serving 判定——UNCONFIGURED 的健康探测未确认服务在听，东向
			// 客户端连不上；非 serving/draining 已在上游 continue）。无身份
			//（未入 mesh/分配未落）= 无提示，零回归。
			hint := fabricHint(in.FabricByIdentity,
				service.MeshDirect && m.ObservedReadiness == "READY", m.ID, m.CurrentExecutionID)
			if hint.ULA.IsValid() {
				backend.ULA = hint.ULA.String()
				backend.IdentityID = hint.IdentityID
				backend.Generation = hint.Generation
				// G2c：同名 .internal 记录聚合（app 粒度 AAAA 集，去重；
				// generation 取该 execution 的 fabric 分配代；project 取
				// 身份行，与 fabric 快照同源）。
				dnsName := fmt.Sprintf("%s.%s.internal", strings.ToLower(m.AppID), strings.ToLower(hint.ProjectID))
				dnsSet[dnsName] = appendUniqueAAA(dnsSet[dnsName], backend.ULA)
				if hint.Generation > dnsGen[dnsName] {
					dnsGen[dnsName] = hint.Generation
				}
			}
			route.Backends = append(route.Backends, backend)
			if generation > route.Generation {
				route.Generation = generation
			}
		}
	}

	routes := make([]store.RouteRow, 0, len(grouped))
	for _, route := range grouped {
		sort.Slice(route.Backends, func(i, j int) bool {
			if route.Backends[i].MachineID != route.Backends[j].MachineID {
				return route.Backends[i].MachineID < route.Backends[j].MachineID
			}
			return route.Backends[i].ExecutionID < route.Backends[j].ExecutionID
		})
		routes = append(routes, *route)
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Hostname != routes[j].Hostname {
			return routes[i].Hostname < routes[j].Hostname
		}
		return routes[i].Port < routes[j].Port
	})
	internalDNS := make([]catalog.InternalDNSRecord, 0, len(dnsSet))
	for name, aaaas := range dnsSet {
		sort.Strings(aaaas)
		internalDNS = append(internalDNS, catalog.InternalDNSRecord{
			Name: name, AAAA: aaaas, Generation: dnsGen[name],
		})
	}
	sort.Slice(internalDNS, func(i, j int) bool { return internalDNS[i].Name < internalDNS[j].Name })
	return Projection{Routes: routes, PrimaryPorts: primaryPorts, InternalDNS: internalDNS}
}

func (p *Publisher) publishRedis(ctx context.Context, projection Projection, revisions map[string]int64) error {
	keepRoutes := make(map[string]bool, len(projection.Routes))
	keepHosts := make(map[string]bool, len(projection.Routes))
	hostRoutes := make(map[string][]catalog.HostRoute)
	for _, route := range projection.Routes {
		keepRoutes[fmt.Sprintf("route:%s:%d", route.Hostname, route.Port)] = true
		keepHosts[route.Hostname] = true
		converted := catalog.Route{
			RouteGeneration: route.Generation,
			Backends:        make([]catalog.Backend, 0, len(route.Backends)),
		}
		for _, backend := range route.Backends {
			converted.Backends = append(converted.Backends, catalog.Backend{
				MachineID: backend.MachineID, ExecutionID: backend.ExecutionID,
				NodeProxyEndpoint: backend.NodeProxyEndpoint, AppPort: backend.AppPort,
				Readiness: backend.Readiness, Weight: backend.Weight, Draining: backend.Draining,
				// G2b（ADR-0040 §16）：mesh 直连提示（mesh_direct ∧ READY 才填；
				// 零值 = 无提示，旧 edge 忽略未知字段）。
				ULA: backend.ULA, IdentityID: backend.IdentityID, Generation: backend.Generation,
			})
		}
		hostRoutes[route.Hostname] = append(
			hostRoutes[route.Hostname],
			catalog.HostRoute{Port: route.Port, Route: converted},
		)
	}
	hostnames := make([]string, 0, len(hostRoutes))
	for hostname := range hostRoutes {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	for _, hostname := range hostnames {
		// revisions 必有该 hostname（SyncRoutes 对每个活跃 hostname 分配）；
		// 缺失视为 0 会被高水位守卫拒绝——宁可拒绝也不发布无 revision 投影。
		rev := revisions[hostname]
		if _, err := p.catalog.ReplaceHostRoutes(ctx, hostname, rev, hostRoutes[hostname], projection.PrimaryPorts[hostname]); err != nil {
			return err
		}
	}
	return p.catalog.PruneRoutes(ctx, keepRoutes, keepHosts)
}

// publishInternalDNS（G2c，ADR-0040 §16/§18）：.internal 投影全量替换。
// TTL = FIREPAAS_AGENT_DNS_STALE_WINDOW（与 edge stale 窗口同值 120s）：
// 发布器每轮刷新；leader 死亡后键在预算内过期，reconciler 下一轮快照
// 摘除记录，节点本地 DNS 随之停服（断流降级语义）。空集也发布（合法
// 状态：全部 mesh_direct 关闭）——只清键不写新键。
func (p *Publisher) publishInternalDNS(ctx context.Context, projection Projection) error {
	return p.catalog.ReplaceInternalDNS(ctx, projection.InternalDNS, p.dnsStaleWindow)
}

// appendUniqueAAA 有序去重追加（ULA /128 全局唯一，重复来自同 execution
// 的多 mesh_direct 服务）。保持首次出现顺序（调用方最终统一排序）。
func appendUniqueAAA(list []string, ula string) []string {
	for _, v := range list {
		if v == ula {
			return list
		}
	}
	return append(list, ula)
}

func machineServing(m store.Machine) bool {
	if m.ObservedState != "RUNNING" && m.ObservedState != "PAUSED" {
		return false
	}
	return m.ObservedReadiness == "READY" || m.ObservedReadiness == "UNCONFIGURED"
}

// fabricHint 取 mesh_direct 服务的 backend 提示（G2b，ADR-0040 §16）。
// 非直连服务或无在役身份（未入 mesh/分配未落）→ 零值（不发布提示）。
func fabricHint(
	in map[string]store.FabricIdentityRow,
	meshDirect bool,
	machineID, executionID string,
) store.FabricIdentityRow {
	if !meshDirect {
		return store.FabricIdentityRow{}
	}
	return in[machineID+"\x00"+executionID]
}
