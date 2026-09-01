// Package nodemanager 实现 M2.1：Nomad native discovery → 节点 mTLS gRPC 连接池
// → InfoService.ServiceInfo 周期同步 → PG nodes observed projection。
//
// 节点健康状态机（ADR-0014）：Nomad 节点非 ready / scheduling ineligible 或
// ServiceInfo 非 HEALTHY → UNHEALTHY；Nomad drain → DRAINING；其余 HEALTHY。
// 发现失败保留上一轮快照但状态置 UNKNOWN（调度器排除）。
package nodemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// Node 是 nodemanager 维护的节点视图（含最近一次 ServiceInfo）。
type Node struct {
	NomadNodeID string
	Name        string
	NodePool    string
	Ready       bool   // Nomad 节点 ready
	Eligible    bool   // Nomad scheduling eligible
	Drain       bool   // Nomad drain
	Status      string // HEALTHY|DRAINING|UNHEALTHY|UNKNOWN
	GRPCAddr    string
	ProxyAddr   string
	Info        *pb.ServiceInfoResponse // 可为 nil（从未成功）
	LastSeen    time.Time
	// capabilitySig（v1.2-A，ADR-0023）：上一次已记录日志的能力签名
	//（protocol|features|snapshot key）；变化时记一条可审计日志。
	capabilitySig string
}

// NomadNodeStub 是 /v1/nodes 返回条目的最小字段集。
type NomadNodeStub struct {
	ID                    string `json:"ID"`
	Name                  string `json:"Name"`
	NodePool              string `json:"NodePool"`
	Status                string `json:"Status"` // ready|down|init|disconnected
	SchedulingEligibility string `json:"SchedulingEligibility"`
	Drain                 bool   `json:"Drain"`
	HTTPAddr              string `json:"HTTPAddr"`
	Address               string `json:"Address"` // Nomad 2.x /v1/nodes advertises the client IP here.
}

// NomadAllocStub 是 /v1/job/{job}/allocations 返回条目的最小字段集。
// 列表接口不回 AllocatedResources，端口信息需要按 alloc 详情补齐。
type NomadAllocStub struct {
	ID                 string          `json:"ID"`
	NodeID             string          `json:"NodeID"`
	JobVersion         uint64          `json:"JobVersion"`
	ClientStatus       string          `json:"ClientStatus"`
	AllocatedResources *allocResources `json:"AllocatedResources"`
}

type allocResources struct {
	Shared struct {
		Ports []struct {
			Label string `json:"Label"`
			Value int    `json:"Value"`
		} `json:"Ports"`
	} `json:"Shared"`
}

// Config 是 nodemanager 运行参数。
type Config struct {
	NomadAddr       string        // Nomad HTTP API 根地址
	JobName         string        // agentd job 名
	DiscoverEvery   time.Duration // 发现周期（10s）
	InfoEvery       time.Duration // ServiceInfo 同步周期（20s）
	ServiceInfoWait time.Duration // 单次 RPC 超时
	Store           *store.Store
}

// Manager 维护节点快照与 gRPC 连接池。
type Manager struct {
	cfg    Config
	http   *http.Client
	mu     sync.RWMutex
	nodes  map[string]*Node // key: Nomad node ID
	conns  map[string]*agentclient.Client
}

// New 构造 Manager。
func New(cfg Config) (*Manager, error) {
	m := &Manager{
		cfg:   cfg,
		http:  &http.Client{Timeout: 10 * time.Second},
		nodes: map[string]*Node{},
		conns: map[string]*agentclient.Client{},
	}
	if cfg.DiscoverEvery == 0 {
		m.cfg.DiscoverEvery = 10 * time.Second
	}
	if cfg.InfoEvery == 0 {
		m.cfg.InfoEvery = 20 * time.Second
	}
	if cfg.ServiceInfoWait == 0 {
		m.cfg.ServiceInfoWait = 5 * time.Second
	}
	return m, nil
}

// Run 周期执行发现与 ServiceInfo 同步，直到 ctx 取消。保留给单写者调用；
// 多 API 副本应分别使用 RunDiscovery（每副本）和 RunServiceInfo（仅 leader）。
func (m *Manager) Run(ctx context.Context) error {
	errCh := make(chan error, 2)
	go func() { errCh <- m.RunDiscovery(ctx) }()
	go func() { errCh <- m.RunServiceInfo(ctx) }()
	return <-errCh
}

// RunDiscovery 只维护 Nomad 节点快照与 agent 连接池，不调用 ServiceInfo，
// 也不写 PG。它可安全地在每个 API 副本运行，为 runtime logs/exec/cp 提供
// 本地 agent client；controller 是否为 leader 与该只读发现生命周期无关。
func (m *Manager) RunDiscovery(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.DiscoverEvery)
	defer ticker.Stop()
	_ = m.Discover(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.Discover(ctx); err != nil {
				slog.Error("nodemanager discover", "error", err)
			}
		}
	}
}

// RunServiceInfo 拉取 agent observed 信息并写 PG nodes 投影。该循环必须只由
// 当前 leader 运行，避免多个 nodemanager 并发覆盖 observed projection。
func (m *Manager) RunServiceInfo(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.InfoEvery)
	defer ticker.Stop()
	_ = m.SyncServiceInfo(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.SyncServiceInfo(ctx); err != nil {
				slog.Error("nodemanager service info sync", "error", err)
			}
		}
	}
}

// Discover 从 Nomad 拉节点与 firepaas-agentd 运行中 alloc，重建连接池。
// 拉取失败保留快照并把全部节点置 UNKNOWN。
func (m *Manager) Discover(ctx context.Context) error {
	nodeStubs, err := m.fetchNodes(ctx)
	if err != nil {
		m.markUnknown()
		return err
	}
	allocStubs, err := m.fetchAllocs(ctx)
	if err != nil {
		m.markUnknown()
		return err
	}

	byNode := map[string]*NomadNodeStub{}
	for i := range nodeStubs {
		byNode[nodeStubs[i].ID] = &nodeStubs[i]
	}
	bestAlloc := map[string]*NomadAllocStub{} // node → 最高 job version 的 running alloc
	for i := range allocStubs {
		a := &allocStubs[i]
		if a.ClientStatus != "running" {
			continue
		}
		if cur, ok := bestAlloc[a.NodeID]; !ok || a.JobVersion > cur.JobVersion {
			bestAlloc[a.NodeID] = a
		}
	}
	// 列表接口不带端口：对入选 alloc 按详情补齐（失败不阻塞，端口缺失=UNKNOWN）。
	for _, a := range bestAlloc {
		if a.AllocatedResources == nil {
			if err := m.fillAllocResources(ctx, a); err != nil {
				slog.Warn("fill alloc resources", "alloc", a.ID, "error", err)
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	seen := map[string]bool{}
	for nomadID, stub := range byNode {
		seen[nomadID] = true
		prev, existed := m.nodes[nomadID]
		n := &Node{
			NomadNodeID: nomadID, Name: stub.Name, NodePool: stub.NodePool,
			Ready: stub.Status == "ready", Eligible: stub.SchedulingEligibility == "eligible", Drain: stub.Drain,
		}
		if existed {
			n.Info = prev.Info
			n.LastSeen = prev.LastSeen
		}
		alloc := bestAlloc[nomadID]
		if alloc != nil {
			grpcPort, proxyPort := allocPorts(alloc)
			host, ok := nodeHost(stub)
			if ok && grpcPort > 0 {
				n.GRPCAddr = net.JoinHostPort(host, fmt.Sprintf("%d", grpcPort))
			}
			if ok && proxyPort > 0 {
				n.ProxyAddr = net.JoinHostPort(host, fmt.Sprintf("%d", proxyPort))
			}
		}
		n.Status = nomadStatus(n)
		if existed && prev.Info != nil && n.GRPCAddr != "" {
			// 保留上一轮 ServiceInfo 的判定，避免 Discover（10s）把
			// SyncServiceInfo（20s）刚写下的 HEALTHY 反复打回 UNKNOWN。
			n.Status = combinedStatus(n, prev.Info)
		}
		m.nodes[nomadID] = n

		if n.GRPCAddr != "" {
			m.ensureConnLocked(nomadID, n.GRPCAddr)
		} else if c, ok := m.conns[nomadID]; ok {
			_ = c.Close()
			delete(m.conns, nomadID)
		}
	}

	// 节点下架：关连接并从快照移除（PG 由 store 层在无 last_seen 更新后标记）。
	for nomadID, c := range m.conns {
		if !seen[nomadID] {
			_ = c.Close()
			delete(m.conns, nomadID)
			delete(m.nodes, nomadID)
		}
	}
	return nil
}

// SyncServiceInfo 对所有有 gRPC 地址的节点拉 ServiceInfo 并写 PG 投影。
func (m *Manager) SyncServiceInfo(ctx context.Context) error {
	m.mu.RLock()
	snapshot := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		snapshot = append(snapshot, n)
	}
	m.mu.RUnlock()

	for _, n := range snapshot {
		info := (*pb.ServiceInfoResponse)(nil)
		if n.GRPCAddr != "" {
			m.mu.RLock()
			client := m.conns[n.NomadNodeID]
			m.mu.RUnlock()
			if client != nil {
				rpcCtx, cancel := context.WithTimeout(ctx, m.cfg.ServiceInfoWait)
				resp, err := client.ServiceInfo(rpcCtx)
				cancel()
				if err != nil {
					slog.Warn("service info rpc", "node", n.Name, "addr", n.GRPCAddr, "error", err)
				} else {
					info = resp
				}
			}
		}

		m.mu.Lock()
		cur := m.nodes[n.NomadNodeID]
		if cur == nil {
			m.mu.Unlock()
			continue
		}
		if info != nil {
			cur.Info = info
			cur.LastSeen = time.Now()
			cur.Status = combinedStatus(cur, info)
			// v1.2-A（ADR-0023）：能力变化（含 agent 重启后的恢复）记审计日志。
			if sig := capabilitySignature(info); sig != cur.capabilitySig {
				slog.Info("node capability projection changed",
					"node", cur.NomadNodeID, "agent_node_id", info.NodeId,
					"protocol_version", info.ProtocolVersion,
					"snapshot_compatibility_key", info.SnapshotCompatibilityKey,
					"feature_ids", info.FeatureIds, "previous", cur.capabilitySig)
				cur.capabilitySig = sig
			}
		} else {
			cur.Status = unknownStatus(cur)
		}
		m.mu.Unlock()

		// P3-6a：单节点投影失败不应中断整轮同步（其余节点仍应更新），
		// 只记日志；下一轮重试。
		if err := m.upsertPG(ctx, cur); err != nil {
			slog.Error("upsert node projection", "node", cur.NomadNodeID, "error", err)
		}
	}
	return nil
}

// Nodes 返回当前节点快照（只读副本）。
func (m *Manager) Nodes() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, *n)
	}
	return out
}

// ClientFor 返回节点 gRPC 客户端；不存在返回 nil。
func (m *Manager) ClientFor(nomadNodeID string) *agentclient.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[nomadNodeID]
}

// ClientForNodeID 同时接受 agent node ID 和 Nomad node ID。它只读本副本的
// discovery 快照，不访问或写入 PG。
func (m *Manager) ClientForNodeID(nodeID string) *agentclient.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for nomadID, n := range m.nodes {
		if nomadID == nodeID || agentNodeID(n) == nodeID {
			return m.conns[nomadID]
		}
	}
	return nil
}

// AgentRuntimeForMachine 为 API runtime 通道解析 machine 所在 agent 与能力。
// machine/node projection 仅从 PG 读取，连接来自本副本的只读 discovery；因此
// follower 可直接服务请求，且不会参与 leader-only observed 同步。
func (m *Manager) AgentRuntimeForMachine(
	ctx context.Context,
	machineID string,
) (*agentclient.Client, map[string]bool, error) {
	if m.cfg.Store == nil {
		return nil, nil, fmt.Errorf("node resolver store not configured")
	}
	machine, err := m.cfg.Store.GetMachine(ctx, machineID)
	if err != nil {
		return nil, nil, err
	}
	if machine == nil {
		return nil, nil, fmt.Errorf("machine %s not found", machineID)
	}
	nodes, err := m.cfg.Store.ListNodes(ctx)
	if err != nil {
		return nil, nil, err
	}
	// machines.node_id 通常保存 agent node ID，而 follower 不调用 ServiceInfo，
	// 因此用 leader 写入的 PG node projection 映射回 Nomad ID，再查本地连接池。
	lookupID := machine.NodeID
	var features map[string]bool
	for i := range nodes {
		if nodes[i].ID == machine.NodeID || nodes[i].NomadNodeID == machine.NodeID {
			if nodes[i].NomadNodeID != "" {
				lookupID = nodes[i].NomadNodeID
			}
			features = capabilities.SetOf(nodes[i].FeatureIDs)
			break
		}
	}
	client := m.ClientForNodeID(lookupID)
	if client == nil {
		return nil, nil, fmt.Errorf("no agent client for machine %s (node %s)", machineID, machine.NodeID)
	}
	return client, features, nil
}

// Close 关闭全部连接。
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, c := range m.conns {
		_ = c.Close()
		delete(m.conns, id)
	}
}

func (m *Manager) fetchNodes(ctx context.Context) ([]NomadNodeStub, error) {
	var stubs []NomadNodeStub
	if err := m.getJSON(ctx, "/v1/nodes", &stubs); err != nil {
		return nil, err
	}
	return stubs, nil
}

func (m *Manager) fetchAllocs(ctx context.Context) ([]NomadAllocStub, error) {
	var stubs []NomadAllocStub
	if err := m.getJSON(ctx, "/v1/job/"+url.PathEscape(m.cfg.JobName)+"/allocations", &stubs); err != nil {
		return nil, err
	}
	return stubs, nil
}

// fillAllocResources 从 /v1/allocation/{id} 详情补端口。
func (m *Manager) fillAllocResources(ctx context.Context, a *NomadAllocStub) error {
	var detail NomadAllocStub
	if err := m.getJSON(ctx, "/v1/allocation/"+url.PathEscape(a.ID), &detail); err != nil {
		return err
	}
	if detail.AllocatedResources != nil {
		a.AllocatedResources = detail.AllocatedResources
	}
	return nil
}

func (m *Manager) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.NomadAddr+path, nil)
	if err != nil {
		return fmt.Errorf("nomad request %s: %w", path, err)
	}
	resp, err := m.http.Do(req)
	if err != nil {
		return fmt.Errorf("nomad %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("nomad %s: status %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("nomad %s decode: %w", path, err)
	}
	return nil
}

// markUnknown 把快照内全部节点置 UNKNOWN（保留 Info/地址）。
func (m *Manager) markUnknown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range m.nodes {
		n.Status = "UNKNOWN"
	}
}

func (m *Manager) ensureConnLocked(nomadID, grpcAddr string) {
	if c, ok := m.conns[nomadID]; ok {
		if c.Addr() == grpcAddr {
			return
		}
		_ = c.Close()
		delete(m.conns, nomadID)
	}
	client, err := agentclient.Dial(grpcAddr)
	if err != nil {
		slog.Error("dial agent", "node", nomadID, "addr", grpcAddr, "error", err)
		return
	}
	m.conns[nomadID] = client
}

func (m *Manager) upsertPG(ctx context.Context, n *Node) error {
	if m.cfg.Store == nil {
		return nil
	}
	row := store.Node{
		ID:          agentNodeID(n),
		NomadNodeID: n.NomadNodeID,
		NodePool:    n.NodePool,
		Status:      n.Status,
		Labels:      map[string]string{},
		GRPCAddr:    n.GRPCAddr,
		ProxyAddr:   n.ProxyAddr,
		LastSeenAt:  n.LastSeen,
	}
	if n.Info != nil {
		info := n.Info
		row.Labels = info.Labels
		if info.Capacity != nil {
			row.VCPUTotal = int64(info.Capacity.VcpuTotal)
			row.MemTotalMib = int64(info.Capacity.MemTotalMib)
			row.DiskTotalMib = int64(info.Capacity.DiskTotalMib)
		}
		if info.Usage != nil {
			row.CPUPercent = info.Usage.CpuPercent
			row.MemUsedMib = int64(info.Usage.MemUsedMib)
			row.MemAllocatedMib = int64(info.Usage.MemAllocatedMib)
			row.DiskUsedMib = int64(info.Usage.DiskUsedMib)
		}
		// v1.1（ADR-0018）：仅以非空 agent 快照更新 PG cache。agent 的
		// ServiceInfo 无法区分“缓存确实为空”和 ListImages 失败后的空列表；
		// 保留旧值优先保证短暂 ListImages 失败不会抹掉亲和信息。
		if len(info.CachedImageDigests) > 0 {
			row.ImageCache = info.CachedImageDigests
		}
		// v1.2-A（ADR-0023）：能力投影随 ServiceInfo 同步落库。未知 feature
		// 原样保留（审计可解释），调度过滤只看集合成员关系。
		row.ProtocolVersion = info.ProtocolVersion
		row.FeatureIDs = info.FeatureIds
		row.SnapshotCompatibilityKey = info.SnapshotCompatibilityKey
	}
	return m.cfg.Store.UpsertNode(ctx, row)
}

func agentNodeID(n *Node) string {
	if n.Info != nil && n.Info.NodeId != "" {
		return n.Info.NodeId
	}
	return n.NomadNodeID
}

// capabilitySignature 是能力投影的紧凑签名（变化检测/审计日志用）。
func capabilitySignature(info *pb.ServiceInfoResponse) string {
	if info == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%v", info.ProtocolVersion, info.SnapshotCompatibilityKey, info.FeatureIds)
}

// nomadStatus 根据 Nomad 节点状态计算状态机初值。
func nomadStatus(n *Node) string {
	if !n.Ready || !n.Eligible {
		return "UNHEALTHY"
	}
	if n.Drain {
		return "DRAINING"
	}
	if n.GRPCAddr == "" {
		return "UNKNOWN"
	}
	return "UNKNOWN" // 未拉到 ServiceInfo 前调度器不采信
}

// combinedStatus 叠加 agent 报告状态；Nomad 侧 unhealthy/drain 优先。
func combinedStatus(n *Node, info *pb.ServiceInfoResponse) string {
	if !n.Ready || !n.Eligible {
		return "UNHEALTHY"
	}
	if n.Drain {
		return "DRAINING"
	}
	if n.GRPCAddr == "" {
		return "UNKNOWN"
	}
	if info.Status != pb.ServiceInfoResponse_HEALTHY {
		return "UNHEALTHY"
	}
	return "HEALTHY"
}

// unknownStatus 保留 Nomad 侧 unhealthy/drain 判定，其余退 UNKNOWN。
func unknownStatus(n *Node) string {
	if !n.Ready || !n.Eligible {
		return "UNHEALTHY"
	}
	if n.Drain {
		return "DRAINING"
	}
	return "UNKNOWN"
}

func allocPorts(a *NomadAllocStub) (grpc, proxy int) {
	if a.AllocatedResources == nil {
		return 0, 0
	}
	for _, p := range a.AllocatedResources.Shared.Ports {
		switch p.Label {
		case "grpc":
			grpc = p.Value
		case "proxy":
			proxy = p.Value
		}
	}
	return grpc, proxy
}

func nodeHost(stub *NomadNodeStub) (string, bool) {
	// Nomad's node list exposes the advertised client IP in Address. Prefer it
	// over HTTPAddr so a server-local HTTP endpoint cannot misroute agent RPCs.
	if ip := net.ParseIP(stub.Address); ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
		return ip.String(), true
	}
	if u, err := url.Parse("//" + stub.HTTPAddr); err == nil {
		host := u.Hostname()
		if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() {
			return ip.String(), true
		}
		if host != "" && host != "localhost" {
			return host, true
		}
	}
	return "", false
}
