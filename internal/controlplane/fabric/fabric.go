// Package fabric 是控制面 fabric 下发协调器（ADR-0040 §18，T4b）：
// 周期读取 PG desired（wg_peers / ipam_allocations × workload_identities），
// 按节点组装 ApplyFabric 全量快照并经 nodemanager 连接池推送。
//
// 语义（与 agent 侧节点级 fencing 配套）：
//   - 每节点推送水位 = fabric_versions.generation，只升不降；
//   - 快照内容哈希不变 = 不推送；内容变化 → generation+1 推送；
//   - 推送失败（含响应丢失）→ 同 operation_id 重试（agent ledger 幂等）；
//   - agent 返回 FailedPrecondition（其水位高于本侧，外部干预/丢更新）→
//     本侧强制推进水位并置空内容哈希，下一轮以更高代重推，自动收敛；
//   - operation_id = fabric-<node>-<gen>-<hash[:16]>：内容变化即新 operation，
//     同内容重试即同 operation（幂等键）。
//
// 只在 leader 任期内运行（cmd/api 装配），与 RunServiceInfo 同一写者纪律。
package fabric

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/zhu327/firepaas/shared/pkg/ulanet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeSource 是节点视图与连接池的最小接口（nodemanager.Manager 满足）。
type NodeSource interface {
	Nodes() []nodemanager.Node
	ClientForNodeID(nodeID string) *agentclient.Client
}

// Config 是协调器运行参数。
// InternalDNSLister 是 .internal 投影的读取缝（G2c，ADR-0040 §16/§18：
// 发布器写 dns:internal:* Redis 键，reconciler 读出随 fabric 快照下发到
// 节点本地 DNS；agent 不碰 Redis）。nil = 不下发 DNS（灰度关）。
type InternalDNSLister interface {
	ListInternalDNS(ctx context.Context) ([]catalog.InternalDNSRecord, error)
}

type Config struct {
	Store       *store.Store
	NodeSource  NodeSource
	CellPrefix  string // 本 cell /40 ULA（RFC 4193 fd00::/8；节点 /64 从内分配）
	WgPort      uint16 // peer endpoint 端口（与 agent FIREPAAS_MESH_WG_PORT 同值）
	SyncEvery   time.Duration
	PushTimeout time.Duration
	// DNS（G2c）：.internal 记录源（Redis dns:internal:* 投影，publisher
	// 唯一写者）。nil = 快照不携带 DNS（节点本地 DNS 服空表）。
	DNS InternalDNSLister
	// EdgeHub（G2d，ADR-0040 §14）：edge hub 的 WG 注册（运维在 edge 侧生成
	// 密钥后配置公钥/endpoint）。注册后 edge 作为具名 peer 出现在全部节点
	// 的 WG AllowedIPs（peer 准入由 WG 加密承担）。Pubkey 空 = 不注册。
	EdgeHub EdgeHubConfig
	// IngressPort（G2d）：节点 fabric ingress 终结器端口（与 agent
	// FIREPAAS_AGENT_FABRIC_INGRESS_PORT 同值；写入 mesh:endpoint 投影）。
	IngressPort int
	// MeshProjection（G2d）：mesh:peer / mesh:endpoint Redis 投影写者
	//（edge 消费）。nil = 不写投影（edge 无直达寻址，回落 legacy）。
	MeshProjection MeshProjectionWriter
}

// EdgeHubConfig 是 edge hub 的 WG 对等注册（G2d；§14 “具名单独身份的
// WG peer，与 workload 身份域隔离”——peer 身份域即 WG 密钥域，不占用
// workload identity ID 空间）。
type EdgeHubConfig struct {
	NodeID   string // 稳定 ID（默认 "edge-hub"；不与 agent node_id 冲突）
	Pubkey   string // edge 侧 wg 公钥（base64；无秘密）
	Endpoint string // edge WG 监听 host:port
}

// MeshProjectionWriter 是 mesh 寻址投影写者缝（catalog.Catalog 实现）。
type MeshProjectionWriter interface {
	ReplaceMeshProjection(
		ctx context.Context,
		peers []catalog.MeshPeerRecord,
		endpoints []catalog.MeshEndpointRecord,
		ttl time.Duration,
	) error
}

// Reconciler 执行周期推送（单线程顺序执行；dnsCache 等轮内状态无需加锁）。
type Reconciler struct {
	cfg  Config
	cell netip.Prefix
	// dnsCache：per-node 上次成功读取的 DNS 全表（W2 serve-stale：Redis
	// 抖动时用缓存继续推送全量快照，避免空表闪断全网 .internal；注释见 syncNode）。
	dnsCache map[string][]*pb.DnsRecord
}

// New 校验配置并构造（cell 必须为规范化 /40 ULA）。
func New(cfg Config) (*Reconciler, error) {
	if cfg.Store == nil {
		return nil, errors.New("fabric: Store is required")
	}
	if cfg.NodeSource == nil {
		return nil, errors.New("fabric: NodeSource is required")
	}
	cell, err := ulanet.ValidatePrefix(cfg.CellPrefix, 40)
	if err != nil {
		return nil, fmt.Errorf("fabric: cell_prefix: %w", err)
	}
	r := &Reconciler{cfg: cfg, cell: cell, dnsCache: map[string][]*pb.DnsRecord{}}
	if r.cfg.WgPort == 0 {
		r.cfg.WgPort = 51820
	}
	if r.cfg.SyncEvery == 0 {
		r.cfg.SyncEvery = 10 * time.Second
	}
	if r.cfg.PushTimeout == 0 {
		r.cfg.PushTimeout = 5 * time.Second
	}
	// G2d：edge hub 注册与 ingress 端口默认值。
	if r.cfg.EdgeHub.NodeID == "" {
		r.cfg.EdgeHub.NodeID = "edge-hub"
	}
	if r.cfg.EdgeHub.Pubkey != "" {
		if _, err := base64.StdEncoding.DecodeString(strings.TrimSpace(r.cfg.EdgeHub.Pubkey)); err != nil {
			return nil, fmt.Errorf("fabric: edge hub pubkey: %w", err)
		}
		if _, _, err := net.SplitHostPort(r.cfg.EdgeHub.Endpoint); err != nil {
			return nil, fmt.Errorf("fabric: edge hub endpoint: %w", err)
		}
	} else {
		r.cfg.EdgeHub = EdgeHubConfig{} // 半配置（仅 endpoint 无 pubkey）视为未注册
	}
	if r.cfg.IngressPort == 0 {
		r.cfg.IngressPort = 5109
	}
	return r, nil
}

// Run 立即执行一轮，之后按 SyncEvery 周期执行直到 ctx 取消。
func (r *Reconciler) Run(ctx context.Context) error {
	_ = r.Sync(ctx)
	ticker := time.NewTicker(r.cfg.SyncEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.Sync(ctx); err != nil {
				slog.Error("fabric sync", "error", err)
			}
		}
	}
}

// roundSnapshot 一轮同步内全节点复用的快照源（W2：逐节点三全表 N+1 改为
// 每轮各一次；peer 集仅差“排除自身”，syncNode 内内存过滤）。
type roundSnapshot struct {
	peers      []store.WGPeer
	identities []store.FabricIdentityRow
	eastwest   *pb.EastWestPolicySpec
	direct     *store.MeshDirectIndex
}

// nodeItem 是一轮同步中已完成注册的节点推送项。
type nodeItem struct {
	node   nodemanager.Node
	client *agentclient.Client
	prefix netip.Prefix
}

// Sync 执行一轮全节点推送，分两阶段：
//  1. 注册阶段：为所有已入 mesh 节点确保 wg_peers 行（分配 node /64、
//     轮换换代）——先全部注册，后组装的快照 peer 集才是完整视图；
//  2. 推送阶段：按节点组装全量快照并 ApplyFabric。
//
// 单节点错误降级（记日志、下轮重试），不中断整轮。
func (r *Reconciler) Sync(ctx context.Context) error {
	var items []nodeItem
	for _, n := range r.cfg.NodeSource.Nodes() {
		if n.Info == nil || n.Info.NodeId == "" {
			continue
		}
		client := r.cfg.NodeSource.ClientForNodeID(n.Info.NodeId)
		if client == nil {
			continue
		}
		// mesh 未启用的节点不上报公钥：不注册、不推送（只在 mesh 里的
		// 节点参与 fabric）。
		if n.Info.FabricPubkey == "" {
			continue
		}
		// WG 端口优先取节点 observed 上报（同主机多节点/异构端口）；
		// 未上报（旧 agent）回落控制面全局默认。
		wgPort := r.cfg.WgPort
		if n.Info.GetFabricWgPort() > 0 && n.Info.GetFabricWgPort() <= 65535 {
			wgPort = uint16(n.Info.GetFabricWgPort())
		}
		endpoint, err := meshEndpoint(n.GRPCAddr, wgPort)
		if err != nil {
			slog.Warn("fabric sync node endpoint", "node", n.Info.NodeId, "error", err)
			continue
		}
		prefix, _, err := r.cfg.Store.EnsureNodeFabric(
			ctx, r.cell, n.Info.NodeId, n.Info.FabricPubkey, endpoint)
		if err != nil {
			slog.Warn("fabric sync node register", "node", n.Info.NodeId, "error", err)
			continue
		}
		items = append(items, nodeItem{node: n, client: client, prefix: prefix})
	}
	// G2d（§14）：edge hub 注册为具名 WG peer（与 workload 身份域隔离：
	// peer 身份域 = WG 密钥域，不占 workload identity ID）。注册后随
	// wg_peers 进入全部节点快照的 peer 集（AllowedIPs = edge /64，peer
	// 准入由 WG 加密承担）。
	if r.cfg.EdgeHub.Pubkey != "" {
		if _, _, err := r.cfg.Store.EnsureNodeFabric(ctx, r.cell,
			r.cfg.EdgeHub.NodeID, r.cfg.EdgeHub.Pubkey, r.cfg.EdgeHub.Endpoint); err != nil {
			slog.Warn("fabric sync edge hub register", "node", r.cfg.EdgeHub.NodeID, "error", err)
		}
	}
	// W2：整轮快照源一次读取（逐节点三全表 N+1 → 每轮一次）。任一失败则整轮
	// 跳过推送（下轮重试；单节点 Apply 失败仍逐节点降级，见 syncNode）。
	round := roundSnapshot{}
	if peers, err := r.cfg.Store.ListWGPeers(ctx); err != nil {
		return fmt.Errorf("fabric round peers: %w", err)
	} else {
		round.peers = peers
	}
	if identities, err := r.cfg.Store.FabricIdentities(ctx); err != nil {
		return fmt.Errorf("fabric round identities: %w", err)
	} else {
		round.identities = identities
	}
	if policies, err := r.cfg.Store.ListEastWestPolicies(ctx); err != nil {
		return fmt.Errorf("fabric round policies: %w", err)
	} else {
		direct, derr := r.cfg.Store.MeshDirectIndex(ctx)
		if derr != nil {
			return fmt.Errorf("fabric round mesh-direct index: %w", derr)
		}
		round.direct = direct
		round.eastwest = buildEastWestSnapshot(policies, direct.Services)
	}
	for _, it := range items {
		if err := r.syncNode(ctx, it, round); err != nil {
			slog.Warn("fabric sync node", "node", it.node.Info.NodeId, "error", err)
		}
	}
	// 剪掉本轮不在 mesh 的节点的 DNS 缓存（重注册后按新内容重建）。
	if len(r.dnsCache) > 0 {
		keep := make(map[string]bool, len(items))
		for _, it := range items {
			keep[it.node.Info.NodeId] = true
		}
		for id := range r.dnsCache {
			if !keep[id] {
				delete(r.dnsCache, id)
			}
		}
	}
	// G2d：mesh 寻址投影（edge 消费；写失败降级记日志——edge 回落 legacy
	// 路径，不阻断快照下发）。复用整轮快照源，不再重查。
	if r.cfg.MeshProjection != nil {
		if err := r.publishMeshProjection(ctx, round); err != nil {
			slog.Warn("fabric mesh projection", "error", err)
		}
	}
	return nil
}

// publishMeshProjection 写 mesh:peer（节点 WG 寻址，edge 侧 WG peer 集）
// 与 mesh:endpoint（execution 直达入口）投影。endpoint 仅含在役分配的
// 寻址事实；READY 门控在 route/backend 层（edge 只对已在 route 的 backend
// 查询 endpoint）。
func (r *Reconciler) publishMeshProjection(ctx context.Context, round roundSnapshot) error {
	peers := round.peers
	peerRecs := make([]catalog.MeshPeerRecord, 0, len(peers))
	nodeULA := make(map[string]string, len(peers))
	for _, p := range peers {
		// edge hub 自身不发布 endpoint（它不是 workload 宿主）；但作为
		// peer 正常发布（节点侧需要它的 AllowedIPs）。
		peerRecs = append(peerRecs, catalog.MeshPeerRecord{
			NodeID: p.NodeID, Pubkey: p.Pubkey, Endpoint: p.Endpoint,
			NodePrefix: p.NodePrefix.String(),
		})
		if p.NodeID != r.cfg.EdgeHub.NodeID {
			nodeULA[p.NodeID] = p.NodePrefix.Addr().String()
		}
	}
	ids := round.identities
	epRecs := make([]catalog.MeshEndpointRecord, 0, len(ids))
	for _, id := range ids {
		ula, ok := nodeULA[id.NodeID]
		if !ok {
			continue // 节点已不在 mesh：不发布直达入口（edge 回落）
		}
		// W3（§15/§17）：execution 任一 service 未声明 mesh_direct 不发布
		// 直达入口（与 route backend ULA 提示的 per-service 门控同向；
		// READY 门控仍在 route/backend 层——edge 只对已在 route 的 backend
		// 查询 endpoint，投影本身是寻址事实）。
		if round.direct == nil || !round.direct.Machines[id.MachineID] {
			continue
		}
		epRecs = append(epRecs, catalog.MeshEndpointRecord{
			MachineID: id.MachineID, ExecutionID: id.ExecutionID,
			NodeID: id.NodeID, NodeULA: ula, IngressPort: r.cfg.IngressPort,
			WorkloadULA: id.ULA.String(), Generation: id.Generation,
		})
	}
	return r.cfg.MeshProjection.ReplaceMeshProjection(ctx, peerRecs, epRecs, meshProjectionTTL)
}

// meshProjectionTTL 与 dns:internal 同源的 serve-stale 预算（§16/§18）。
const meshProjectionTTL = 120 * time.Second

func (r *Reconciler) syncNode(ctx context.Context, it nodeItem, round roundSnapshot) error {
	nodeID := it.node.Info.NodeId
	prefix := it.prefix
	peers := round.peers
	identities := round.identities
	// G2a：东西向放行全表已随整轮快照装配（空表 = nil 默认 deny）。
	eastwest := round.eastwest
	// G2c：.internal 记录（Redis 投影，发布器唯一写者）。W2 serve-stale：
	// 读失败用本节点上次成功内容继续推送全量快照（哈希不变则不推；缓存无
	// 内容（首轮即失败）才推空表——与“无 DNS”不可区分，重建后收敛）。
	// 空表闪断全网 .internal 的代价远大于一轮（秒级）策略/peer 延迟，故不
	// 跳过推送（B P1-2：读失败推空表曾造成全网闪断；此处推缓存=显式 serve-stale）。
	var dnsRecords []*pb.DnsRecord
	if r.cfg.DNS != nil {
		records, derr := r.cfg.DNS.ListInternalDNS(ctx)
		if derr != nil {
			slog.Warn("fabric dns projection unavailable, serving last-known snapshot",
				"node", nodeID, "error", derr)
			dnsRecords = r.dnsCache[nodeID]
		} else {
			dnsRecords = make([]*pb.DnsRecord, 0, len(records))
			for _, rec := range records {
				dnsRecords = append(dnsRecords, &pb.DnsRecord{
					Name: rec.Name, Aaaa: append([]string(nil), rec.AAAA...),
					Generation: uint64(rec.Generation),
				})
			}
			r.dnsCache[nodeID] = dnsRecords
		}
	}
	// 快照 peer 集 = 其他已入 mesh 节点（自身与未上报公钥的节点排除）。
	// 哈希与请求同源过滤，保证「内容未变不推送」精确对应请求内容。
	meshPeers := make([]store.WGPeer, 0, len(peers))
	for _, p := range peers {
		if p.NodeID != nodeID && p.Pubkey != "" {
			meshPeers = append(meshPeers, p)
		}
	}
	hash := snapshotHash(prefix, meshPeers, identities, eastwest, dnsRecords)
	version, lastHash, err := r.cfg.Store.FabricVersion(ctx, nodeID)
	if err != nil {
		return err
	}
	if version > 0 && lastHash == hash {
		return nil // 内容未变：不推送
	}
	gen := version + 1
	req := &pb.ApplyFabricRequest{
		NodeId:           nodeID,
		FabricGeneration: uint64(gen),
		OperationId:      operationID(nodeID, gen, hash),
		NodePrefix:       prefix.String(),
	}
	for _, p := range meshPeers {
		req.Peers = append(req.Peers, &pb.FabricPeer{
			NodeId:           p.NodeID,
			Pubkey:           p.Pubkey,
			Endpoint:         p.Endpoint,
			NodePrefix:       p.NodePrefix.String(),
			FabricGeneration: uint64(p.FabricGeneration),
		})
	}
	for _, id := range identities {
		req.Identities = append(req.Identities, &pb.IdentityMapping{
			IdentityId:  id.IdentityID,
			TrustDomain: id.TrustDomain,
			ProjectId:   id.ProjectID,
			AppId:       id.AppID,
			Service:     id.Service,
			Ula:         id.ULA.String(),
			MachineId:   id.MachineID,
			ExecutionId: id.ExecutionID,
			Generation:  uint64(id.Generation),
			MeshDirect:  id.MeshDirect,
		})
	}
	req.Eastwest = eastwest
	req.Dns = dnsRecords

	pushCtx, cancel := context.WithTimeout(ctx, r.cfg.PushTimeout)
	resp, err := it.client.ApplyFabric(pushCtx, req)
	cancel()
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			// agent 水位高于本侧（丢更新/外部干预）：强制推进水位并清空
			// 内容哈希，下一轮以更高代重推，自动收敛。
			if aerr := r.cfg.Store.AdvanceFabricVersion(ctx, nodeID, gen+1, ""); aerr != nil {
				return fmt.Errorf("stale push, bump version: %w", aerr)
			}
			slog.Warn("fabric push rejected as stale; version bumped",
				"node", nodeID, "next_generation", gen+1)
			return fmt.Errorf("agent fabric watermark ahead of control plane: %v", err)
		}
		return fmt.Errorf("apply fabric: %w", err)
	}
	if resp.GetAppliedGeneration() != uint64(gen) {
		return fmt.Errorf("agent applied generation %d, want %d", resp.GetAppliedGeneration(), gen)
	}
	if err := r.cfg.Store.AdvanceFabricVersion(ctx, nodeID, gen, hash); err != nil {
		return fmt.Errorf("advance fabric version: %w", err)
	}
	slog.Info("fabric snapshot pushed", "node", nodeID, "generation", gen,
		"peers", len(req.Peers), "identities", len(req.Identities),
		"eastwest_rules", len(req.GetEastwest().GetRules()), "dns_records", len(req.GetDns()))
	return nil
}

// buildEastWestSnapshot 把 PG 全表转为快照形态（nil = 空表）。规则已按
// (src,app,dst,app,service) 稳定排序（store ORDER BY），哈希与请求同源。
// W3（§15 粒度统一）：只收录 dst_service 在直连声明索引内的规则——
// “本服务可被直连”半边在组装侧裁决（publisher 的 per-machine 判定在快照层面
// 的最近似；在役 machine 的 deployment 任一声明即收录，dst 身份缺席时 agent
// 侧仍跳过）。未声明的 dst_service 规则缺席 = 默认 deny（fail closed）。
func buildEastWestSnapshot(policies []store.EastWestPolicy, direct map[string]bool) *pb.EastWestPolicySpec {
	if len(policies) == 0 {
		return nil
	}
	out := &pb.EastWestPolicySpec{}
	for _, p := range policies {
		ports := uint32Ports(p.Ports)
		if len(ports) == 0 {
			// 脏行（无有效端口）跳过而非毒化整个快照：缺席 = 默认 deny，
			// 失败方向安全（fail-closed per-rule，而非整快照被 agent 拒收）。
			slog.Warn("fabric eastwest rule has no valid ports, skipped",
				"src", p.SrcProject+"/"+p.SrcApp,
				"dst", p.DstProject+"/"+p.DstApp+"/"+p.DstService)
			continue
		}
		if !direct[store.MeshServiceKey(p.DstProject, p.DstApp, p.DstService)] {
			// dst service 未声明 mesh_direct（§15 缺一不可）：规则不进快照。
			continue
		}
		if p.Generation > 0 && uint64(p.Generation) > out.Generation {
			out.Generation = uint64(p.Generation)
		}
		out.Rules = append(out.Rules, &pb.EastWestPolicyRule{
			SrcProject: p.SrcProject,
			SrcApp:     p.SrcApp,
			DstProject: p.DstProject,
			DstApp:     p.DstApp,
			DstService: p.DstService,
			Ports:      ports,
		})
	}
	if len(out.Rules) == 0 {
		return nil
	}
	if out.Generation == 0 {
		out.Generation = 1
	}
	return out
}

func uint32Ports(in []int32) []uint32 {
	out := make([]uint32, 0, len(in))
	for _, p := range in {
		if p > 0 {
			out = append(out, uint32(p))
		}
	}
	return out
}

// meshEndpoint 从节点 gRPC 地址取主机名拼接 WG 端口（peer endpoint =
// agent 可路由主机:wg 端口）。
func meshEndpoint(grpcAddr string, wgPort uint16) (string, error) {
	host := ""
	if grpcAddr != "" {
		if h, _, err := net.SplitHostPort(grpcAddr); err == nil {
			host = h
		}
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return "", fmt.Errorf("cannot derive mesh endpoint from grpc addr %q", grpcAddr)
	}
	return net.JoinHostPort(host, strconv.Itoa(int(wgPort))), nil
}

// snapshotHash 计算快照内容哈希（确定性：peers/identities 均按稳定键排序，
// 与 ListWGPeers / FabricIdentities 的 ORDER BY 同口径）。哈希只用
// 于变更检测与幂等键派生，不承载任何秘密。
func snapshotHash(
	prefix netip.Prefix,
	peers []store.WGPeer,
	identities []store.FabricIdentityRow,
	eastwest *pb.EastWestPolicySpec,
	dns []*pb.DnsRecord,
) string {
	type peerHash struct {
		NodeID           string `json:"node_id"`
		Pubkey           string `json:"pubkey"`
		Endpoint         string `json:"endpoint"`
		NodePrefix       string `json:"node_prefix"`
		FabricGeneration int64  `json:"fabric_generation"`
	}
	type identityHash struct {
		IdentityID  uint32 `json:"identity_id"`
		TrustDomain string `json:"trust_domain"`
		ProjectID   string `json:"project_id"`
		AppID       string `json:"app_id"`
		Service     string `json:"service"`
		ULA         string `json:"ula"`
		MachineID   string `json:"machine_id"`
		ExecutionID string `json:"execution_id"`
		Generation  int64  `json:"generation"`
		MeshDirect  bool   `json:"mesh_direct"`
	}
	type ruleHash struct {
		SrcProject string   `json:"src_project"`
		SrcApp     string   `json:"src_app"`
		DstProject string   `json:"dst_project"`
		DstApp     string   `json:"dst_app"`
		DstService string   `json:"dst_service"`
		Ports      []uint32 `json:"ports"`
	}
	type dnsHash struct {
		Name       string   `json:"name"`
		AAAA       []string `json:"aaaa"`
		Generation uint64   `json:"generation"`
	}
	type input struct {
		NodePrefix  string         `json:"node_prefix"`
		Peers       []peerHash     `json:"peers"`
		Identities  []identityHash `json:"identities"`
		EastWest    []ruleHash     `json:"eastwest,omitempty"`
		EastWestGen uint64         `json:"eastwest_generation,omitempty"`
		DNS         []dnsHash      `json:"dns,omitempty"`
	}
	in := input{NodePrefix: prefix.String()}
	for _, p := range peers {
		in.Peers = append(in.Peers, peerHash{
			NodeID: p.NodeID, Pubkey: p.Pubkey, Endpoint: p.Endpoint,
			NodePrefix: p.NodePrefix.String(), FabricGeneration: p.FabricGeneration,
		})
	}
	for _, id := range identities {
		in.Identities = append(in.Identities, identityHash{
			IdentityID: id.IdentityID, TrustDomain: id.TrustDomain, ProjectID: id.ProjectID,
			AppID: id.AppID, Service: id.Service, ULA: id.ULA.String(),
			MachineID: id.MachineID, ExecutionID: id.ExecutionID, Generation: id.Generation,
			MeshDirect: id.MeshDirect,
		})
	}
	if eastwest != nil {
		in.EastWestGen = eastwest.GetGeneration()
		for _, r := range eastwest.GetRules() {
			in.EastWest = append(in.EastWest, ruleHash{
				SrcProject: r.GetSrcProject(), SrcApp: r.GetSrcApp(),
				DstProject: r.GetDstProject(), DstApp: r.GetDstApp(),
				DstService: r.GetDstService(), Ports: append([]uint32(nil), r.GetPorts()...),
			})
		}
	}
	for _, d := range dns {
		in.DNS = append(in.DNS, dnsHash{
			Name: d.GetName(), AAAA: append([]string(nil), d.GetAaaa()...),
			Generation: d.GetGeneration(),
		})
	}
	raw, err := json.Marshal(in)
	if err != nil {
		// 纯文本结构体，Marshal 不会失败；防御性 fallback 不会掩盖逻辑错误。
		return fmt.Sprintf("hash-error-%v", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func operationID(nodeID string, gen int64, hash string) string {
	suffix := hash
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	return fmt.Sprintf("fabric-%s-%d-%s", nodeID, gen, suffix)
}
