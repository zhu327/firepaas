package server

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/zhu327/firepaas/internal/agent/mutation"
	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/state"
	contracts "github.com/zhu327/firepaas/internal/contracts/agentv1"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApplyFabric 实现 FabricService（ADR-0040 §18，T3）：
//
//	fencing 键 = node_id + fabric_generation + operation_id。
//	流程：契约校验 → 本节点身份核对 → 节点级 generation fence → ledger
//	durable claim → 全量快照替换（幂等）→ durable complete。
//
// 崩溃恢复：in-progress claim 的重试在同一 operation 下重跑 Effect——
// ApplyFabric 是全量替换，重放自然幂等；fabric 高水位随快照落盘一起
// 持久化，重启后旧代请求继续被拒。
func (s *Server) ApplyFabric(ctx context.Context, req *pb.ApplyFabricRequest) (*pb.ApplyFabricResponse, error) {
	if err := contracts.ValidateApplyFabricRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.fabric == nil {
		return nil, status.Error(codes.Unimplemented, "fabric service not enabled on this agent")
	}
	if s.info == nil || s.info.NodeID == "" {
		return nil, status.Error(codes.FailedPrecondition, "agent node identity unavailable")
	}
	if req.GetNodeId() != s.info.NodeID {
		return nil, status.Errorf(codes.FailedPrecondition,
			"fabric snapshot node %q does not match this agent %q", req.GetNodeId(), s.info.NodeID)
	}
	opID := req.GetOperationId()
	hash := hashRequest(req)
	// 重放信号：调用前 operation 已在 ledger（含 in-progress 崩溃重试）。
	_, existed, err := s.ledger.Get(opID, hash)
	if err != nil {
		return nil, mutationError(err)
	}
	out, err := mutation.RunFabricMutation(s.mutations, mutation.ClaimedMutation[*pb.ApplyFabricResponse]{
		Identity: mutation.Identity{
			OperationID: opID,
			Kind:        "fabric.apply",
			RequestHash: hash,
		},
		// 节点级串行化：同一把锁下完成 fence 检查 → claim → 快照替换，
		// 并发不同 operation 也不会乱序（低代先占锁则高代后应用；反之
		// 低代被 fence 拒绝，不留孤儿 claim）。
		SerializationKey: "fabric.node",
		Fence:            func() error { return s.fabric.CheckSnapshot(snapshotFromRequest(req)) },
		Effect: func() (*pb.ApplyFabricResponse, error) {
			if _, err := s.fabric.Apply(snapshotFromRequest(req)); err != nil {
				return nil, err
			}
			// fabric 状态先落盘、underlay 后生效：underlay 失败时 ledger claim
			// 保持 in-progress，重试同 operation 重跑幂等的 UpdatePeers 收敛。
			if s.underlay != nil {
				peers, err := apiPeersFromRequest(req)
				if err != nil {
					return nil, err
				}
				if err := s.underlay.UpdatePeers(ctx, req.GetFabricGeneration(), peers); err != nil {
					return nil, status.Errorf(codes.Internal, "underlay update: %v", err)
				}
			}
			// G2a（§15）：policy 快照全量替换（underlay 之后）。快照已是幂等
			// 全集，重试重放收敛；nil 后端（nft emergency）跳过。
			if s.fabricPolicy != nil {
				if err := s.fabricPolicy.ApplyFabricPolicy(ctx, fabricPolicyFromSnapshot(req)); err != nil {
					return nil, status.Errorf(codes.Internal, "fabric policy apply: %v", err)
				}
			}
			return &pb.ApplyFabricResponse{
				AppliedGeneration: req.GetFabricGeneration(),
				Replayed:          existed,
			}, nil
		},
		Codec: protoCodec(func() *pb.ApplyFabricResponse { return &pb.ApplyFabricResponse{} }),
	})
	if err != nil {
		return nil, fabricApplyError(err, s.fabric)
	}
	// 已完成重放直接按存储结果返回时，补上 replay 信号。
	if existed {
		out.Replayed = true
	}
	return out, nil
}

// fabricApplyError 映射 ApplyFabric 失败：stale 水位拒绝时随 FailedPrecondition
// 附带 agent 当前已应用水位（gRPC status detail）。控制面据此一步跳到该水位 +1
// 重推，避免 DB 水位回退（备份恢复/丢更新）后逐代爬升造成的长窗口冻结；
// 其余错误仍走 mutationError 的统一映射。
func fabricApplyError(err error, fabric *state.Fabric) error {
	if errors.Is(err, state.ErrStaleFabricGeneration) && fabric != nil {
		applied := fabric.Current().Generation
		st, derr := status.New(codes.FailedPrecondition, err.Error()).
			WithDetails(&pb.FabricWatermarkDetail{AppliedGeneration: applied})
		if derr == nil {
			return st.Err()
		}
	}
	return mutationError(err)
}

// FabricIdentityView / FabricRuleView 是 policy 组装的中立输入：接收路径
// （pb 请求）与 agentd 重启重放（持久化快照）共用同一组装实现，避免两处
// 各自维护导致语义漂移。
type FabricIdentityView struct {
	IdentityID uint32
	ProjectID  string
	AppID      string
	Service    string
	ULA        string
}

type FabricRuleView struct {
	SrcProject string
	SrcApp     string
	DstProject string
	DstApp     string
	DstService string
	Ports      []uint32
}

// BuildFabricPolicy 由 identity/规则集合组装 eBPF policy 快照：
// ipcache = identities 全集（ULA→identity，v6 源绑定）；dst 按
// (project,app,service) 精确解析；src 按 app 展开到全部 service identity
// （规则只声明 src_app 的语义）；回程条目携带同一端口集并按源端口匹配。
// mesh_direct 与在役门控在控制面组装侧完成（未声明的 dst_service 规则不
// 会进快照），此处不重查（W3 分工：identity 跨 execution 共享，逐行声明
// 无法在 identity 粒度收紧）。
func BuildFabricPolicy(
	generation uint64,
	nodePrefix string,
	ids []FabricIdentityView,
	rules []FabricRuleView,
) api.FabricPolicySnapshot {
	snap := api.FabricPolicySnapshot{
		Generation: generation,
		Identities: make([]api.IdentityMapEntry, 0, len(ids)),
	}
	// 本节点自身 ULA = NodePrefix /64 基址（wg 设备地址）：平台流量放行。
	if p, err := netip.ParsePrefix(nodePrefix); err == nil {
		snap.NodeULA = p.Addr().String()
	}
	type identityKey struct {
		project, app, service string
	}
	type appKey struct {
		project, app string
	}
	// identity 按 (project, app, service) 解析：service 是策略粒度的真实
	// 一维，按 (project, app) 折叠会让同 app 的多个 service 互相覆盖
	// （规则落到最后遍历到的 identity，过多/过少授权）。src 侧规则只声明
	// src_app，语义是“该 app 任一声明 service 都可发起”，故按 app 聚合同
	// project/app 下的全部 identity。
	byService := make(map[identityKey]uint32, len(ids))
	byApp := make(map[appKey][]uint32, len(ids))
	for _, m := range ids {
		snap.Identities = append(snap.Identities, api.IdentityMapEntry{
			IdentityID: m.IdentityID,
			ULA:        m.ULA,
		})
		byService[identityKey{m.ProjectID, m.AppID, m.Service}] = m.IdentityID
		ak := appKey{m.ProjectID, m.AppID}
		byApp[ak] = append(byApp[ak], m.IdentityID)
	}
	for _, r := range rules {
		dstID, ok := byService[identityKey{r.DstProject, r.DstApp, r.DstService}]
		if !ok {
			// dst 身份不在本节点/未部署：条目无从命中，跳过（规则仍随快照
			// 持久化，dst 上线后下一代快照再生效）。
			continue
		}
		for _, srcID := range byApp[appKey{r.SrcProject, r.SrcApp}] {
			snap.Entries = append(snap.Entries, api.FabricPolicyEntry{
				SrcIdentity: srcID,
				DstIdentity: dstID,
				Generation:  generation,
				Ports:       append([]uint32(nil), r.Ports...),
			})
			// 对称回程条目：跨节点时请求在 src 节点、回包在 dst 节点分别过
			// eBPF，回程授权无法靠运行时流表（节点间不共享 map），所以回程
			// 必须是静态条目。条目携带同一端口集但按**源端口**校验
			// （SrcPorts），把回程从“任意端口”收窄到声明服务端口；反向
			// 新建 TCP 仍被纯 SYN 的 dport 检查拒绝。
			snap.Entries = append(snap.Entries, api.FabricPolicyEntry{
				SrcIdentity: dstID,
				DstIdentity: srcID,
				Generation:  generation,
				Ports:       append([]uint32(nil), r.Ports...),
				SrcPorts:    true,
			})
		}
	}
	return snap
}

// FabricPolicyFromState 把持久化 fabric 快照转换为重放输入（agentd 重启
// 后重播 ipcache/policy；与接收路径共用 BuildFabricPolicy）。
func FabricPolicyFromState(snap state.FabricSnapshot) api.FabricPolicySnapshot {
	ids := make([]FabricIdentityView, 0, len(snap.Identities))
	for _, m := range snap.Identities {
		ids = append(ids, FabricIdentityView{
			IdentityID: m.IdentityID, ProjectID: m.ProjectID, AppID: m.AppID,
			Service: m.Service, ULA: m.ULA,
		})
	}
	rules := make([]FabricRuleView, 0, len(snap.EastWest))
	for _, r := range snap.EastWest {
		rules = append(rules, FabricRuleView{
			SrcProject: r.SrcProject, SrcApp: r.SrcApp,
			DstProject: r.DstProject, DstApp: r.DstApp, DstService: r.DstService,
			Ports: append([]uint32(nil), r.Ports...),
		})
	}
	return BuildFabricPolicy(snap.Generation, snap.NodePrefix, ids, rules)
}

// fabricPolicyFromSnapshot 把契约请求解析为 policy 组装输入并构建快照。
// ipcache = identities 全集（ULA→identity，v6 源绑定）；policy 条目 =
// EastWest 规则 ∧ dst 身份存在（dst 上线前跳过，上线后下一代快照生效）。
// “本服务可被直连”半边由控制面组装侧裁决（buildEastWestSnapshot 按在役
// deployment 的 mesh_direct 剪裁，未声明的 dst_service 规则不进快照）；
// 端口取规则集（逐端口白名单本身即 per-service 隔离）。
// 同 app 自连（src/dst 同身份）放行全部声明端口：EastWest 委托语义。
func fabricPolicyFromSnapshot(req *pb.ApplyFabricRequest) api.FabricPolicySnapshot {
	ids := make([]FabricIdentityView, 0, len(req.GetIdentities()))
	for _, m := range req.GetIdentities() {
		ids = append(ids, FabricIdentityView{
			IdentityID: m.GetIdentityId(), ProjectID: m.GetProjectId(), AppID: m.GetAppId(),
			Service: m.GetService(), ULA: m.GetUla(),
		})
	}
	var rules []FabricRuleView
	if ew := req.GetEastwest(); ew != nil {
		rules = make([]FabricRuleView, 0, len(ew.GetRules()))
		for _, r := range ew.GetRules() {
			rules = append(rules, FabricRuleView{
				SrcProject: r.GetSrcProject(), SrcApp: r.GetSrcApp(),
				DstProject: r.GetDstProject(), DstApp: r.GetDstApp(), DstService: r.GetDstService(),
				Ports: append([]uint32(nil), r.GetPorts()...),
			})
		}
	}
	return BuildFabricPolicy(req.GetFabricGeneration(), req.GetNodePrefix(), ids, rules)
}

// apiPeersFromRequest 把契约 peer 集转换为 api.Peer（underlay 消费）。
func apiPeersFromRequest(req *pb.ApplyFabricRequest) ([]api.Peer, error) {
	out := make([]api.Peer, 0, len(req.GetPeers()))
	for _, p := range req.GetPeers() {
		prefix, err := netip.ParsePrefix(p.GetNodePrefix())
		if err != nil {
			return nil, fmt.Errorf("peer %s node_prefix %q: %w", p.GetNodeId(), p.GetNodePrefix(), err)
		}
		out = append(out, api.Peer{
			NodeID:           p.GetNodeId(),
			Pubkey:           p.GetPubkey(),
			Endpoint:         p.GetEndpoint(),
			NodePrefix:       prefix,
			FabricGeneration: p.GetFabricGeneration(),
		})
	}
	return out, nil
}

// snapshotFromRequest 把契约快照转换为持久化形态（不含任何秘密材料：
// peers 只有公钥/endpoint，identities 只有公开身份坐标）。
func snapshotFromRequest(req *pb.ApplyFabricRequest) state.FabricSnapshot {
	snap := state.FabricSnapshot{
		NodeID:     req.GetNodeId(),
		Generation: req.GetFabricGeneration(),
		NodePrefix: req.GetNodePrefix(),
	}
	for _, p := range req.GetPeers() {
		snap.Peers = append(snap.Peers, state.FabricPeer{
			NodeID:           p.GetNodeId(),
			Pubkey:           p.GetPubkey(),
			Endpoint:         p.GetEndpoint(),
			NodePrefix:       p.GetNodePrefix(),
			FabricGeneration: p.GetFabricGeneration(),
		})
	}
	for _, m := range req.GetIdentities() {
		snap.Identities = append(snap.Identities, state.FabricIdentity{
			IdentityID:  m.GetIdentityId(),
			TrustDomain: m.GetTrustDomain(),
			ProjectID:   m.GetProjectId(),
			AppID:       m.GetAppId(),
			Service:     m.GetService(),
			ULA:         m.GetUla(),
			MachineID:   m.GetMachineId(),
			ExecutionID: m.GetExecutionId(),
			Generation:  m.GetGeneration(),
			MeshDirect:  m.GetMeshDirect(),
		})
	}
	if ew := req.GetEastwest(); ew != nil {
		snap.EastWestGeneration = ew.GetGeneration()
		for _, r := range ew.GetRules() {
			snap.EastWest = append(snap.EastWest, state.EastWestRule{
				SrcProject: r.GetSrcProject(),
				SrcApp:     r.GetSrcApp(),
				DstProject: r.GetDstProject(),
				DstApp:     r.GetDstApp(),
				DstService: r.GetDstService(),
				Ports:      append([]uint32(nil), r.GetPorts()...),
			})
		}
	}
	// G2c（§16）：.internal 记录全量映射（nil/空 = 无记录）。
	for _, r := range req.GetDns() {
		snap.DNS = append(snap.DNS, state.DnsRecord{
			Name:       r.GetName(),
			AAAA:       append([]string(nil), r.GetAaaa()...),
			Generation: r.GetGeneration(),
		})
	}
	return snap
}
