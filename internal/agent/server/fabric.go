package server

import (
	"context"
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
		return nil, mutationError(err)
	}
	// 已完成重放直接按存储结果返回时，补上 replay 信号。
	if existed {
		out.Replayed = true
	}
	return out, nil
}

// fabricPolicyFromSnapshot 把契约请求解析为 eBPF policy 快照（G2a §15）。
// ipcache = identities 全集（ULA→identity，v6 源绑定）；policy 条目 =
// EastWest 规则 ∧ dst 身份存在（dst 上线前跳过，上线后下一代快照生效）。
// “本服务可被直连”半边由控制面组装侧裁决（buildEastWestSnapshot 按在役
// deployment 的 mesh_direct 剪裁，未声明的 dst_service 规则不进快照）；
// 端口取规则集（逐端口白名单本身即 per-service 隔离）。
// 同 app 自连（src/dst 同身份）放行全部声明端口：EastWest 委托语义。
func fabricPolicyFromSnapshot(req *pb.ApplyFabricRequest) api.FabricPolicySnapshot {
	snap := api.FabricPolicySnapshot{
		Generation: req.GetFabricGeneration(),
		Identities: make([]api.IdentityMapEntry, 0, len(req.GetIdentities())),
	}
	// 本节点自身 ULA = NodePrefix /64 基址（wg 设备地址）：平台流量放行。
	if p, err := netip.ParsePrefix(req.GetNodePrefix()); err == nil {
		snap.NodeULA = p.Addr().String()
	}
	type identityKey struct {
		project, app string
	}
	byCoord := make(map[identityKey]uint32, len(req.GetIdentities()))
	for _, m := range req.GetIdentities() {
		snap.Identities = append(snap.Identities, api.IdentityMapEntry{
			IdentityID: m.GetIdentityId(),
			ULA:        m.GetUla(),
		})
		key := identityKey{m.GetProjectId(), m.GetAppId()}
		byCoord[key] = m.GetIdentityId()
	}
	ew := req.GetEastwest()
	if ew == nil {
		return snap
	}
	for _, r := range ew.GetRules() {
		dstID, ok := byCoord[identityKey{r.GetDstProject(), r.GetDstApp()}]
		if !ok {
			// dst 身份不在本节点/未部署：条目无从命中，跳过（规则仍随快照
			// 持久化，dst 上线后下一代快照再生效）。
			continue
		}
		if srcID, ok := byCoord[identityKey{r.GetSrcProject(), r.GetSrcApp()}]; ok {
			snap.Entries = append(snap.Entries, api.FabricPolicyEntry{
				SrcIdentity: srcID,
				DstIdentity: dstID,
				Generation:  ew.GetGeneration(),
				Ports:       append([]uint32(nil), r.GetPorts()...),
			})
			// 对称回程条目（无 ports）：跨节点时请求在 src 节点、回包在 dst
			// 节点分别过 eBPF，回程授权无法靠运行时流表（节点间不共享 map）。
			// 纯 SYN 端口白名单只挂在正向条目上，对称条目只放行非发起包
			// （SYN-ACK/ACK/数据/ICMP）——反向新建 TCP 仍被端口检查拒。
			snap.Entries = append(snap.Entries, api.FabricPolicyEntry{
				SrcIdentity: dstID,
				DstIdentity: srcID,
				Generation:  ew.GetGeneration(),
			})
		}
	}
	return snap
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
