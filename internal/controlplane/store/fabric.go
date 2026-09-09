// fabric.go：ADR-0040 G1/G2 网络 fabric 的 desired 事实访问（§18 四表）。
//
// 分层纪律（ADR-0003/ADR-0040 G0）：
//   - 本文件全部走 PG 事务，是 ULA/identity/peer/eastwest policy 的唯一权威；
//   - Redis 投影只能从这些方法的结果重建，不得反向写入或反推业务结论；
//   - 行级 fencing 水位（fabric_generation / generation）只升不降，旧值重放
//     在 store 层即拒绝（agent 侧节点级 fencing 见 T3/T4）。
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/zhu327/firepaas/shared/pkg/ulanet"
)

// ErrFabricGenerationStale 表示 fabric 变更携带的 generation 低于已持久化
// 水位（旧 generation 重放，fenced 拒绝）。
var ErrFabricGenerationStale = errors.New("fabric generation below persisted watermark")

// WorkloadIdentity 是稳定层 service 身份（ADR-0040 §6）。
type WorkloadIdentity struct {
	IdentityID  uint32
	TrustDomain string
	ProjectID   string
	AppID       string
	Service     string
}

// EnsureWorkloadIdentity 为 (project, app, service) 分配或复用稳定 identity_id。
// 身份跨 execution 稳定：同三元组返回既有 ID，绝不重发。identity_id 由 PG
// sequence 分配（uint32 域），可作 eBPF policy map key。
func (s *Store) EnsureWorkloadIdentity(
	ctx context.Context,
	trustDomain, projectID, appID, service string,
) (WorkloadIdentity, error) {
	var id uint32
	err := s.pool.QueryRow(ctx, `
		INSERT INTO workload_identities (identity_id, trust_domain, project_id, app_id, service)
		VALUES (nextval('workload_identity_ids'), $1, $2, $3, $4)
		ON CONFLICT (project_id, app_id, service) DO NOTHING
		RETURNING identity_id`,
		trustDomain, projectID, appID, service).Scan(&id)
	if err == nil {
		return WorkloadIdentity{
			IdentityID:  id,
			TrustDomain: trustDomain,
			ProjectID:   projectID,
			AppID:       appID,
			Service:     service,
		}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return WorkloadIdentity{}, fmt.Errorf("insert workload identity: %w", err)
	}
	// 并发分配撞唯一约束：读既有行（trust_domain 必须是同一值，否则报
	// 冲突——identity 的 trust domain 是业务身份的一部分，不可改属）。
	var got WorkloadIdentity
	err = s.pool.QueryRow(ctx, `
		SELECT identity_id, trust_domain, project_id, app_id, service
		FROM workload_identities
		WHERE project_id=$1 AND app_id=$2 AND service=$3`,
		projectID, appID, service).Scan(
		&got.IdentityID, &got.TrustDomain, &got.ProjectID, &got.AppID, &got.Service)
	if err != nil {
		return WorkloadIdentity{}, fmt.Errorf("read existing workload identity: %w", err)
	}
	if got.TrustDomain != trustDomain {
		return WorkloadIdentity{}, fmt.Errorf(
			"workload identity %s/%s/%s exists under trust domain %q, want %q",
			projectID, appID, service, got.TrustDomain, trustDomain)
	}
	return got, nil
}

// ListWorkloadIdentities 返回全部稳定身份（fabric 快照下发用）。
func (s *Store) ListWorkloadIdentities(ctx context.Context) ([]WorkloadIdentity, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT identity_id, trust_domain, project_id, app_id, service
		FROM workload_identities
		ORDER BY identity_id`)
	if err != nil {
		return nil, fmt.Errorf("list workload identities: %w", err)
	}
	defer rows.Close()
	var out []WorkloadIdentity
	for rows.Next() {
		var w WorkloadIdentity
		if err := rows.Scan(&w.IdentityID, &w.TrustDomain, &w.ProjectID, &w.AppID, &w.Service); err != nil {
			return nil, fmt.Errorf("scan workload identity: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ULAAllocation 是 ipam_allocations 表行。
type ULAAllocation struct {
	ULA         netip.Addr
	CellPrefix  netip.Prefix
	ProjectID   string
	NodeID      string
	MachineID   string
	ExecutionID string
	Generation  int64
}

// ErrFabricNodePinned：同 machine+execution 的 ULA 已分配在其它节点。ULA
// 在 execution 生命周期内不可变（ADR-0040 §7 稳定域），调用方必须回绑定
// 节点重试，不得换节点重分配（与已推送 fabric 快照竞争会导致 guest 地址
// 与库内不一致，跨节点东西向永不可达——真机 spike 实测）。
type ErrFabricNodePinned struct {
	NodeID string
	ULA    netip.Addr
}

func (e *ErrFabricNodePinned) Error() string {
	return fmt.Sprintf("fabric allocation pinned to node %s (ula %s)", e.NodeID, e.ULA)
}

// AllocateULA 在 node /64 内从低到高分配首个空闲 /128（PG 事务 + 唯一键
// 冲突收敛，多分配器并发安全）。cell /40 → project /48 → node /64 分层由
// 调用方规划，本方法校验 node_prefix 落于 cell_prefix 内。
func (s *Store) AllocateULA(
	ctx context.Context,
	cellPrefix, nodePrefix netip.Prefix,
	projectID, nodeID, machineID, executionID string,
	generation int64,
) (netip.Addr, error) {
	cell, err := canonicalULAPrefix(cellPrefix, 40)
	if err != nil {
		return netip.Addr{}, err
	}
	node, err := canonicalULAPrefix(nodePrefix, 64)
	if err != nil {
		return netip.Addr{}, err
	}
	if !cell.Contains(node.Addr()) {
		return netip.Addr{}, fmt.Errorf("node prefix %s outside cell prefix %s", node, cell)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("begin ula allocation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 确定性分配：node /64 内从 +1 起逐个尝试。两条 active 唯一索引仲裁并发：
	//   - ula 冲突（其他 execution 在用）→ 跳过候选；
	//   - (machine_id, execution_id) 冲突（同绑定的并发重放）→ 读并返回
	//     既有行，绝不产生一个 execution 两条在役 /128。
	// 每次插入走 savepoint：唯一键冲突会 abort 当前事务，跳过候选前必须
	// ROLLBACK TO SAVEPOINT 恢复。
	const maxAttempts = 4096
	for i := 0; i < maxAttempts; i++ {
		cand := nextAddr(node.Addr(), uint64(i+1))
		if !node.Contains(cand) {
			break
		}
		sp, err := tx.Begin(ctx)
		if err != nil {
			return netip.Addr{}, fmt.Errorf("begin ula allocation savepoint: %w", err)
		}
		var got netip.Addr
		err = sp.QueryRow(ctx, `
			INSERT INTO ipam_allocations
				(ula, cell_prefix, project_id, node_id, machine_id, execution_id, generation)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (ula) WHERE released_at IS NULL DO NOTHING
			RETURNING ula`,
			cand, cell, projectID, nodeID, machineID, executionID, generation).Scan(&got)
		if err == nil {
			if err := sp.Commit(ctx); err != nil {
				return netip.Addr{}, fmt.Errorf("commit ula allocation savepoint: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return netip.Addr{}, fmt.Errorf("commit ula allocation: %w", err)
			}
			return got, nil
		}
		if err := sp.Rollback(ctx); err != nil {
			return netip.Addr{}, fmt.Errorf("rollback ula allocation savepoint: %w", err)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			continue // 候选 /128 已被在役行占用
		}
		if isUniqueViolation(err) && hasConstraint(err, "ipam_allocations_machine_exec_active_idx") {
			// 同 machine+execution 的并发重放已先落一行：复用它（幂等收敛）。
			var existing netip.Addr
			var existingNode string
			err := tx.QueryRow(ctx, `
				SELECT ula, node_id FROM ipam_allocations
				WHERE machine_id=$1 AND execution_id=$2 AND released_at IS NULL`,
				machineID, executionID).Scan(&existing, &existingNode)
			if err != nil {
				return netip.Addr{}, fmt.Errorf("read concurrent ula allocation: %w", err)
			}
			if existingNode == nodeID {
				if err := tx.Commit(ctx); err != nil {
					return netip.Addr{}, fmt.Errorf("commit ula reuse: %w", err)
				}
				return existing, nil
			}
			// ULA 在 execution 生命周期内不可变（ADR-0040 §7）：已在别的节点
			// 分配过 → 调用方必须回那台节点重试，不得在本节点重分配（与已
			// 推送快照竞争会让 guest 拿到与库不一致的旧地址，跨节点东西向
			// 因此永不可达——真机 spike 实测）。
			return netip.Addr{}, &ErrFabricNodePinned{NodeID: existingNode, ULA: existing}
		}
		return netip.Addr{}, fmt.Errorf("insert ula %s: %w", cand, err)
	}
	return netip.Addr{}, fmt.Errorf("no free ULA in node prefix %s (tried %d candidates)", node, maxAttempts)
}

// ReleaseULA 释放 machine+execution 的在役 /128（置 released_at，行保留）。
func (s *Store) ReleaseULA(ctx context.Context, machineID, executionID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE ipam_allocations SET released_at = now()
		WHERE machine_id=$1 AND execution_id=$2 AND released_at IS NULL`,
		machineID, executionID)
	if err != nil {
		return fmt.Errorf("release ula: %w", err)
	}
	return nil
}

// ListActiveULAs 返回 node 或 project 的在役 /128（参数为空表示不限）。
func (s *Store) ListActiveULAs(ctx context.Context, nodeID, projectID string) ([]ULAAllocation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ula, cell_prefix, project_id, node_id, machine_id, execution_id, generation
		FROM ipam_allocations
		WHERE released_at IS NULL
		  AND ($1 = '' OR node_id = $1)
		  AND ($2 = '' OR project_id = $2)
		ORDER BY ula`,
		nodeID, projectID)
	if err != nil {
		return nil, fmt.Errorf("list ula allocations: %w", err)
	}
	defer rows.Close()
	return scanULAs(rows)
}

func scanULAs(rows pgx.Rows) ([]ULAAllocation, error) {
	var out []ULAAllocation
	for rows.Next() {
		var a ULAAllocation
		if err := rows.Scan(&a.ULA, &a.CellPrefix, &a.ProjectID, &a.NodeID,
			&a.MachineID, &a.ExecutionID, &a.Generation); err != nil {
			return nil, fmt.Errorf("scan ula allocation: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// WGPeer 是 wg_peers 表行（ADR-0040 §9：peer 数 = 节点数 + 网关/edge hub 数）。
type WGPeer struct {
	NodeID           string
	Pubkey           string
	Endpoint         string
	NodePrefix       netip.Prefix
	FabricGeneration int64
}

// UpsertWGPeer 写入/更新节点 WG peer。fabric_generation 只升不降；同
// generation 携带不同内容（密钥/endpoint/prefix）同样拒绝。原子 upsert：
// 行不存在与并发插入都在同一语句内仲裁，无 check-then-act 竞态。
func (s *Store) UpsertWGPeer(ctx context.Context, p WGPeer) error {
	var nodeID string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO wg_peers (node_id, pubkey, endpoint, node_prefix, fabric_generation)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (node_id) DO UPDATE SET
			pubkey = EXCLUDED.pubkey,
			endpoint = EXCLUDED.endpoint,
			node_prefix = EXCLUDED.node_prefix,
			fabric_generation = EXCLUDED.fabric_generation,
			updated_at = now()
		WHERE wg_peers.fabric_generation < EXCLUDED.fabric_generation
		   OR (wg_peers.fabric_generation = EXCLUDED.fabric_generation
		       AND wg_peers.pubkey IS NOT DISTINCT FROM EXCLUDED.pubkey
		       AND wg_peers.endpoint IS NOT DISTINCT FROM EXCLUDED.endpoint
		       AND wg_peers.node_prefix IS NOT DISTINCT FROM EXCLUDED.node_prefix)
		RETURNING node_id`,
		p.NodeID, p.Pubkey, p.Endpoint, p.NodePrefix, p.FabricGeneration).Scan(&nodeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: node %s (requested generation %d)",
			ErrFabricGenerationStale, p.NodeID, p.FabricGeneration)
	}
	if err != nil {
		return fmt.Errorf("upsert wg peer: %w", err)
	}
	return nil
}

// ListWGPeers 返回全部 WG peer 行。
func (s *Store) ListWGPeers(ctx context.Context) ([]WGPeer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, pubkey, endpoint, node_prefix, fabric_generation
		FROM wg_peers ORDER BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("list wg peers: %w", err)
	}
	defer rows.Close()
	var out []WGPeer
	for rows.Next() {
		var p WGPeer
		if err := rows.Scan(&p.NodeID, &p.Pubkey, &p.Endpoint, &p.NodePrefix, &p.FabricGeneration); err != nil {
			return nil, fmt.Errorf("scan wg peer: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// EastWestPolicy 是 eastwest_policies 表行（ADR-0040 §15；G2 前无业务行）。
type EastWestPolicy struct {
	SrcProject string
	SrcApp     string
	DstProject string
	DstApp     string
	DstService string
	Ports      []int32
	Generation int64
}

// MeshDirectIndex 是 mesh 直连声明的整轮索引（W3 §15 粒度统一）：
//   - Services：proj\x00app\x00service → true（任一在役 machine 的 deployment
//     声明该 service mesh_direct；publisher 的 per-machine 判定在快照层面的
//     最近似——规则生效仍需 dst 身份存在，agent 侧跳过无身份 dst）；
//   - Machines：至少一个 service direct 的在役 machine（endpoint 投影门控用；
//     ingress 按 credential 终结，与 service 无关，machine 粒度足够）。
//
// 只读 deployments 的在役引用（machines.deployment_id），legacy 无 services
// 声明的 deployment 自然不在索引内（未声明 = 不直连）。
type MeshDirectIndex struct {
	Services map[string]bool
	Machines map[string]bool
}

// MeshServiceKey 是直连声明索引的 service 键（proj\x00app\x00svc）。
func MeshServiceKey(project, app, service string) string {
	return project + "\x00" + app + "\x00" + service
}

// MeshDirectIndex 返回在役 mesh 直连声明索引（reconciler 每轮一次）。
func (s *Store) MeshDirectIndex(ctx context.Context) (*MeshDirectIndex, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT a.project_id, m.app_id, m.id, svc->>'name',
			COALESCE((svc->>'mesh_direct')::boolean, false)
		FROM machines m
		JOIN apps a ON a.id = m.app_id
		JOIN deployments d ON d.id = m.deployment_id,
		LATERAL jsonb_array_elements(COALESCE(d.services, '[]'::jsonb)) AS svc`)
	if err != nil {
		return nil, fmt.Errorf("mesh direct index: %w", err)
	}
	defer rows.Close()
	out := &MeshDirectIndex{
		Services: map[string]bool{},
		Machines: map[string]bool{},
	}
	for rows.Next() {
		var project, app, machine, service string
		var direct bool
		if err := rows.Scan(&project, &app, &machine, &service, &direct); err != nil {
			return nil, fmt.Errorf("scan mesh direct index: %w", err)
		}
		if service == "" || !direct {
			continue
		}
		out.Services[MeshServiceKey(project, app, service)] = true
		out.Machines[machine] = true
	}
	return out, rows.Err()
}

// generation 不同 ports 同样拒绝。原子 upsert，无 check-then-act 竞态。
func (s *Store) PutEastWestPolicy(ctx context.Context, p EastWestPolicy) error {
	var srcProject string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO eastwest_policies
			(src_project, src_app, dst_project, dst_app, dst_service, ports, generation)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (src_project, src_app, dst_project, dst_app, dst_service) DO UPDATE SET
			ports = EXCLUDED.ports,
			generation = EXCLUDED.generation,
			updated_at = now()
		WHERE eastwest_policies.generation < EXCLUDED.generation
		   OR (eastwest_policies.generation = EXCLUDED.generation
		       AND eastwest_policies.ports IS NOT DISTINCT FROM EXCLUDED.ports)
		RETURNING src_project`,
		p.SrcProject, p.SrcApp, p.DstProject, p.DstApp, p.DstService, p.Ports, p.Generation).Scan(&srcProject)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: eastwest %s/%s→%s/%s/%s (requested generation %d)",
			ErrFabricGenerationStale, p.SrcProject, p.SrcApp, p.DstProject, p.DstApp, p.DstService, p.Generation)
	}
	if err != nil {
		return fmt.Errorf("upsert eastwest policy: %w", err)
	}
	return nil
}

// ListEastWestPolicies 返回全部东西向规则。
func (s *Store) ListEastWestPolicies(ctx context.Context) ([]EastWestPolicy, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT src_project, src_app, dst_project, dst_app, dst_service, ports, generation
		FROM eastwest_policies ORDER BY src_project, src_app, dst_project, dst_app, dst_service`)
	if err != nil {
		return nil, fmt.Errorf("list eastwest policies: %w", err)
	}
	defer rows.Close()
	var out []EastWestPolicy
	for rows.Next() {
		var p EastWestPolicy
		if err := rows.Scan(&p.SrcProject, &p.SrcApp, &p.DstProject, &p.DstApp,
			&p.DstService, &p.Ports, &p.Generation); err != nil {
			return nil, fmt.Errorf("scan eastwest policy: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeleteEastWestPolicy 删除一条东西向规则。generation fencing：请求水位低于
// 行内已应用 generation 时删除不生效并返回 ErrFabricGenerationStale；行不存在
// 视为幂等成功。
func (s *Store) DeleteEastWestPolicy(ctx context.Context, p EastWestPolicy) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM eastwest_policies
		WHERE src_project=$1 AND src_app=$2 AND dst_project=$3 AND dst_app=$4 AND dst_service=$5
		  AND generation <= $6`,
		p.SrcProject, p.SrcApp, p.DstProject, p.DstApp, p.DstService, p.Generation)
	if err != nil {
		return fmt.Errorf("delete eastwest policy: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	var current int64
	err = s.pool.QueryRow(ctx, `
		SELECT generation FROM eastwest_policies
		WHERE src_project=$1 AND src_app=$2 AND dst_project=$3 AND dst_app=$4 AND dst_service=$5`,
		p.SrcProject, p.SrcApp, p.DstProject, p.DstApp, p.DstService).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // 行不存在：幂等
	}
	if err != nil {
		return fmt.Errorf("read eastwest policy watermark: %w", err)
	}
	return fmt.Errorf("%w: eastwest %s/%s→%s/%s/%s applied %d > requested %d",
		ErrFabricGenerationStale, p.SrcProject, p.SrcApp, p.DstProject, p.DstApp, p.DstService,
		current, p.Generation)
}

// canonicalULAPrefix 校验并规范化 ULA 前缀（单一实现 shared/pkg/ulanet：
// RFC 4193 fd00::/8、位宽、host bits 必须为 0）。
func canonicalULAPrefix(prefix netip.Prefix, bits int) (netip.Prefix, error) {
	return ulanet.CanonicalizePrefix(prefix, bits)
}

// hasConstraint 报告 err 是否携带指定约束名的唯一键冲突（23505）。
func hasConstraint(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.ConstraintName == constraint
}

// nextAddr 返回 base 之后第 offset 个地址（确定性低到高分配）。
func nextAddr(base netip.Addr, offset uint64) netip.Addr {
	b := base.As16()
	for i := 15; i >= 0 && offset > 0; i-- {
		sum := uint64(b[i]) + offset
		b[i] = byte(sum)
		offset = sum >> 8
	}
	return netip.AddrFrom16(b)
}

// EnsureNodeFabric 注册/更新节点 fabric 身份并返回其 node /64 与行 generation
// （ADR-0040 §9，T4b）。事务内：
//   - 行存在且 pubkey/endpoint 未变 → 幂等返回既有前缀与 generation；
//   - pubkey/endpoint 变化（密钥轮换/地址迁移）→ generation+1（旧代重放拒）；
//   - 行不存在 → 从 cell /40 低地址扫描分配首个空闲 /64（唯一索引
//     wg_peers_node_prefix_active_idx 仲裁并发分配）。
//
// node_id 稳定：前缀一经分配不随 pubkey/endpoint 变更（换 IP 不换策略）。
func (s *Store) EnsureNodeFabric(
	ctx context.Context,
	cellPrefix netip.Prefix,
	nodeID, pubkey, endpoint string,
) (netip.Prefix, int64, error) {
	cell, err := canonicalULAPrefix(cellPrefix, 40)
	if err != nil {
		return netip.Prefix{}, 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return netip.Prefix{}, 0, fmt.Errorf("begin node fabric ensure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var prefix netip.Prefix
	var gen int64
	var existingPubkey, existingEndpoint string
	err = tx.QueryRow(ctx, `
		SELECT node_prefix, fabric_generation, pubkey, endpoint
		FROM wg_peers WHERE node_id=$1 FOR UPDATE`, nodeID).Scan(
		&prefix, &gen, &existingPubkey, &existingEndpoint)
	switch {
	case err == nil:
		if existingPubkey != pubkey || existingEndpoint != endpoint {
			gen++
			if _, err := tx.Exec(ctx, `
				UPDATE wg_peers SET pubkey=$2, endpoint=$3, fabric_generation=$4, updated_at=now()
				WHERE node_id=$1`, nodeID, pubkey, endpoint, gen); err != nil {
				return netip.Prefix{}, 0, fmt.Errorf("update node fabric: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return netip.Prefix{}, 0, fmt.Errorf("commit node fabric ensure: %w", err)
		}
		return prefix, gen, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return netip.Prefix{}, 0, fmt.Errorf("read node fabric: %w", err)
	}

	// 新节点：cell /40 内确定性低地址分配。每次插入走 savepoint（唯一键
	// 冲突 abort 事务，跳过候选前必须恢复）。子网索引占 bits 40..63。
	const maxCandidates = 65535
	for i := 0; i < maxCandidates; i++ {
		cand := nextSubnetAddr(cell.Addr(), uint64(i+1))
		if !cell.Contains(cand) {
			break
		}
		sp, err := tx.Begin(ctx)
		if err != nil {
			return netip.Prefix{}, 0, fmt.Errorf("begin node fabric savepoint: %w", err)
		}
		var got netip.Prefix
		err = sp.QueryRow(ctx, `
			INSERT INTO wg_peers (node_id, pubkey, endpoint, node_prefix, fabric_generation)
			VALUES ($1, $2, $3, $4, 1)
			RETURNING node_prefix`, nodeID, pubkey, endpoint, netip.PrefixFrom(cand, 64)).Scan(&got)
		if err == nil {
			if err := sp.Commit(ctx); err != nil {
				return netip.Prefix{}, 0, fmt.Errorf("commit node fabric savepoint: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return netip.Prefix{}, 0, fmt.Errorf("commit node fabric insert: %w", err)
			}
			return got, 1, nil
		}
		if err := sp.Rollback(ctx); err != nil {
			return netip.Prefix{}, 0, fmt.Errorf("rollback node fabric savepoint: %w", err)
		}
		if isUniqueViolation(err) && hasConstraint(err, "wg_peers_pkey") {
			// 并发注册同一 node：读并返回先落盘者的行（幂等收敛）。
			var existing netip.Prefix
			var existingGen int64
			err := tx.QueryRow(ctx, `
				SELECT node_prefix, fabric_generation FROM wg_peers WHERE node_id=$1`,
				nodeID).Scan(&existing, &existingGen)
			if err != nil {
				return netip.Prefix{}, 0, fmt.Errorf("read concurrent node fabric: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return netip.Prefix{}, 0, fmt.Errorf("commit concurrent node fabric: %w", err)
			}
			return existing, existingGen, nil
		}
		if isUniqueViolation(err) && hasConstraint(err, "wg_peers_node_prefix_active_idx") {
			continue // 候选 /64 已被其他节点占用
		}
		return netip.Prefix{}, 0, fmt.Errorf("insert node fabric %s: %w", cand, err)
	}
	return netip.Prefix{}, 0, fmt.Errorf("no free node /64 in cell %s (tried %d)", cell, maxCandidates)
}

// DeleteWGPeer 下线节点 fabric 身份（运维退役/重装改名场景）：删除 wg_peers
// 行与其推送水位。下一轮 reconciler 因 peer 集变化（快照哈希改变）自动向
// 全节点 gen+1 重推，死 peer 路由与密钥准入随之摘除（§9 peer 集只含在役节点）。
//
// 不做自动 GC（见 controller sweeper 注释）：短暂失联的节点若被自动摘除后
// 重注册，可能分到新 /64 造成全网重编号抖动；退役必须是显式运维动作。
func (s *Store) DeleteWGPeer(ctx context.Context, nodeID string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID); err != nil {
		return fmt.Errorf("delete wg peer: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, nodeID); err != nil {
		return fmt.Errorf("delete fabric version: %w", err)
	}
	return nil
}

// nextSubnetAddr 返回 cell base 之后的第 n 个 /64 子网起始地址（子网索引
// 占 bits 40..63；调用方保证 n < 2^24）。nextAddr 是 /128 步进，不适用于
// /64 子网枚举。
func nextSubnetAddr(base netip.Addr, n uint64) netip.Addr {
	b := base.As16()
	b[5] = byte(n >> 16)
	b[6] = byte(n >> 8)
	b[7] = byte(n)
	return netip.AddrFrom16(b)
}

// FabricIdentityRow 是 fabric 快照 identity 集合的一行（ipam × machines ×
// deployments × workload_identities 连接产物，ADR-0040 §6/§7）。
type FabricIdentityRow struct {
	IdentityID  uint32
	TrustDomain string
	ProjectID   string
	AppID       string
	Service     string
	ULA         netip.Addr
	MachineID   string
	ExecutionID string
	Generation  int64
	// MeshDirect 是本 service 行的直连声明（G2a，deployments.services 同名
	// 条 mesh_direct；缺省 false）。与 Service 名同源（LATERAL unnest），
	// 保持 MeshDirectIndex/快照 SQL 三方同口径。
	MeshDirect bool
	// NodeID 是分配所在节点（G2d：mesh:endpoint 投影需要节点 ULA 寻址；
	// 与 wg_peers.node_id 同源）。
	NodeID string
}

// FabricIdentities 返回 mesh 内全部在役 execution 的 ULA↔identity 映射
// （集群全域；G2a 起快照不再按节点裁剪：dst 节点的 eBPF 需要远端 src 的
// ipcache 源绑定与 policy 键解析，跨节点东西向才能闭环）。
// 逐 service 出行（LATERAL unnest，与 MeshDirectIndex/EffectiveServices 同
// 口径；空 services = 单 'default' 行）：IdentityMapping 的 service 键与
// eastwest/route 侧 per-service 门控同源（P1 独立评审：services->0 只读首
// 条会让多 service 部署的次服务映射错位）。
// 在役分配找不到稳定身份 → 跳过该行并 Warn（分配先于身份落库的竞态/脏数据
// 不得熔断整轮；调用方 fabric reconciler 任一源失败即整轮跳过，单行坏数据
// 不能有整轮杀伤力；publisher 侧本就降级处理）。
func (s *Store) FabricIdentities(ctx context.Context) ([]FabricIdentityRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.identity_id, w.trust_domain, a.project_id, m.app_id,
		       COALESCE(svc->>'name', 'default'),
		       COALESCE((svc->>'mesh_direct')::boolean, false),
		       a.ula, a.machine_id, a.execution_id, a.generation, a.node_id
		FROM ipam_allocations a
		JOIN machines m ON m.id = a.machine_id
		JOIN deployments d ON d.id = m.deployment_id
		LEFT JOIN LATERAL jsonb_array_elements(
			CASE WHEN COALESCE(d.services, '[]'::jsonb) = '[]'::jsonb
				THEN '[{"name":"default"}]'::jsonb
				ELSE d.services END) AS svc ON true
		LEFT JOIN workload_identities w
		  ON w.project_id = a.project_id
		 AND w.app_id = m.app_id
		 AND w.service = COALESCE(svc->>'name', 'default')
		WHERE a.released_at IS NULL
		ORDER BY a.machine_id, a.execution_id, svc->>'name'`)
	if err != nil {
		return nil, fmt.Errorf("list fabric identities: %w", err)
	}
	defer rows.Close()
	var out []FabricIdentityRow
	for rows.Next() {
		var id FabricIdentityRow
		var identityID *uint32
		var trustDomain *string
		if err := rows.Scan(&identityID, &trustDomain, &id.ProjectID, &id.AppID, &id.Service,
			&id.MeshDirect, &id.ULA, &id.MachineID, &id.ExecutionID, &id.Generation, &id.NodeID); err != nil {
			return nil, fmt.Errorf("scan fabric identity: %w", err)
		}
		if identityID == nil || trustDomain == nil {
			slog.Warn("fabric identity missing, skipping allocation (no whole-round abort)",
				"project", id.ProjectID, "app", id.AppID,
				"machine", id.MachineID, "execution", id.ExecutionID)
			continue
		}
		id.IdentityID = *identityID
		id.TrustDomain = *trustDomain
		out = append(out, id)
	}
	return out, rows.Err()
}

// FabricVersion 读取节点推送水位（generation 与上次 snapshot_hash；无记录
// = 0 / 空串）。
func (s *Store) FabricVersion(ctx context.Context, nodeID string) (int64, string, error) {
	var gen int64
	var hash string
	err := s.pool.QueryRow(ctx, `
		SELECT generation, snapshot_hash FROM fabric_versions WHERE node_id=$1`, nodeID).Scan(&gen, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("read fabric version: %w", err)
	}
	return gen, hash, nil
}

// AdvanceFabricVersion 原子推进推送水位：generation 只升不降（fencing），
// 同代幂等。行不存在时插入。
// AdvanceFabricVersion 推进节点推送水位（只升不降：同代覆写拒绝，stale 自愈
// 走更高代）。agent 侧对同代异内容拒绝（snapshotEqual），控制面侧同代覆写
// 会制造“agent 拒收、控制面以为已推”的分叉，故同代不同内容在此直接拒绝
// （ErrFabricGenerationStale），调用方以 gen+1 重试（见 reconciler stale 路径）。
func (s *Store) AdvanceFabricVersion(ctx context.Context, nodeID string, generation int64, hash string) error {
	var got int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO fabric_versions (node_id, generation, snapshot_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (node_id) DO UPDATE SET
			generation = EXCLUDED.generation,
			snapshot_hash = EXCLUDED.snapshot_hash,
			updated_at = now()
		WHERE fabric_versions.generation < EXCLUDED.generation
		RETURNING generation`, nodeID, generation, hash).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: node %s version above %d", ErrFabricGenerationStale, nodeID, generation)
	}
	if err != nil {
		return fmt.Errorf("advance fabric version: %w", err)
	}
	return nil
}
