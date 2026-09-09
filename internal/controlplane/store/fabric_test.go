package store

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"testing"
)

// Fabric 测试用固定前缀（RFC 4193 合法 ULA 占位；正式值由部署生成）。
var (
	fabricCell = netip.MustParsePrefix("fd7a:9a55::/40")
	fabricNode = netip.MustParsePrefix("fd7a:9a55:1::/64")
)

func TestWorkloadIdentityStableAcrossExecutions(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project, app := "p-id-"+suffix, "app-id-"+suffix

	w1, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", project, app, "api")
	if err != nil {
		t.Fatal(err)
	}
	if w1.IdentityID == 0 || w1.ProjectID != project || w1.AppID != app || w1.Service != "api" {
		t.Fatalf("identity = %+v", w1)
	}
	// 跨 execution 稳定：同三元组复用同一 identity_id，绝不重发。
	w2, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", project, app, "api")
	if err != nil {
		t.Fatal(err)
	}
	if w2.IdentityID != w1.IdentityID {
		t.Fatalf("identity_id changed across calls: %d → %d", w1.IdentityID, w2.IdentityID)
	}
	// 不同 service 得到不同 ID。
	w3, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", project, app, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if w3.IdentityID == w1.IdentityID {
		t.Fatalf("distinct services share identity_id %d", w3.IdentityID)
	}
	// trust domain 冲突必须拒绝（身份不可改属）。
	if _, err := s.EnsureWorkloadIdentity(ctx, "other.local", project, app, "api"); err == nil {
		t.Fatal("trust domain conflict must fail")
	}
}

func TestAllocateULAIsDeterministicIdempotentAndFenced(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-ula-" + suffix
	nodeID := "node-ula-" + suffix
	machineID := "m-ula-" + suffix
	machines := []string{machineID, "m-ula2-" + suffix, "m-ula3-" + suffix}
	t.Cleanup(func() {
		for _, m := range machines {
			_, _ = s.Pool().Exec(ctx, `DELETE FROM ipam_allocations WHERE machine_id=$1`, m)
		}
	})

	// 同 machine+execution 重放幂等：返回同一 /128。
	a1, err := s.AllocateULA(ctx, fabricCell, fabricNode, project, nodeID,
		machineID, "exec-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !fabricNode.Contains(a1) || a1 == fabricNode.Addr() {
		t.Fatalf("allocated %s outside node prefix %s", a1, fabricNode)
	}
	a1b, err := s.AllocateULA(ctx, fabricCell, fabricNode, project, nodeID,
		machineID, "exec-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if a1b != a1 {
		t.Fatalf("replay returned different ULA: %s vs %s", a1b, a1)
	}

	// 确定性分配：下一 /128 是低地址方向的下一个。
	a2, err := s.AllocateULA(ctx, fabricCell, fabricNode, project, nodeID,
		machines[1], "exec-2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !a1.Less(a2) {
		t.Fatalf("second allocation %s must be above first %s", a2, a1)
	}

	// Release 后行保留（released_at 置位），地址可回收（低地址优先复用 a1）。
	if err := s.ReleaseULA(ctx, machineID, "exec-1"); err != nil {
		t.Fatal(err)
	}
	a3, err := s.AllocateULA(ctx, fabricCell, fabricNode, project, nodeID,
		machines[2], "exec-3", 1)
	if err != nil {
		t.Fatal(err)
	}
	if a3 != a1 {
		t.Fatalf("released address not recycled deterministically: got %s want %s", a3, a1)
	}

	listed, err := s.ListActiveULAs(ctx, nodeID, "")
	if err != nil {
		t.Fatal(err)
	}
	// exec-1 已释放：在役 = exec-2 与 exec-3（回收后的 ::1）。
	if len(listed) != 2 {
		t.Fatalf("active ULAs = %d, want 2", len(listed))
	}
	listed2, err := s.ListActiveULAs(ctx, "", project)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed2) != 2 {
		t.Fatalf("project active ULAs = %d, want 2", len(listed2))
	}
}

func TestAllocateULARejectsBadHierarchy(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	cases := []struct {
		name string
		cell netip.Prefix
		node netip.Prefix
	}{
		{"node outside cell", fabricCell, netip.MustParsePrefix("fd7a:9a56:1::/64")},
		{"cell not /40", netip.MustParsePrefix("fd7a:9a55::/48"), fabricNode},
		{"node not /64", fabricCell, netip.MustParsePrefix("fd7a:9a55:1::/56")},
		{"non ULA", fabricCell, netip.MustParsePrefix("2001:db8:1::/64")},
		{"fc00 reserved", fabricCell, netip.MustParsePrefix("fc00:1::/64")},
		{"host bits set", fabricCell, netip.MustParsePrefix("fd7a:9a55:1::5/64")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.AllocateULA(ctx, tc.cell, tc.node, "p", "n", "m", "e", 1); err == nil {
				t.Fatal("invalid hierarchy must fail")
			}
		})
	}
}

func TestWGPeerGenerationFencing(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	nodeID := "node-wg-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID)
	})

	p1 := WGPeer{
		NodeID: nodeID, Pubkey: "pk-old", Endpoint: "10.0.0.1:51820",
		NodePrefix: fabricNode, FabricGeneration: 5,
	}
	if err := s.UpsertWGPeer(ctx, p1); err != nil {
		t.Fatal(err)
	}
	// 旧 generation 重放拒绝。
	replay := p1
	replay.FabricGeneration = 4
	if err := s.UpsertWGPeer(ctx, replay); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("stale replay err = %v, want ErrFabricGenerationStale", err)
	}
	// 同 generation 不同内容（密钥材料变化）同样拒绝。
	sameGen := p1
	sameGen.Pubkey = "pk-sneaky"
	if err := s.UpsertWGPeer(ctx, sameGen); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("same-generation different content err = %v, want ErrFabricGenerationStale", err)
	}
	// 同 generation 同内容幂等。
	if err := s.UpsertWGPeer(ctx, p1); err != nil {
		t.Fatalf("idempotent replay rejected: %v", err)
	}
	// 新 generation 替换成功（密钥轮换 = 全量替换）。
	p2 := p1
	p2.Pubkey = "pk-new"
	p2.FabricGeneration = 6
	if err := s.UpsertWGPeer(ctx, p2); err != nil {
		t.Fatal(err)
	}
	peers, err := s.ListWGPeers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine *WGPeer
	for i := range peers {
		if peers[i].NodeID == nodeID {
			mine = &peers[i]
		}
	}
	if mine == nil || mine.Pubkey != "pk-new" || mine.FabricGeneration != 6 {
		t.Fatalf("peers = %+v", peers)
	}
}

func TestEastWestPolicyGenerationFencingAndRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	p := EastWestPolicy{
		SrcProject: "src-" + suffix, SrcApp: "web",
		DstProject: "dst-" + suffix, DstApp: "db", DstService: "pg",
		Ports: []int32{5432}, Generation: 1,
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx,
			`DELETE FROM eastwest_policies WHERE src_project=$1 OR dst_project=$1`, p.SrcProject)
	})
	if err := s.PutEastWestPolicy(ctx, p); err != nil {
		t.Fatal(err)
	}
	stale := p
	stale.Ports = []int32{5432, 6432}
	stale.Generation = 0
	if err := s.PutEastWestPolicy(ctx, stale); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("stale policy err = %v, want ErrFabricGenerationStale", err)
	}
	fresh := p
	fresh.Ports = []int32{5432, 6432}
	fresh.Generation = 2
	if err := s.PutEastWestPolicy(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	// 低 generation 删除不生效（fencing 删除，全量替换协调器才可删）。
	if err := s.DeleteEastWestPolicy(ctx, stale); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("stale delete err = %v, want ErrFabricGenerationStale", err)
	}
	got, err := s.ListEastWestPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var mine *EastWestPolicy
	for i := range got {
		if got[i].SrcProject == p.SrcProject {
			mine = &got[i]
		}
	}
	if mine == nil || len(mine.Ports) != 2 || mine.Generation != 2 {
		t.Fatalf("policies = %+v", got)
	}
	// 当前水位删除生效且幂等。
	if err := s.DeleteEastWestPolicy(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteEastWestPolicy(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListEastWestPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range got {
		if row.SrcProject == p.SrcProject {
			t.Fatalf("policy survived delete: %+v", got)
		}
	}
}

// TestAllocateULAConcurrentReplayConverges：同 machine+execution 的并发重放
// 只能产生一条在役 /128（active 唯一索引仲裁 + 冲突读既有行收敛）。
func TestAllocateULAConcurrentReplayConverges(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	machineID := "m-ula-conc-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM ipam_allocations WHERE machine_id=$1`, machineID)
	})

	const workers = 8
	results := make(chan netip.Addr, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			a, err := s.AllocateULA(ctx, fabricCell, fabricNode, "p-conc-"+suffix,
				"node-conc-"+suffix, machineID, "exec-conc", 1)
			if err != nil {
				errs <- err
				return
			}
			results <- a
		}()
	}
	seen := map[netip.Addr]bool{}
	for i := 0; i < workers; i++ {
		select {
		case a := <-results:
			seen[a] = true
		case err := <-errs:
			t.Fatalf("concurrent allocation failed: %v", err)
		}
	}
	if len(seen) != 1 {
		t.Fatalf("concurrent replay produced %d distinct ULAs: %v", len(seen), seen)
	}
	active, err := s.ListActiveULAs(ctx, "node-conc-"+suffix, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].MachineID != machineID {
		t.Fatalf("active allocations = %+v", active)
	}
}

func TestEnsureNodeFabricAllocatesStablePrefixAndRotates(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	nodeA := "node-a-" + suffix
	nodeB := "node-b-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id IN ($1,$2)`, nodeA, nodeB)
	})

	// 新节点：cell 内可用 /64（序号取决于库内既有节点——共享 lab 库），
	// generation=1。
	pA, genA, err := s.EnsureNodeFabric(ctx, fabricCell, nodeA, "pk-a", "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	if !fabricCell.Contains(pA.Addr()) || pA.Bits() != 64 || genA != 1 {
		t.Fatalf("node A = %s gen %d (cell %s)", pA, genA, fabricCell)
	}
	// 幂等：同内容不换代。
	pA2, genA2, err := s.EnsureNodeFabric(ctx, fabricCell, nodeA, "pk-a", "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	if pA2 != pA || genA2 != 1 {
		t.Fatalf("idempotent = %s gen %d, want %s gen 1", pA2, genA2, pA)
	}
	// 密钥轮换 → generation+1，前缀不变（换 IP 不换策略）。
	_, genRot, err := s.EnsureNodeFabric(ctx, fabricCell, nodeA, "pk-a-rotated", "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	if genRot != 2 {
		t.Fatalf("rotation gen = %d, want 2", genRot)
	}
	// endpoint 迁移 → 再换代。
	_, genMig, err := s.EnsureNodeFabric(ctx, fabricCell, nodeA, "pk-a-rotated", "10.0.0.9:51820")
	if err != nil {
		t.Fatal(err)
	}
	if genMig != 3 {
		t.Fatalf("migration gen = %d, want 3", genMig)
	}
	// 第二个节点拿下一个 /64。
	pB, genB, err := s.EnsureNodeFabric(ctx, fabricCell, nodeB, "pk-b", "10.0.0.2:51820")
	if err != nil {
		t.Fatal(err)
	}
	if !fabricCell.Contains(pB.Addr()) || pB.Bits() != 64 || genB != 1 || pB == pA {
		t.Fatalf("node B = %s gen %d", pB, genB)
	}
	// ListWGPeers 含两行且 node_prefix 全局唯一。
	peers, err := s.ListWGPeers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, p := range peers {
		if p.NodeID == nodeA || p.NodeID == nodeB {
			if seen[p.NodePrefix.String()] {
				t.Fatalf("duplicate prefix %s", p.NodePrefix)
			}
			seen[p.NodePrefix.String()] = true
		}
	}
}

func TestEnsureNodeFabricConcurrentRegistrationConverges(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	nodeID := "node-conc-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID)
	})
	const workers = 6
	type result struct {
		prefix netip.Prefix
		gen    int64
		err    error
	}
	res := make(chan result, workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			p, g, err := s.EnsureNodeFabric(ctx, fabricCell, nodeID,
				fmt.Sprintf("pk-%d", i), fmt.Sprintf("10.0.0.%d:51820", i+1))
			res <- result{p, g, err}
		}(i)
	}
	prefixes := map[netip.Prefix]bool{}
	var lastGen int64
	for i := 0; i < workers; i++ {
		r := <-res
		if r.err != nil {
			t.Fatalf("concurrent ensure: %v", r.err)
		}
		prefixes[r.prefix] = true
		if r.gen > lastGen {
			lastGen = r.gen
		}
	}
	if len(prefixes) != 1 {
		t.Fatalf("distinct prefixes across concurrent registration: %v", prefixes)
	}
	// 并发轮换可能发生多次换代，但最终行存在且前缀唯一。
	var n int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM wg_peers WHERE node_id=$1`, nodeID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("wg_peers rows = %d", n)
	}
}

func TestFabricIdentitiesForNode(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-fab-id-" + suffix
	app := "app-fab-id-" + suffix
	nodeID := "node-fab-id-" + suffix
	machineID := "m-fab-id-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM ipam_allocations WHERE machine_id=$1`, machineID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM workload_identities WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, nodeID)
		cleanupProject(t, s, project)
	})
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	nodePrefix, _, err := s.EnsureNodeFabric(ctx, fabricCell, nodeID, "pk", "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	// app + deployment（主 service 名 api）+ machine。
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname) VALUES($1,$2,$3)`,
		app, project, "fab-id-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, services)
		VALUES($1,$2,1,'img','[{"name":"api","internal_port":8080}]'::jsonb)`,
		"dep-"+suffix, app); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
		VALUES($1,$2,$3,0,$4,'img')`, machineID, app, "dep-"+suffix, "m-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	// 稳定身份（部署主 service）+ ULA 分配。
	if _, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", project, app, "api"); err != nil {
		t.Fatal(err)
	}
	ula, err := s.AllocateULA(ctx, fabricCell, nodePrefix, project, nodeID, machineID, "exec-1", 1)
	if err != nil {
		t.Fatal(err)
	}

	ids, err := s.FabricIdentities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := identitiesForMachine(ids, machineID)
	if got == nil {
		t.Fatalf("fixture identity missing: %+v", ids)
	}
	if got.IdentityID == 0 || got.TrustDomain != "firepaas.local" || got.Service != "api" ||
		got.ULA != ula || got.ExecutionID != "exec-1" || got.Generation != 1 {
		t.Fatalf("identity row = %+v", got)
	}
	// G2a 起快照身份为集群全域（跨节点源绑定/策略解析）：另一节点的分配
	// 也在同一集合内。
	otherNode := "other-node-" + suffix
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
		VALUES($1,$2,$3,1,$4,'img')`,
		machineID+"-2", app, "dep-"+suffix, "m2-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AllocateULA(ctx, fabricCell, nodePrefix, project, otherNode,
		machineID+"-2", "exec-2", 1); err != nil {
		t.Fatal(err)
	}
	ids, err = s.FabricIdentities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if identitiesForMachine(ids, machineID+"-2") == nil {
		t.Fatalf("cluster-wide identities missing other node: %+v", ids)
	}
	// 缺稳定身份 → 跳过该行并 Warn（P1 独立评审：单行坏数据不得熔断整轮；
	// 查询错误仍上抛，见 fabric reconciler 整轮跳过）。
	if _, err := s.Pool().Exec(ctx, `DELETE FROM workload_identities WHERE project_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	ids, err = s.FabricIdentities(ctx)
	if err != nil {
		t.Fatalf("missing identity must skip rows, not abort round: %v", err)
	}
	if identitiesForMachine(ids, machineID) != nil || identitiesForMachine(ids, machineID+"-2") != nil {
		t.Fatalf("rows without stable identity must be skipped: %+v", ids)
	}
}

func TestFabricVersionFencing(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	nodeID := "node-ver-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, nodeID)
	})
	gen, hash, err := s.FabricVersion(ctx, nodeID)
	if err != nil || gen != 0 || hash != "" {
		t.Fatalf("initial version = (%d, %q, %v)", gen, hash, err)
	}
	if err := s.AdvanceFabricVersion(ctx, nodeID, 1, "h1"); err != nil {
		t.Fatal(err)
	}
	if gen, hash, err := s.FabricVersion(ctx, nodeID); err != nil || gen != 1 || hash != "h1" {
		t.Fatalf("version = (%d, %q, %v)", gen, hash, err)
	}
	// 同代覆写拒绝（W2：即使内容相同；agent 侧同代异内容拒绝，控制面侧
	// 同代覆写会制造分叉。重放用更高代，见 reconciler stale 路径）。
	if err := s.AdvanceFabricVersion(ctx, nodeID, 1, "h1"); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("same-generation restamp err = %v, want stale", err)
	}
	if err := s.AdvanceFabricVersion(ctx, nodeID, 1, "h1x"); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("same-generation different-hash err = %v, want stale", err)
	}
	if gen, hash, err := s.FabricVersion(ctx, nodeID); err != nil || gen != 1 || hash != "h1" {
		t.Fatalf("version moved on rejected restamp = (%d, %q, %v)", gen, hash, err)
	}
	// 旧代拒绝。
	if err := s.AdvanceFabricVersion(ctx, nodeID, 0, "h0"); !errors.Is(err, ErrFabricGenerationStale) {
		t.Fatalf("stale advance err = %v", err)
	}
	if err := s.AdvanceFabricVersion(ctx, nodeID, 2, "h2"); err != nil {
		t.Fatal(err)
	}
}

// TestDeleteWGPeer 退役节点摘除 peer 与水位（W2 P0）：删除后 peer 集不再
// 含该节点（reconciler 快照哈希改变 → 全节点重推摘路由），重注册可复用
// 同 node_id（新 /64，前缀不保留——退役语义）。
func TestDeleteWGPeer(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	nodeID := "node-del-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, nodeID)
	})
	if _, _, err := s.EnsureNodeFabric(ctx, fabricCell, nodeID, "pk-del", "10.9.0.1:51820"); err != nil {
		t.Fatal(err)
	}
	if err := s.AdvanceFabricVersion(ctx, nodeID, 1, "h"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWGPeer(ctx, nodeID); err != nil {
		t.Fatal(err)
	}
	peers, err := s.ListWGPeers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range peers {
		if p.NodeID == nodeID {
			t.Fatalf("peer still present after delete: %+v", p)
		}
	}
	if gen, _, err := s.FabricVersion(ctx, nodeID); err != nil || gen != 0 {
		t.Fatalf("version after delete = (%d, %v)", gen, err)
	}
	// 幂等：重复删除不报错（退役重试安全）。
	if err := s.DeleteWGPeer(ctx, nodeID); err != nil {
		t.Fatal(err)
	}
}

// identitiesForMachine 从集群全域身份集合取指定 machine 的行（共享 lab 库
// 可能含其他在役分配）。
func identitiesForMachine(ids []FabricIdentityRow, machineID string) *FabricIdentityRow {
	for i := range ids {
		if ids[i].MachineID == machineID {
			return &ids[i]
		}
	}
	return nil
}

// TestAllocateULAPinsNodeWithinExecution：同一 machine+execution 的 ULA 在
// execution 生命周期内不可变（§7）——换节点重派（ResourceExhausted 重试/
// 重调度）时返回 ErrFabricNodePinned，调用方回绑定节点重试；绝不在新节点
// 重分配（与已推送快照竞争会让 guest 拿到与库不一致的旧地址——真机 spike
// 抓到：spike-7 宿主 0:2::/64 却拿到 0:1::7）。
func TestAllocateULAPinsNodeWithinExecution(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-rebind-" + suffix
	machineID := "m-rebind-" + suffix
	nodeA, nodeB := "node-rb-a-"+suffix, "node-rb-b-"+suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM ipam_allocations WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id IN ($1,$2)`, nodeA, nodeB)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM machines WHERE id=$1`, machineID)
		cleanupProject(t, s, project)
	})
	cell := mustParsePrefix("fd7a:9a55::/40")
	pA, _, err := s.EnsureNodeFabric(ctx, cell, nodeA, "pk-a", "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	pB, _, err := s.EnsureNodeFabric(ctx, cell, nodeB, "pk-b", "10.0.0.2:51820")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `INSERT INTO apps(id, project_id, hostname) VALUES('app-rebind',$1,'rebind.test')`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `INSERT INTO deployments(id, app_id, generation, image_ref) VALUES('dep-rebind','app-rebind',1,'img')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
		VALUES($1,'app-rebind','dep-rebind',0,'m-rebind.test','img')`, machineID); err != nil {
		t.Fatal(err)
	}
	// 第一次尝试：nodeA 上分配。
	ula1, err := s.AllocateULA(ctx, cell, pA, project, nodeA, machineID, "exec-rb", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !pA.Contains(ula1) {
		t.Fatalf("ula1 %s outside nodeA prefix %s", ula1, pA)
	}
	// 换节点重派：ULA 不变，返回 ErrFabricNodePinned 指回 nodeA。
	_, err = s.AllocateULA(ctx, cell, pB, project, nodeB, machineID, "exec-rb", 1)
	var pinned *ErrFabricNodePinned
	if !errors.As(err, &pinned) {
		t.Fatalf("cross-node allocate err = %v, want ErrFabricNodePinned", err)
	}
	if pinned.NodeID != nodeA || pinned.ULA != ula1 {
		t.Fatalf("pinned = %+v, want node %s ula %s", pinned, nodeA, ula1)
	}
	// 回绑定节点重放：幂等复用原 ULA。
	ula3, err := s.AllocateULA(ctx, cell, pA, project, nodeA, machineID, "exec-rb", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ula3 != ula1 {
		t.Fatalf("pinned-node replay = %s, want %s", ula3, ula1)
	}
	// 在役行唯一。
	var active int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM ipam_allocations WHERE machine_id=$1 AND execution_id='exec-rb' AND released_at IS NULL`,
		machineID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active allocations = %d, want 1", active)
	}
}

func mustParsePrefix(raw string) netip.Prefix { return netip.MustParsePrefix(raw) }

// TestMeshDirectIndex 在役直连声明索引（W3 §15）：只有在役 machine 的
// deployment 声明进入索引；legacy 无 services 声明的不在索引内。
func TestMeshDirectIndex(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-mdi-" + suffix
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	mkapp := func(app, dep, services string) {
		if _, err := s.Pool().Exec(ctx, `INSERT INTO apps(id, project_id, hostname) VALUES($1,$2,$3)`,
			app, project, app+".test"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Pool().Exec(ctx, `
			INSERT INTO deployments(id, app_id, generation, image_ref, services)
			VALUES($1,$2,1,'img',$3::jsonb)`, dep, app, services); err != nil {
			t.Fatal(err)
		}
	}
	mkmachine := func(mid, app, dep string) {
		if _, err := s.Pool().Exec(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
			VALUES($1,$2,$3,0,$4,'img')`, mid, app, dep, mid+".test"); err != nil {
			t.Fatal(err)
		}
	}
	mkapp("app-mdi-"+suffix, "dep-mdi-"+suffix,
		`[{"name":"api","internal_port":8080,"mesh_direct":true},{"name":"admin","internal_port":9090}]`)
	mkmachine("m-mdi-"+suffix, "app-mdi-"+suffix, "dep-mdi-"+suffix)
	// 僵尸 deployment（无在役 machine 引用）：声明不进索引。
	mkapp("app-mdi-z-"+suffix, "dep-mdi-z-"+suffix,
		`[{"name":"api","internal_port":8080,"mesh_direct":true}]`)
	idx, err := s.MeshDirectIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Services[MeshServiceKey(project, "app-mdi-"+suffix, "api")] {
		t.Fatalf("direct service missing: %+v", idx.Services)
	}
	if idx.Services[MeshServiceKey(project, "app-mdi-"+suffix, "admin")] {
		t.Fatalf("non-direct service indexed: %+v", idx.Services)
	}
	if !idx.Machines["m-mdi-"+suffix] {
		t.Fatalf("direct machine missing: %+v", idx.Machines)
	}
	if idx.Services[MeshServiceKey(project, "app-mdi-z-"+suffix, "api")] {
		t.Fatalf("stale deployment service indexed: %+v", idx.Services)
	}
}

// TestFabricIdentitiesMultiService（P1 独立评审）：多 service 部署逐 service
// 出行（LATERAL），次服务的 Service/MeshDirect 不得取首服务的值。
func TestFabricIdentitiesMultiService(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid()) + "ms"
	project := "p-fab-ms-" + suffix
	app := "app-fab-ms-" + suffix
	nodeID := "node-fab-ms-" + suffix
	machineID := "m-fab-ms-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM ipam_allocations WHERE machine_id=$1`, machineID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM workload_identities WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, nodeID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, nodeID)
		cleanupProject(t, s, project)
	})
	if err := s.EnsureProject(ctx, project, project); err != nil {
		t.Fatal(err)
	}
	nodePrefix, _, err := s.EnsureNodeFabric(ctx, fabricCell, nodeID, "pk", "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname) VALUES($1,$2,$3)`,
		app, project, "fab-ms-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	// 首服务非 mesh、次服务 mesh（错位即被发现）。
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, services)
		VALUES($1,$2,1,'img','[{"name":"web","internal_port":80},{"name":"api","internal_port":8080,"mesh_direct":true}]'::jsonb)`,
		"dep-"+suffix, app); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
		VALUES($1,$2,$3,0,$4,'img')`, machineID, app, "dep-"+suffix, "m-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	for _, svc := range []string{"web", "api"} {
		if _, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", project, app, svc); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.AllocateULA(ctx, fabricCell, nodePrefix, project, nodeID, machineID, "exec-1", 1); err != nil {
		t.Fatal(err)
	}
	ids, err := s.FabricIdentities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bySvc := map[string]FabricIdentityRow{}
	for _, id := range ids {
		if id.MachineID == machineID {
			bySvc[id.Service] = id
		}
	}
	if len(bySvc) != 2 {
		t.Fatalf("want 2 service rows, got %+v", bySvc)
	}
	if bySvc["web"].MeshDirect {
		t.Fatalf("web must not be mesh_direct: %+v", bySvc["web"])
	}
	if !bySvc["api"].MeshDirect {
		t.Fatalf("api must be mesh_direct: %+v", bySvc["api"])
	}
	if bySvc["web"].IdentityID == bySvc["api"].IdentityID {
		t.Fatal("per-service identities must differ")
	}
}
