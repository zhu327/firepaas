package fabric

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// fakeAgent 是内存 FabricService 实现：记录请求并按脚本响应。
type fakeAgent struct {
	pb.UnimplementedFabricServiceServer
	requests []*pb.ApplyFabricRequest
	respond  func(req *pb.ApplyFabricRequest) (*pb.ApplyFabricResponse, error)
}

func newFakeAgent() *fakeAgent { return &fakeAgent{} }

func (f *fakeAgent) ApplyFabric(_ context.Context, req *pb.ApplyFabricRequest) (*pb.ApplyFabricResponse, error) {
	f.requests = append(f.requests, proto.Clone(req).(*pb.ApplyFabricRequest))
	if f.respond != nil {
		return f.respond(req)
	}
	return &pb.ApplyFabricResponse{AppliedGeneration: req.GetFabricGeneration()}, nil
}

func (f *fakeAgent) count() int { return len(f.requests) }

// startAgent 用 bufconn 启动 fake agent 并返回 agentclient.Client。
func startAgent(t *testing.T, agent *fakeAgent) *agentclient.Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterFabricServiceServer(srv, agent)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return agentclient.NewFromConn(conn, "bufnet")
}

// fakeNodeSource 提供固定节点视图与客户端映射。
type fakeNodeSource struct {
	nodes   []nodemanager.Node
	clients map[string]*agentclient.Client
}

func (f *fakeNodeSource) Nodes() []nodemanager.Node { return f.nodes }
func (f *fakeNodeSource) ClientForNodeID(nodeID string) *agentclient.Client {
	return f.clients[nodeID]
}

// fabricTestStore 返回跑过迁移的 PG store；未设置 FIREPAAS_TEST_POSTGRES 时跳过。
//
// review 2026-09-10：用独立 schema（search_path）隔离——共享 lab 库里
// 残留的 eastwest_policies / wg_peers / ipam 行会让“全表快照”类断言
// （如 TestSyncCarriesEastWestAndMeshDirect）误判，且测试自身不应清理
// 别人的历史数据。每个测试进程一个 schema，t.Cleanup 里 CASCADE 删除。
func fabricTestStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run fabric tests")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("fabric_test_%d", os.Getpid())

	boot, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boot.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
		boot.Close()
		t.Fatal(err)
	}
	if _, err := boot.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		boot.Close()
		t.Fatal(err)
	}
	boot.Close()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, cerr := pgxpool.New(context.Background(), dsn)
		if cerr != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	return store.New(pool)
}

// fabricFixture 建立 project/app/deployment/machine/identity/ULA 并返回清理。
type fabricFixture struct {
	project, app, nodeID, machineID, executionID string
}

func makeFixture(t *testing.T, s *store.Store, suffix, nodeID, service string) fabricFixture {
	t.Helper()
	ctx := context.Background()
	f := fabricFixture{
		project:     "p-fab-" + suffix,
		app:         "app-fab-" + suffix,
		nodeID:      nodeID,
		machineID:   "m-fab-" + suffix,
		executionID: "exec-fab-" + suffix,
	}
	if err := s.EnsureProject(ctx, f.project, f.project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM ipam_allocations WHERE project_id=$1`, f.project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM workload_identities WHERE project_id=$1`, f.project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, f.nodeID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, f.nodeID)
		cleanupProjectSQL(t, s, f.project)
	})
	if _, err := s.Pool().Exec(ctx, `INSERT INTO apps(id, project_id, hostname) VALUES($1,$2,$3)`,
		f.app, f.project, "fab-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, services)
		VALUES($1,$2,1,'img','[{"name":"api","internal_port":8080}]'::jsonb)`,
		"dep-"+suffix, f.app); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
		VALUES($1,$2,$3,0,$4,'img')`, f.machineID, f.app, "dep-"+suffix, "m-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureWorkloadIdentity(ctx, "firepaas.local", f.project, f.app, service); err != nil {
		t.Fatal(err)
	}
	// node /64 由 EnsureNodeFabric 分配，随后在期内分配实例 /128。
	prefix, _, err := s.EnsureNodeFabric(ctx, mustPrefix("fd7a:9a55::/40"), f.nodeID, "pk-"+suffix, "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AllocateULA(ctx, mustPrefix("fd7a:9a55::/40"), prefix,
		f.project, f.nodeID, f.machineID, f.executionID, 1); err != nil {
		t.Fatal(err)
	}
	return f
}

func mustPrefix(raw string) netip.Prefix { return netip.MustParsePrefix(raw) }

// wgTestPubkey 生成契约合法的 32 字节 WG 公钥（base64；测试键材料无秘密）。
func wgTestPubkey(fill byte) string {
	b := make([]byte, 32)
	for i := range b {
		b[i] = fill
	}
	return base64.StdEncoding.EncodeToString(b)
}

func cleanupProjectSQL(t *testing.T, s *store.Store, projectID string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`DELETE FROM machines WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
		`DELETE FROM apps WHERE project_id=$1`,
		`DELETE FROM projects WHERE id=$1`,
	} {
		if _, err := s.Pool().Exec(ctx, q, projectID); err != nil {
			t.Logf("cleanup %q: %v", q, err)
		}
	}
}

func nodeInfo(nodeID, pubkey, grpcAddr string) nodemanager.Node {
	return nodemanager.Node{
		NomadNodeID: "nomad-" + nodeID,
		Name:        nodeID,
		GRPCAddr:    grpcAddr,
		Info: &pb.ServiceInfoResponse{
			NodeId:       nodeID,
			FabricPubkey: pubkey,
		},
	}
}

func TestSyncPushesAndSkipsUnchanged(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-a-"+suffix, "api")
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 {
		t.Fatalf("pushes = %d, want 1", agent.count())
	}
	req := agent.requests[0]
	// 集群全域身份（G2a）：本库可能含其他在役分配（共享 lab 库），
	// 断言包含本 fixture 的身份而非精确计数。
	// 共享 lab 库可能含其他节点（wg_peers 活跃行）：只断言本节点 peer
	// 集不含自己，不锁精确成员。
	// /64 序号取决于库内既有节点（共享 lab 库）：只断言落在 cell /40 内。
	nodePrefix, perr := netip.ParsePrefix(req.GetNodePrefix())
	if req.GetFabricGeneration() != 1 || perr != nil ||
		!mustPrefix("fd7a:9a55::/40").Contains(nodePrefix.Addr()) || nodePrefix.Bits() != 64 {
		t.Fatalf("first request prefix = %q (err %v)", req.GetNodePrefix(), perr)
	}
	for _, p := range req.GetPeers() {
		if p.GetNodeId() == fix.nodeID {
			t.Fatalf("self peer leaked: %+v", p)
		}
	}
	if !containsIdentity(req, fix.machineID, fix.executionID, "api") {
		t.Fatalf("fixture identity missing: %+v", req.GetIdentities())
	}
	// 内容未变：第二轮不推送。
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 {
		t.Fatalf("unchanged sync pushed again: %d", agent.count())
	}
	// 版本水位只升不降。
	gen, hash, err := s.FabricVersion(ctx, fix.nodeID)
	if err != nil || gen != 1 || hash == "" {
		t.Fatalf("version = (%d, %q, %v)", gen, hash, err)
	}
}

// TestSyncForceResyncAfterWindow：内容哈希未变时默认不推；距上次成功推送
// 超过 ForceSyncEvery 后即使内容未变也强制推进一代重推（agent 无水位查询，
// 此窗口是 agent 状态丢失后的自愈上限）。
func TestSyncForceResyncAfterWindow(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-force-"+suffix, "api")
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{
		Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40",
		ForceSyncEvery: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 {
		t.Fatalf("pushes = %d, want 1", agent.count())
	}
	// 窗口内：内容未变不推。
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 {
		t.Fatalf("unchanged within window pushed again: %d", agent.count())
	}
	// 把上次成功推送时刻拨到窗口之外（确定性，不 sleep 依赖时钟）→ 强制重推。
	r.statusMu.Lock()
	r.lastPushAt[fix.nodeID] = time.Now().Add(-2 * time.Hour)
	r.statusMu.Unlock()
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("force resync did not push: %d", agent.count())
	}
	if got := agent.requests[1].GetFabricGeneration(); got != 2 {
		t.Fatalf("force resync generation = %d, want 2", got)
	}
	// 强制推送后再入窗口：内容未变不再推。
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("post-force unchanged pushed again: %d", agent.count())
	}
}

func TestSyncPushesOnContentChange(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-b-"+suffix, "api")
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// 新增一个 execution → 内容变化 → 下一代推送。
	prefix, _, err := s.EnsureNodeFabric(ctx, mustPrefix("fd7a:9a55::/40"), fix.nodeID, "pk-"+suffix, "10.0.0.1:51820")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
		VALUES($1,$2,$3,1,$4,'img')`, "m2-"+suffix, fix.app, "dep-"+suffix, "m2-"+suffix+".test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AllocateULA(ctx, mustPrefix("fd7a:9a55::/40"), prefix,
		fix.project, fix.nodeID, "m2-"+suffix, "exec2-"+suffix, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("pushes = %d, want 2", agent.count())
	}
	if got := agent.requests[1]; got.GetFabricGeneration() != 2 ||
		!containsIdentity(got, "m2-"+suffix, "exec2-"+suffix, "api") {
		t.Fatalf("second request = %+v", got)
	}
}

func TestSyncStaleConvergesToHigherGeneration(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-c-"+suffix, "api")
	agent := newFakeAgent()
	// 第一次推送 agent 报 stale（其水位更高），之后恢复。
	calls := 0
	agent.respond = func(req *pb.ApplyFabricRequest) (*pb.ApplyFabricResponse, error) {
		calls++
		if calls == 1 {
			return nil, status.Error(codes.FailedPrecondition, "stale fabric generation")
		}
		return &pb.ApplyFabricResponse{AppliedGeneration: req.GetFabricGeneration()}, nil
	}
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Sync(ctx); err != nil {
		t.Fatal(err) // Sync 单节点错误降级，不整体失败
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("pushes = %d, want 2", agent.count())
	}
	// stale 后强制推进水位并清空哈希 → 下一轮 gen 从 2 跳到 3。
	if got := agent.requests[0].GetFabricGeneration(); got != 1 {
		t.Fatalf("first gen = %d, want 1", got)
	}
	if got := agent.requests[1].GetFabricGeneration(); got != 3 {
		t.Fatalf("second gen = %d, want 3", got)
	}
	gen, hash, err := s.FabricVersion(ctx, fix.nodeID)
	if err != nil || gen != 3 || hash == "" {
		t.Fatalf("version = (%d, %q, %v)", gen, hash, err)
	}
	// 收敛后不再推送。
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("post-convergence pushes = %d", agent.count())
	}
}

func TestSyncSkipsMeshDisabledNode(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo("node-off", "", "10.0.0.9:5108")},
		clients: map[string]*agentclient.Client{"node-off": client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 0 {
		t.Fatalf("mesh-disabled node pushed: %d", agent.count())
	}
	var n int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM wg_peers WHERE node_id='node-off'`).Scan(&n); err != nil ||
		n != 0 {
		t.Fatalf("wg_peers rows=%d err=%v", n, err)
	}
}

func TestSyncPeerSetExcludesSelf(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	nodeA := "node-pa-" + suffix
	nodeB := "node-pb-" + suffix
	agentA, agentB := newFakeAgent(), newFakeAgent()
	clientA, clientB := startAgent(t, agentA), startAgent(t, agentB)
	src := &fakeNodeSource{
		nodes: []nodemanager.Node{
			nodeInfo(nodeA, wgTestPubkey(0x0a), "10.0.0.1:5108"),
			nodeInfo(nodeB, wgTestPubkey(0x0b), "10.0.0.2:5108"),
		},
		clients: map[string]*agentclient.Client{nodeA: clientA, nodeB: clientB},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id IN ($1,$2)`, nodeA, nodeB)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id IN ($1,$2)`, nodeA, nodeB)
	})
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agentA.count() != 1 || agentB.count() != 1 {
		t.Fatalf("pushes = (%d, %d)", agentA.count(), agentB.count())
	}
	// 共享 lab 库可能含活跃 mesh 节点：断言互相包含且不含自己。
	reqA := agentA.requests[0]
	if !peerSetHas(reqA, nodeB, "10.0.0.2:51820") || peerSetHas(reqA, nodeA, "") {
		t.Fatalf("A peers = %+v", reqA.GetPeers())
	}
	reqB := agentB.requests[0]
	if !peerSetHas(reqB, nodeA, "10.0.0.1:51820") || peerSetHas(reqB, nodeB, "") {
		t.Fatalf("B peers = %+v", reqB.GetPeers())
	}
}

// TestSyncRejectsInvalidPeerPubkey：快照 peer 公钥 base64 合法但不足 32 字节
// 时，发送前被契约校验拦截：不出网、不推进水位、状态记录错误（否则 agent 以
// InvalidArgument 拒收，控制面把失败当可重试，形成全网永久重试）。
func TestSyncRejectsInvalidPeerPubkey(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-badkey-"+suffix, "api")
	badNode := "node-badkey-peer-" + suffix
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id=$1`, badNode)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM fabric_versions WHERE node_id=$1`, badNode)
	})
	// base64 合法但解码为 9 字节，非 32 字节 WireGuard 公钥。
	badPubkey := base64.StdEncoding.EncodeToString([]byte("short-key"))
	if _, _, err := s.EnsureNodeFabric(ctx, mustPrefix("fd7a:9a55::/40"), badNode, badPubkey, "10.0.0.7:51820"); err != nil {
		t.Fatal(err)
	}
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err) // 单节点错误降级，不整体失败
	}
	if agent.count() != 0 {
		t.Fatalf("invalid snapshot must not be sent: %d", agent.count())
	}
	st, ok := r.Status(fix.nodeID)
	if !ok || st.LastError == "" {
		t.Fatalf("status must carry validation error: ok=%v status=%+v", ok, st)
	}
	if st.AppliedGeneration != 0 {
		t.Fatalf("watermark must not advance: %+v", st)
	}
	gen, hash, err := s.FabricVersion(ctx, fix.nodeID)
	if err != nil || gen != 0 || hash != "" {
		t.Fatalf("fabric version = (%d, %q, %v), want 0/empty", gen, hash, err)
	}
}

func TestBuildEastWestSnapshot(t *testing.T) {
	direct := map[string]bool{
		store.MeshServiceKey("p", "db", "pg"): true,
	}
	if got := buildEastWestSnapshot(nil, direct); got != nil {
		t.Fatalf("empty table must be nil, got %+v", got)
	}
	if got := buildEastWestSnapshot([]store.EastWestPolicy{}, direct); got != nil {
		t.Fatalf("empty table must be nil, got %+v", got)
	}
	got := buildEastWestSnapshot([]store.EastWestPolicy{
		{
			SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "db", DstService: "pg",
			Ports: []int32{5432}, Generation: 3,
		},
		{
			SrcProject: "p", SrcApp: "cron", DstProject: "p", DstApp: "db", DstService: "pg",
			Ports: []int32{0, -1}, Generation: 7,
		}, // 脏行跳过，不毒化快照
	}, direct)
	if got == nil || got.GetGeneration() != 3 || len(got.GetRules()) != 1 {
		t.Fatalf("snapshot = %+v, want 1 rule generation 3", got)
	}
	if r := got.GetRules()[0]; r.GetSrcApp() != "web" || len(r.GetPorts()) != 1 {
		t.Fatalf("rule = %+v", r)
	}
}

// TestBuildEastWestSnapshotPrunesNonDirectService（W3 §15）：dst service 未
// 声明 mesh_direct 的规则不进快照（缺一不可的组装侧裁决）；全剪掉 = nil。
func TestBuildEastWestSnapshotPrunesNonDirectService(t *testing.T) {
	direct := map[string]bool{
		store.MeshServiceKey("p", "db", "pg"): true,
	}
	got := buildEastWestSnapshot([]store.EastWestPolicy{
		{
			SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "db", DstService: "pg",
			Ports: []int32{5432}, Generation: 2,
		},
		{
			SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "db", DstService: "admin",
			Ports: []int32{8080}, Generation: 9,
		},
	}, direct)
	if got == nil || len(got.GetRules()) != 1 || got.GetGeneration() != 2 {
		t.Fatalf("pruned snapshot = %+v, want 1 rule generation 2", got)
	}
	if got := buildEastWestSnapshot([]store.EastWestPolicy{
		{
			SrcProject: "p", SrcApp: "web", DstProject: "p", DstApp: "db", DstService: "admin",
			Ports: []int32{8080}, Generation: 9,
		},
	}, direct); got != nil {
		t.Fatalf("fully pruned table must be nil, got %+v", got)
	}
}

// TestSyncCarriesEastWestAndMeshDirect：reconciler 快照携带策略全表与直连
// 声明；任一变化都触发新一代推送（哈希覆盖）。
func TestSyncCarriesEastWestAndMeshDirect(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-c-"+suffix, "api")
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM eastwest_policies WHERE src_project=$1`, fix.project)
	})
	// 本 deployment 主服务声明 mesh_direct。
	if _, err := s.Pool().Exec(ctx, `
		UPDATE deployments SET services='[{"name":"api","internal_port":8080,"mesh_direct":true}]'::jsonb
		WHERE id=$1`, "dep-"+suffix); err != nil {
		t.Fatal(err)
	}
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 {
		t.Fatalf("pushes = %d, want 1", agent.count())
	}
	req := agent.requests[0]
	if id := findIdentity(req, "m-fab-"+suffix); id == nil || !id.GetMeshDirect() {
		t.Fatalf("fixture mesh_direct not carried: %+v", req.GetIdentities())
	}
	// 共享 lab 库可能残留其它 project 的规则：只断言本 fixture project
	// 的规则不在首代快照（空表 → nil 的语义由 buildEastWestSnapshot 单测覆盖）。
	for _, r := range req.GetEastwest().GetRules() {
		if r.GetDstProject() == fix.project || r.GetSrcProject() == fix.project {
			t.Fatalf("fixture project rule leaked into first snapshot: %+v", r)
		}
	}
	// 新增策略 → 内容变化 → 下一代推送且携带全表。
	if err := s.PutEastWestPolicy(ctx, store.EastWestPolicy{
		SrcProject: fix.project, SrcApp: "web", DstProject: fix.project,
		DstApp: fix.app, DstService: "api", Ports: []int32{8080}, Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("policy change must trigger push: %d", agent.count())
	}
	req2 := agent.requests[1]
	if req2.GetFabricGeneration() != 2 || req2.GetEastwest() == nil ||
		len(req2.GetEastwest().GetRules()) != 1 || req2.GetEastwest().GetGeneration() != 1 {
		t.Fatalf("second request = %+v", req2)
	}
	// 内容未变不再推送。
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("unchanged sync pushed again: %d", agent.count())
	}
}

// findIdentity 按 machine_id 前缀取快照身份（共享库容错）。
func findIdentity(req *pb.ApplyFabricRequest, machineID string) *pb.IdentityMapping {
	for _, id := range req.GetIdentities() {
		if id.GetMachineId() == machineID {
			return id
		}
	}
	return nil
}

// peerSetHas 断言 peer 集含指定节点（endpoint 非空时也须匹配）。
func peerSetHas(req *pb.ApplyFabricRequest, nodeID, endpoint string) bool {
	for _, p := range req.GetPeers() {
		if p.GetNodeId() != nodeID {
			continue
		}
		return endpoint == "" || p.GetEndpoint() == endpoint
	}
	return false
}

// containsIdentity 断言快照含指定 machine/execution/service 的身份（集群
// 全域身份后共享测试库不再适合精确计数）。
func containsIdentity(req *pb.ApplyFabricRequest, machineID, executionID, service string) bool {
	for _, id := range req.GetIdentities() {
		if id.GetMachineId() == machineID && id.GetExecutionId() == executionID &&
			id.GetService() == service && id.GetIdentityId() != 0 {
			return true
		}
	}
	return false
}

// fakeDNSLister 提供固定 .internal 记录集（可变注入）。
type fakeDNSLister struct {
	records []catalog.InternalDNSRecord
	err     error
}

func (f *fakeDNSLister) ListInternalDNS(context.Context) ([]catalog.InternalDNSRecord, error) {
	return f.records, f.err
}

// TestSyncCarriesInternalDNS（G2c，ADR-0040 §16）：.internal 记录随快照下发；
// 记录变化触发新一代推送（哈希覆盖）；读失败降级（本轮跳过，不断发其余）。
func TestSyncCarriesInternalDNS(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-dns-"+suffix, "api")
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	dns := &fakeDNSLister{records: []catalog.InternalDNSRecord{
		{Name: "app.p.internal", AAAA: []string{"fd7a:9a55:0:1::5"}, Generation: 3},
	}}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40", DNS: dns})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 || len(agent.requests[0].GetDns()) != 1 {
		t.Fatalf("first push dns = %+v (count %d)", agent.requests[0].GetDns(), agent.count())
	}
	rec := agent.requests[0].GetDns()[0]
	if rec.GetName() != "app.p.internal" || len(rec.GetAaaa()) != 1 || rec.GetAaaa()[0] != "fd7a:9a55:0:1::5" ||
		rec.GetGeneration() != 3 {
		t.Fatalf("dns record = %+v", rec)
	}
	// 内容未变：不推送。
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 1 {
		t.Fatalf("unchanged dns pushed again: %d", agent.count())
	}
	// 记录变化 → 新一代推送。
	dns.records = []catalog.InternalDNSRecord{
		{Name: "app.p.internal", AAAA: []string{"fd7a:9a55:0:1::5", "fd7a:9a55:0:1::6"}, Generation: 4},
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("dns change must trigger push: %d", agent.count())
	}
	req2 := agent.requests[1]
	if req2.GetFabricGeneration() != 2 || len(req2.GetDns()) != 1 || len(req2.GetDns()[0].GetAaaa()) != 2 {
		t.Fatalf("second push = %+v", req2.GetDns())
	}
	// 读失败 → serve-stale：缓存与已推内容一致 → 哈希不变 → 不推（全网
	// .internal 不闪断；underlay/策略随下轮恢复一起推进）。
	dns.err = errors.New("redis down")
	dns.records = nil
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("degraded round must reuse cache (no push on identical hash): %d", agent.count())
	}
	// 推送失败后叠加读失败 → 用缓存内容重试（同 operation_id 幂等），
	// 而非推空表。先让一次内容变更的推送失败（水位不推进，缓存已更新）。
	dns.records = []catalog.InternalDNSRecord{
		{
			Name:       "app.p.internal",
			AAAA:       []string{"fd7a:9a55:0:1::5", "fd7a:9a55:0:1::6", "fd7a:9a55:0:1::7"},
			Generation: 5,
		},
	}
	dns.err = nil
	failed := false
	agent.respond = func(req *pb.ApplyFabricRequest) (*pb.ApplyFabricResponse, error) {
		if !failed {
			failed = true
			return nil, status.Error(codes.Unavailable, "agent down")
		}
		return &pb.ApplyFabricResponse{AppliedGeneration: req.GetFabricGeneration()}, nil
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 3 {
		t.Fatalf("failed push must be attempted: %d", agent.count())
	}
	dns.err = errors.New("redis down")
	dns.records = nil
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 4 {
		t.Fatalf("degraded round after failed push must retry with cache: %d", agent.count())
	}
	if got := agent.requests[3].GetDns(); len(got) != 1 || len(got[0].GetAaaa()) != 3 {
		t.Fatalf("retry must carry cached (3-AAAA) dns, not empty: %+v", got)
	}
	if agent.requests[3].GetFabricGeneration() != agent.requests[2].GetFabricGeneration() {
		t.Fatalf("retry must reuse generation (same operation_id): %d vs %d",
			agent.requests[3].GetFabricGeneration(), agent.requests[2].GetFabricGeneration())
	}
	// 恢复 → 推新表收敛。
	agent.respond = nil
	dns.err = nil
	dns.records = []catalog.InternalDNSRecord{
		{Name: "app.p.internal", AAAA: []string{"fd7a:9a55:0:1::9"}, Generation: 6},
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 5 || len(agent.requests[4].GetDns()[0].GetAaaa()) != 1 {
		t.Fatalf("recovery must push fresh dns: %d", agent.count())
	}
}

// fakeMeshProjection 记录写入的 mesh 投影（G2d）。
type fakeMeshProjection struct {
	peers     []catalog.MeshPeerRecord
	endpoints []catalog.MeshEndpointRecord
}

func (f *fakeMeshProjection) ReplaceMeshProjection(
	_ context.Context,
	peers []catalog.MeshPeerRecord,
	endpoints []catalog.MeshEndpointRecord,
	_ time.Duration,
) error {
	f.peers = append([]catalog.MeshPeerRecord(nil), peers...)
	f.endpoints = append([]catalog.MeshEndpointRecord(nil), endpoints...)
	return nil
}

// TestSyncRegistersEdgeHubAndPublishesMeshProjection（G2d，§14/§18）：
// 配置的 edge hub 注册进 wg_peers（随节点快照 peer 集下发）；mesh:peer /
// mesh:endpoint 投影全量写出（edge hub 有 peer 无 endpoint；endpoint 含
// 节点 ULA 与 ingress 端口）。
func TestSyncRegistersEdgeHubAndPublishesMeshProjection(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-eh-"+suffix, "api")
	agent := newFakeAgent()
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	proj := &fakeMeshProjection{}
	// 共享 lab 库：清理本测试注册的 edge hub 行（避免污染其它用例的
	// 快照 peer 集断言）。
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM wg_peers WHERE node_id='edge-hub'`)
	})
	r, err := New(Config{
		Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40",
		EdgeHub: EdgeHubConfig{
			NodeID:   "edge-hub",
			Pubkey:   wgTestPubkey(0x2a),
			Endpoint: "10.9.9.9:51821",
		},
		IngressPort:    5199,
		MeshProjection: proj,
	})
	if err != nil {
		t.Fatal(err)
	}
	// W3：endpoint 投影要求 execution 任一 service 直连声明（§15/§17 门控）。
	if _, err := s.Pool().Exec(ctx, `
		UPDATE deployments SET services='[{"name":"api","internal_port":8080,"mesh_direct":true}]'::jsonb
		WHERE id=$1`, "dep-"+suffix); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// 快照 peer 集含 edge hub。
	var sawEdge bool
	for _, p := range agent.requests[0].GetPeers() {
		if p.GetNodeId() == "edge-hub" {
			sawEdge = true
			// 宽松断言：base64 常量手写易错，只要求非空匹配注册值。
			if p.GetPubkey() == "" {
				t.Fatalf("edge hub pubkey empty in snapshot")
			}
		}
	}
	if !sawEdge {
		t.Fatalf("edge hub missing from snapshot peers: %+v", agent.requests[0].GetPeers())
	}
	// 投影：peer 含节点与 edge hub；endpoint 含 fixture 的 machine/execution
	// 与节点 ULA/ingress 端口；edge hub 无 endpoint。
	var edgePeer, nodePeer bool
	for _, p := range proj.peers {
		switch p.NodeID {
		case "edge-hub":
			edgePeer = true
		case fix.nodeID:
			nodePeer = true
		}
	}
	if !edgePeer || !nodePeer {
		t.Fatalf("projection peers = %+v", proj.peers)
	}
	var ep *catalog.MeshEndpointRecord
	for i := range proj.endpoints {
		if proj.endpoints[i].MachineID == fix.machineID {
			ep = &proj.endpoints[i]
		}
	}
	if ep == nil {
		t.Fatalf("fixture endpoint missing: %+v", proj.endpoints)
	}
	if ep.ExecutionID != fix.executionID || ep.IngressPort != 5199 || ep.NodeULA == "" || ep.WorkloadULA == "" {
		t.Fatalf("endpoint = %+v", ep)
	}
	for _, e := range proj.endpoints {
		if e.NodeID == "edge-hub" {
			t.Fatalf("edge hub must not have endpoints: %+v", e)
		}
	}
}

// TestNewRejectsBrokenEdgeHubConfig：非法 pubkey/endpoint 拒绝构造；仅
// endpoint 无 pubkey = 功能关闭（静默不注册，见 New 注释），不在此断言。
func TestNewRejectsBrokenEdgeHubConfig(t *testing.T) {
	s := fabricTestStore(t)
	src := &fakeNodeSource{}
	for name, cfg := range map[string]Config{
		"bad pubkey": {
			Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40",
			EdgeHub: EdgeHubConfig{Pubkey: "not-base64!!", Endpoint: "10.0.0.1:1"},
		},
		"bad endpoint": {
			Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40",
			EdgeHub: EdgeHubConfig{Pubkey: wgTestPubkey(0x2a), Endpoint: "no-port"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg); err == nil {
				t.Fatal("expected config rejection")
			}
		})
	}
}

// TestSyncStaleJumpsToAgentWatermark：agent 随 FailedPrecondition 返回
// FabricWatermarkDetail 时，控制面一步跳到 applied+1 重推，而不是逐代 +1
// 爬升（DB 水位回退/丢更新场景的长窗口冻结）。
func TestSyncStaleJumpsToAgentWatermark(t *testing.T) {
	s := fabricTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	fix := makeFixture(t, s, suffix, "node-wm-"+suffix, "api")
	agent := newFakeAgent()
	calls := 0
	agent.respond = func(req *pb.ApplyFabricRequest) (*pb.ApplyFabricResponse, error) {
		calls++
		if calls == 1 {
			st, err := status.New(codes.FailedPrecondition, "stale fabric generation").
				WithDetails(&pb.FabricWatermarkDetail{AppliedGeneration: 9})
			if err != nil {
				t.Fatal(err)
			}
			return nil, st.Err()
		}
		return &pb.ApplyFabricResponse{AppliedGeneration: req.GetFabricGeneration()}, nil
	}
	client := startAgent(t, agent)
	src := &fakeNodeSource{
		nodes:   []nodemanager.Node{nodeInfo(fix.nodeID, "pk-"+suffix, "10.0.0.1:5108")},
		clients: map[string]*agentclient.Client{fix.nodeID: client},
	}
	r, err := New(Config{Store: s, NodeSource: src, CellPrefix: "fd7a:9a55::/40"})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if agent.count() != 2 {
		t.Fatalf("pushes = %d, want 2", agent.count())
	}
	if got := agent.requests[0].GetFabricGeneration(); got != 1 {
		t.Fatalf("first gen = %d, want 1", got)
	}
	if got := agent.requests[1].GetFabricGeneration(); got != 11 {
		t.Fatalf("second gen = %d, want 11 (agent watermark 9 + 1)", got)
	}
	gen, hash, err := s.FabricVersion(ctx, fix.nodeID)
	if err != nil || gen != 11 || hash == "" {
		t.Fatalf("version = (%d, %q, %v), want (11, non-empty)", gen, hash, err)
	}
}

// TestWatermarkRegressionForcesPush：DB 水位低于本进程最近成功推送世代时，
// 即使内容哈希未变也必须立即重推（不能等 ForceSyncEvery 窗口）。
func TestWatermarkRegressionForcesPush(t *testing.T) {
	r := &Reconciler{}
	r.markPushed("n1", 42, time.Now())
	if !r.watermarkRegressed("n1", 1) {
		t.Fatal("version below in-memory last push must be detected as regression")
	}
	if r.watermarkRegressed("n1", 42) {
		t.Fatal("equal watermark must not be treated as regression")
	}
	if r.watermarkRegressed("n2", 1) {
		t.Fatal("unknown node must not be treated as regression")
	}
}
