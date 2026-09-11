package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testPeerPubkey = "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE=" // 32 字节 'a'
)

func fabricReq(opID string, gen uint64, mutate func(*pb.ApplyFabricRequest)) *pb.ApplyFabricRequest {
	req := &pb.ApplyFabricRequest{
		NodeId:           "test-node",
		FabricGeneration: gen,
		OperationId:      opID,
		NodePrefix:       "fd7a:9a55:1::/64",
		Peers: []*pb.FabricPeer{
			{
				NodeId:           "node-b",
				Pubkey:           testPeerPubkey,
				Endpoint:         "10.0.0.2:51820",
				NodePrefix:       "fd7a:9a55:2::/64",
				FabricGeneration: gen,
			},
		},
		Identities: []*pb.IdentityMapping{
			{
				IdentityId:  7,
				TrustDomain: "td.example",
				ProjectId:   "p1",
				AppId:       "a1",
				Service:     "web",
				Ula:         "fd7a:9a55:1::5",
				MachineId:   "m1",
				ExecutionId: "e1",
				Generation:  3,
			},
		},
	}
	if mutate != nil {
		mutate(req)
	}
	return req
}

func newFabricServer(t *testing.T) (*Server, *state.Ledger, *state.Fabric) {
	t.Helper()
	dir := t.TempDir()
	ledger, err := state.Open(filepath.Join(dir, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	fences, err := state.OpenFences(filepath.Join(dir, "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	fabric, err := state.OpenFabric(filepath.Join(dir, "fabric.json"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := machine.New(&fakeInstances{byName: map[string]*instances.Instance{}}, fakeImages{}, nil, nil)
	provider := info.New("test-node", "test", "test", "compute", "v1.14.2", "10.100.0.0/16", dir, nil, nil)
	srv := New(adapter, ledger, fences, provider, WithFabric(fabric))
	return srv, ledger, fabric
}

func TestApplyFabricAppliesReplaysAndFences(t *testing.T) {
	srv, ledger, fabric := newFabricServer(t)
	ctx := context.Background()

	// 首次应用。
	out, err := srv.ApplyFabric(ctx, fabricReq("op-1", 1, nil))
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if out.AppliedGeneration != 1 || out.Replayed {
		t.Fatalf("first apply out = %+v", out)
	}
	if got := fabric.Current(); got.Generation != 1 || got.NodePrefix != "fd7a:9a55:1::/64" ||
		len(got.Peers) != 1 || len(got.Identities) != 1 {
		t.Fatalf("fabric state = %+v", got)
	}

	// 同 operation 重放：幂等命中，replayed=true。
	out, err = srv.ApplyFabric(ctx, fabricReq("op-1", 1, nil))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if out.AppliedGeneration != 1 || !out.Replayed {
		t.Fatalf("replay out = %+v", out)
	}

	// 新 operation、同 generation 同内容：幂等收敛。
	out, err = srv.ApplyFabric(ctx, fabricReq("op-2", 1, nil))
	if err != nil {
		t.Fatalf("same-gen new op: %v", err)
	}
	if out.AppliedGeneration != 1 || out.Replayed {
		t.Fatalf("same-gen out = %+v", out)
	}

	// 推进 generation 2。
	out, err = srv.ApplyFabric(ctx, fabricReq("op-3", 2, func(r *pb.ApplyFabricRequest) {
		r.NodePrefix = "fd7a:9a55:2::/64"
		r.Identities[0].Ula = "fd7a:9a55:2::5"
		r.Peers[0].FabricGeneration = 2
	}))
	if err != nil {
		t.Fatalf("gen 2 apply: %v", err)
	}
	if out.AppliedGeneration != 2 {
		t.Fatalf("gen 2 out = %+v", out)
	}

	// 旧代新 operation 重放：FailedPrecondition，且不留孤儿 claim。
	stale := fabricReq("op-stale", 1, nil)
	_, err = srv.ApplyFabric(ctx, stale)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale err = %v, want FailedPrecondition", err)
	}
	wm := int64(-1)
	for _, detail := range status.Convert(err).Details() {
		if d, ok := detail.(*pb.FabricWatermarkDetail); ok {
			wm = int64(d.GetAppliedGeneration())
		}
	}
	if wm != 2 {
		t.Fatalf("stale watermark detail = %d, want 2 (control plane can jump to applied+1)", wm)
	}
	if _, ok, err := ledger.Get("op-stale", hashRequest(stale)); err != nil || ok {
		t.Fatalf("stale request left a ledger claim: ok=%v err=%v", ok, err)
	}
}

func TestApplyFabricRejectsNodeMismatch(t *testing.T) {
	srv, _, _ := newFabricServer(t)
	_, err := srv.ApplyFabric(context.Background(), fabricReq("op-1", 1, func(r *pb.ApplyFabricRequest) {
		r.NodeId = "some-other-node"
	}))
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("node mismatch err = %v, want FailedPrecondition", err)
	}
}

func TestApplyFabricRejectsSameGenerationDifferentContent(t *testing.T) {
	srv, _, _ := newFabricServer(t)
	ctx := context.Background()
	if _, err := srv.ApplyFabric(ctx, fabricReq("op-1", 1, nil)); err != nil {
		t.Fatal(err)
	}
	// 同 generation 不同内容：fence 预检阶段拒绝（不写 claim）。
	req := fabricReq("op-2", 1, func(r *pb.ApplyFabricRequest) {
		r.Peers[0].Pubkey = testPeerPubkey
		r.Peers[0].Endpoint = "10.0.0.9:51820"
	})
	if _, err := srv.ApplyFabric(ctx, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("same-gen different content err = %v, want FailedPrecondition", err)
	}
}

func TestApplyFabricRejectsHashConflict(t *testing.T) {
	srv, _, _ := newFabricServer(t)
	ctx := context.Background()
	if _, err := srv.ApplyFabric(ctx, fabricReq("op-1", 1, nil)); err != nil {
		t.Fatal(err)
	}
	// 同 operation 不同 request hash：AlreadyExists。
	req := fabricReq("op-1", 2, func(r *pb.ApplyFabricRequest) {
		r.NodePrefix = "fd7a:9a55:2::/64"
		r.Identities[0].Ula = "fd7a:9a55:2::5"
		r.Peers[0].FabricGeneration = 2
	})
	if _, err := srv.ApplyFabric(ctx, req); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("hash conflict err = %v, want AlreadyExists", err)
	}
}

func TestApplyFabricInvalidRequest(t *testing.T) {
	srv, _, _ := newFabricServer(t)
	_, err := srv.ApplyFabric(context.Background(), fabricReq("op-1", 1, func(r *pb.ApplyFabricRequest) {
		r.NodePrefix = "2001:db8:1::/64" // 非 ULA
	}))
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("invalid request err = %v, want InvalidArgument", err)
	}
}

func TestApplyFabricCrashRecoveryReappliesInProgressClaim(t *testing.T) {
	srv, ledger, fabric := newFabricServer(t)
	ctx := context.Background()
	req := fabricReq("op-recover", 2, nil)

	// 模拟崩溃窗口：Effect 前已持久化 in-progress claim。
	if _, _, err := ledger.Begin(state.Record{
		OperationID: req.OperationId, Kind: "fabric.apply", RequestHash: hashRequest(req),
	}); err != nil {
		t.Fatal(err)
	}
	if fabric.Current().Generation != 0 {
		t.Fatal("fabric must be empty before recovery")
	}

	// 重启后的同一 operation 重试：同一 claim 下重跑 Effect 并完成。
	out, err := srv.ApplyFabric(ctx, req)
	if err != nil {
		t.Fatalf("recovery apply: %v", err)
	}
	if out.AppliedGeneration != 2 || !out.Replayed {
		t.Fatalf("recovery out = %+v", out)
	}
	if got := fabric.Current(); got.Generation != 2 {
		t.Fatalf("fabric state = %+v", got)
	}
	rec, ok, err := ledger.Get(req.OperationId, hashRequest(req))
	if err != nil || !ok || !rec.Completed() {
		t.Fatalf("ledger record after recovery: rec=%+v ok=%v err=%v", rec, ok, err)
	}
}

func TestApplyFabricWithoutFabricState(t *testing.T) {
	srv, _, _ := newTestServer(t)
	if _, err := srv.ApplyFabric(context.Background(), fabricReq("op-1", 1, nil)); status.Code(
		err,
	) != codes.Unimplemented {
		t.Fatalf("err = %v, want Unimplemented", err)
	}
}

func TestApplyFabricConcurrentGenerationOrdering(t *testing.T) {
	srv, ledger, fabric := newFabricServer(t)
	ctx := context.Background()
	// 两个不同 operation 并发下发不同 generation：最终必须收敛到高代，
	// gen2（最新代）无论先后都必须成功。
	gen1 := fabricReq("op-a", 1, nil)
	gen2 := fabricReq("op-b", 2, func(r *pb.ApplyFabricRequest) {
		r.NodePrefix = "fd7a:9a55:2::/64"
		r.Identities[0].Ula = "fd7a:9a55:2::5"
		r.Peers[0].FabricGeneration = 2
	})
	type result struct {
		op  string
		err error
	}
	res := make(chan result, 2)
	go func() {
		_, err := srv.ApplyFabric(ctx, gen1)
		res <- result{"gen1", err}
	}()
	go func() {
		_, err := srv.ApplyFabric(ctx, gen2)
		res <- result{"gen2", err}
	}()
	var gen1Err, gen2Err error
	for i := 0; i < 2; i++ {
		r := <-res
		switch r.op {
		case "gen1":
			gen1Err = r.err
		case "gen2":
			gen2Err = r.err
		}
		if r.err != nil && status.Code(r.err) != codes.FailedPrecondition {
			t.Fatalf("unexpected err: %v", r.err)
		}
	}
	if gen2Err != nil {
		t.Fatalf("gen2 must succeed regardless of ordering: %v", gen2Err)
	}
	if got := fabric.Current(); got.Generation != 2 {
		t.Fatalf("final generation = %d, want 2", got.Generation)
	}
	if gen1Err == nil {
		// gen1 先应用：claim 存在且 completed。
		rec, ok, err := ledger.Get(gen1.OperationId, hashRequest(gen1))
		if err != nil || !ok || !rec.Completed() {
			t.Fatalf("applied gen1 claim: rec=%+v ok=%v err=%v", rec, ok, err)
		}
	} else {
		// gen1 被 fence 拒绝：不得留孤儿 claim。
		if _, ok, err := ledger.Get(gen1.OperationId, hashRequest(gen1)); err != nil || ok {
			t.Fatalf("stale gen1 left claim ok=%v err=%v", ok, err)
		}
	}
}

// TestFabricPolicyFromSnapshotCarriesAssembledRules（W3 §15）：组装侧已按
// dst_service 直连声明剪裁，agent 侧只看“规则 ∧ dst 身份存在”（主服务
// MeshDirect 标志不再是门控——混合 service 部署下非主服务规则同样生效）。
func TestFabricPolicyFromSnapshotCarriesAssembledRules(t *testing.T) {
	req := fabricReq("op-pol", 1, func(r *pb.ApplyFabricRequest) {
		r.Identities = append(r.Identities, &pb.IdentityMapping{
			IdentityId: 9, TrustDomain: "td.example", ProjectId: "p1",
			AppId: "a2", Service: "worker", Ula: "fd7a:9a55:1::6",
			MachineId: "m2", ExecutionId: "e2", Generation: 3,
			// 主服务未声明直连：旧逻辑会跳过本条，W3 后必须放行
			//（组装侧已裁决）。
			MeshDirect: false,
		})
		r.Eastwest = &pb.EastWestPolicySpec{Generation: 1, Rules: []*pb.EastWestPolicyRule{
			{
				SrcProject: "p1",
				SrcApp:     "a1",
				DstProject: "p1",
				DstApp:     "a2",
				DstService: "worker",
				Ports:      []uint32{9000},
			},
			{SrcProject: "p1", SrcApp: "a1", DstProject: "p1", DstApp: "ghost", DstService: "x", Ports: []uint32{1}},
		}}
	})
	snap := fabricPolicyFromSnapshot(req)
	if len(snap.Identities) != 2 {
		t.Fatalf("identities = %+v, want 2", snap.Identities)
	}
	// 正向 + 对称回程 = 2 条；dst 缺席的 ghost 规则跳过。
	if len(snap.Entries) != 2 {
		t.Fatalf("entries = %+v, want forward+return", snap.Entries)
	}
	fwd := snap.Entries[0]
	if fwd.SrcIdentity != 7 || fwd.DstIdentity != 9 || len(fwd.Ports) != 1 || fwd.Ports[0] != 9000 {
		t.Fatalf("forward = %+v", fwd)
	}
	ret := snap.Entries[1]
	if ret.SrcIdentity != 9 || ret.DstIdentity != 7 || !ret.SrcPorts ||
		len(ret.Ports) != 1 || ret.Ports[0] != 9000 {
		t.Fatalf("return = %+v (reverse must carry same ports with SrcPorts)", ret)
	}
}

// TestFabricPolicyFromSnapshotServiceGranularity：identity 解析必须保留
// service 维度（同 app 多 service 各自 identity），且 src 侧规则按 app
// 语义展开到该 app 的全部 service identity。旧 (project, app) 折叠实现下，
// dst 会绑到最后遍历到的 identity（这里会导致越权/漏授权）。
func TestFabricPolicyFromSnapshotServiceGranularity(t *testing.T) {
	req := &pb.ApplyFabricRequest{
		NodeId:           "test-node",
		FabricGeneration: 9,
		OperationId:      "op-gran",
		NodePrefix:       "fd7a:9a55:1::/64",
		Identities: []*pb.IdentityMapping{
			// 注意顺序：worker 在前，api 在后——旧实现后者覆盖前者。
			{
				IdentityId:  21,
				ProjectId:   "p1",
				AppId:       "a2",
				Service:     "worker",
				Ula:         "fd7a:9a55:1::21",
				MachineId:   "m2",
				ExecutionId: "e2",
			},
			{
				IdentityId:  22,
				ProjectId:   "p1",
				AppId:       "a2",
				Service:     "api",
				Ula:         "fd7a:9a55:1::22",
				MachineId:   "m3",
				ExecutionId: "e3",
			},
			{
				IdentityId:  31,
				ProjectId:   "p1",
				AppId:       "a1",
				Service:     "web",
				Ula:         "fd7a:9a55:1::31",
				MachineId:   "m1",
				ExecutionId: "e1",
			},
			{
				IdentityId:  32,
				ProjectId:   "p1",
				AppId:       "a1",
				Service:     "admin",
				Ula:         "fd7a:9a55:1::32",
				MachineId:   "m1",
				ExecutionId: "e1",
			},
		},
		Eastwest: &pb.EastWestPolicySpec{Generation: 4, Rules: []*pb.EastWestPolicyRule{
			{
				SrcProject: "p1",
				SrcApp:     "a1",
				DstProject: "p1",
				DstApp:     "a2",
				DstService: "worker",
				Ports:      []uint32{9000},
			},
			{SrcProject: "p1", SrcApp: "a1", DstProject: "p1", DstApp: "a2", DstService: "ghost", Ports: []uint32{1}},
		}},
	}
	snap := fabricPolicyFromSnapshot(req)
	// worker 规则：src a1 的两个 service 各正向+回程 = 4 条；ghost 跳过。
	type pair struct{ src, dst uint32 }
	got := map[pair]bool{}
	for _, e := range snap.Entries {
		got[pair{e.SrcIdentity, e.DstIdentity}] = true
	}
	for _, want := range []pair{{31, 21}, {21, 31}, {32, 21}, {21, 32}} {
		if !got[want] {
			t.Fatalf("missing entry %+v in %+v", want, snap.Entries)
		}
	}
	if len(snap.Entries) != 4 {
		t.Fatalf("entries = %+v, want exactly 4", snap.Entries)
	}
	// dst 必须绑 worker(21)，不能落到 api(22)。
	for _, e := range snap.Entries {
		if e.SrcIdentity == 31 && e.DstIdentity != 21 {
			t.Fatalf("dst_service=worker resolved to identity %d, want 21", e.DstIdentity)
		}
	}
}

// TestFabricPolicyFromStateReplay：agentd 重启重放与接收路径共用
// BuildFabricPolicy——回程条目必须带 SrcPorts 端口集，identity 按 service
// 解析（旧重放实现按 app 折叠且回程无端口，重启后策略会松/错）。
func TestFabricPolicyFromStateReplay(t *testing.T) {
	snap := state.FabricSnapshot{
		NodeID: "n1", Generation: 7, NodePrefix: "fd7a:9a55:1::/64",
		EastWestGeneration: 3,
		Identities: []state.FabricIdentity{
			{IdentityID: 7, ProjectID: "p1", AppID: "a1", Service: "web", ULA: "fd7a:9a55:1::5"},
			{IdentityID: 9, ProjectID: "p1", AppID: "a2", Service: "worker", ULA: "fd7a:9a55:1::9"},
		},
		EastWest: []state.EastWestRule{{
			SrcProject: "p1", SrcApp: "a1", DstProject: "p1", DstApp: "a2",
			DstService: "worker", Ports: []uint32{9000},
		}},
	}
	out := FabricPolicyFromState(snap)
	if out.Generation != 7 || out.NodeULA != "fd7a:9a55:1::" {
		t.Fatalf("snapshot header = gen %d ula %q", out.Generation, out.NodeULA)
	}
	if len(out.Entries) != 2 {
		t.Fatalf("entries = %+v, want forward+reverse", out.Entries)
	}
	fwd, rev := out.Entries[0], out.Entries[1]
	if fwd.SrcIdentity != 7 || fwd.DstIdentity != 9 || fwd.SrcPorts || len(fwd.Ports) != 1 {
		t.Fatalf("forward = %+v", fwd)
	}
	if rev.SrcIdentity != 9 || rev.DstIdentity != 7 || !rev.SrcPorts || len(rev.Ports) != 1 || rev.Ports[0] != 9000 {
		t.Fatalf("reverse = %+v (must carry ports with SrcPorts)", rev)
	}
}
