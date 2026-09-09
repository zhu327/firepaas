package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeUnderlay 记录 UpdatePeers 调用（断言 server→underlay 接线正确）。
type fakeUnderlay struct {
	mu      sync.Mutex
	gens    []uint64
	peerIDs [][]string
	failN   int // 前 N 次调用失败（重试收敛测试）
}

func (f *fakeUnderlay) UpdatePeers(_ context.Context, gen uint64, peers []api.Peer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gens = append(f.gens, gen)
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.NodeID)
	}
	f.peerIDs = append(f.peerIDs, ids)
	if f.failN > 0 {
		f.failN--
		return errors.New("injected underlay failure")
	}
	return nil
}

func (f *fakeUnderlay) DialULA(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error) {
	return nil, errors.New("not used")
}

func TestApplyFabricDrivesUnderlay(t *testing.T) {
	srv, _, _ := newFabricServer(t)
	underlay := &fakeUnderlay{}
	srv.underlay = underlay

	req := fabricReq("op-1", 1, nil)
	if _, err := srv.ApplyFabric(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	underlay.mu.Lock()
	defer underlay.mu.Unlock()
	if len(underlay.gens) != 1 || underlay.gens[0] != 1 {
		t.Fatalf("underlay gens = %v", underlay.gens)
	}
	if len(underlay.peerIDs[0]) != 1 || underlay.peerIDs[0][0] != "node-b" {
		t.Fatalf("underlay peers = %v", underlay.peerIDs)
	}
}

func TestApplyFabricUnderlayFailureRetriesToConvergence(t *testing.T) {
	srv, ledger, _ := newFabricServer(t)
	underlay := &fakeUnderlay{failN: 1}
	srv.underlay = underlay
	ctx := context.Background()
	req := fabricReq("op-1", 1, nil)

	// 首次：underlay 失败 → Internal，claim 保持 in-progress（可恢复）。
	_, err := srv.ApplyFabric(ctx, req)
	if status.Code(err) != codes.Internal {
		t.Fatalf("first err = %v, want Internal", err)
	}
	rec, ok, err := ledger.Get(req.OperationId, hashRequest(req))
	if err != nil || !ok || rec.Completed() {
		t.Fatalf("claim after failure: rec=%+v ok=%v err=%v", rec, ok, err)
	}
	// 重试同 operation：fabric 幂等 + underlay 重试成功 → 收敛完成。
	out, err := srv.ApplyFabric(ctx, req)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if out.AppliedGeneration != 1 || !out.Replayed {
		t.Fatalf("retry out = %+v", out)
	}
	rec, _, _ = ledger.Get(req.OperationId, hashRequest(req))
	if !rec.Completed() {
		t.Fatal("claim not completed after retry")
	}
	underlay.mu.Lock()
	defer underlay.mu.Unlock()
	if len(underlay.gens) != 2 {
		t.Fatalf("underlay calls = %d, want 2 (fail + retry)", len(underlay.gens))
	}
}
