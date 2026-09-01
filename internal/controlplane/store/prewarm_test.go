// prewarm_test.go：v1.4-C store 层回归——prewarm 幂等入队、pin 节点作用域
// GC roots 与 pinned bytes 记账。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func seedNode(t *testing.T, s *Store, nodeID, pool string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `
		INSERT INTO nodes(id,nomad_node_id,node_pool,status,grpc_addr,proxy_addr) VALUES($1,$1,$2,'HEALTHY','','')
		ON CONFLICT (id) DO UPDATE SET node_pool=EXCLUDED.node_pool`, nodeID, pool); err != nil {
		t.Fatal(err)
	}
}

// v1.4-C：prewarm 入队幂等（同 op ID 重放不重复建行、不重复 target）。
func TestCreatePrewarmAndEnqueueIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-prewarm-store-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "prewarm-store-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_prewarm_targets WHERE operation_id LIKE 'op-prewarm-store%'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	digest := "sha256:" + repeatHex(0, 64)
	raw := []byte(fmt.Sprintf(`{"image_ref":"r/app@%s","digest":"%s","targets":["n1","n2"]}`, digest, digest))
	p := EnqueueOperationParams{OperationID: "op-prewarm-store-1", ProjectID: project, Kind: "image_prewarm", Request: raw}
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", p, []string{"n1", "n2"}, 4); err != nil {
		t.Fatal(err)
	}
	second, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", p, []string{"n1", "n2"}, 4)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if second.ID != "op-prewarm-store-1" || second.Status != "PENDING" {
		t.Fatalf("replay returned %+v", second)
	}
	targets, err := s.ListPrewarmTargets(ctx, "op-prewarm-store-1")
	if err != nil || len(targets) != 2 {
		t.Fatalf("targets=%d err=%v", len(targets), err)
	}
	// HTTP idempotency keys are project scoped and conflict on a changed request.
	p2 := p
	p2.OperationID = "op-prewarm-store-http-1"
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "http-key", p2, []string{"n1", "n2"}, 4); err != nil {
		t.Fatal(err)
	}
	p2.OperationID = "op-prewarm-store-http-2"
	replayed, err := s.CreatePrewarmAndEnqueue(ctx, digest, "http-key", p2, []string{"n1", "n2"}, 4)
	if err != nil || replayed.ID != "op-prewarm-store-http-1" {
		t.Fatalf("HTTP idempotency replay = %+v, %v", replayed, err)
	}
	p2.Request = []byte(`{"different":true}`)
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "http-key", p2, []string{"n1"}, 4); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("HTTP idempotency mismatch = %v, want ErrRequestConflict", err)
	}

	// Same operation ID with a changed request also conflicts.
	p.Request = []byte(`{"different":true}`)
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", p, []string{"n1"}, 4); err == nil {
		t.Fatal("request mismatch must conflict")
	}
}

// v1.4-C：并发上限在事务内强制（事务性检查，非 check-then-insert）。
func TestCreatePrewarmEnforcesActiveLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-prewarm-limit-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "prewarm-limit-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_prewarm_targets WHERE operation_id LIKE 'op-prewarm-limit%'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	digest := "sha256:" + repeatHex(7, 64)
	for i := 1; i <= 2; i++ {
		opID := fmt.Sprintf("op-prewarm-limit-%d", i)
		raw := []byte(fmt.Sprintf(`{"digest":"%s","targets":["n1"]}`, digest))
		if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", EnqueueOperationParams{
			OperationID: opID, ProjectID: project, Kind: "image_prewarm", Request: raw,
		}, []string{"n1"}, 2); err != nil {
			t.Fatalf("prewarm %d: %v", i, err)
		}
	}
	// 第三个在途 prewarm 被拒；终态后可再次入队。
	raw := []byte(fmt.Sprintf(`{"digest":"%s","targets":["n1"]}`, digest))
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", EnqueueOperationParams{
		OperationID: "op-prewarm-limit-3", ProjectID: project, Kind: "image_prewarm", Request: raw,
	}, []string{"n1"}, 2); !errors.Is(err, ErrPrewarmNotAllowed) {
		t.Fatalf("active limit not enforced: %v", err)
	}
	if err := s.CompleteOperation(ctx, "op-prewarm-limit-1", "SUCCEEDED", nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", EnqueueOperationParams{
		OperationID: "op-prewarm-limit-3", ProjectID: project, Kind: "image_prewarm", Request: raw,
	}, []string{"n1"}, 2); err != nil {
		t.Fatalf("prewarm after terminal op: %v", err)
	}
}

func TestImagePinCommandsAreIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-pin-idem-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "pin-idem-test"); err != nil {
		t.Fatal(err)
	}
	digest := testDigest(9)
	seedNode(t, s, "pin-idem-n1", "pin-idem-pool")
	if err := s.UpsertImageSize(ctx, digest, 10); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_pin_idempotency WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_pins WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_sizes WHERE digest=$1`, digest)
		_, _ = s.pool.Exec(ctx, `DELETE FROM nodes WHERE id='pin-idem-n1'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	limits := ImagePinLimits{MaxPins: 4, MaxBytesMib: 100, MaxTargets: 4, HardWatermark: .9}
	request := []byte(`{"digest":"same","ttl_seconds":60}`)
	batch := []ImagePin{{ID: "pin-idem-1", ProjectID: project, ImageDigest: digest, Selector: "node:pin-idem-n1", Owner: "test"}}
	first, err := s.CreateImagePinsAtomic(ctx, batch, time.Minute, "create-key", request, limits)
	if err != nil || len(first) != 1 {
		t.Fatalf("first pin = %+v, %v", first, err)
	}
	time.Sleep(time.Millisecond)
	batch[0].ID = "pin-idem-2"
	replay, err := s.CreateImagePinsAtomic(ctx, batch, time.Minute, "create-key", request, limits)
	if err != nil || len(replay) != 1 || replay[0].ID != first[0].ID || !replay[0].ExpiresAt.Equal(first[0].ExpiresAt) {
		t.Fatalf("pin replay = %+v, %v; first=%+v", replay, err, first)
	}
	if _, err := s.CreateImagePinsAtomic(ctx, batch, time.Minute, "create-key", []byte(`{"different":true}`), limits); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed pin request = %v", err)
	}

	unpinRequest := []byte(`{"pin_id":"pin-idem-1"}`)
	if err := s.DeleteImagePin(ctx, first[0].ID, project, "delete-key", unpinRequest); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteImagePin(ctx, first[0].ID, project, "delete-key", unpinRequest); err != nil {
		t.Fatalf("unpin replay: %v", err)
	}
	if err := s.DeleteImagePin(ctx, "other", project, "delete-key", []byte(`{"pin_id":"other"}`)); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed unpin request = %v", err)
	}
}

func TestPrewarmAttemptBudgetBecomesTerminal(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-prewarm-attempt-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "prewarm-attempt-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_prewarm_targets WHERE operation_id='op-prewarm-attempt'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	digest := testDigest(8)
	raw := []byte(fmt.Sprintf(`{"digest":%q}`, digest))
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digest, "", EnqueueOperationParams{OperationID: "op-prewarm-attempt", ProjectID: project, Kind: "image_prewarm", Request: raw}, []string{"n1"}, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE image_prewarm_targets SET max_attempts=2 WHERE operation_id='op-prewarm-attempt'`); err != nil {
		t.Fatal(err)
	}
	terminal, err := s.RecordPrewarmTargetAttemptFailure(ctx, "op-prewarm-attempt", "n1", "temporary")
	if err != nil || terminal {
		t.Fatalf("attempt 1 terminal=%v err=%v", terminal, err)
	}
	terminal, err = s.RecordPrewarmTargetAttemptFailure(ctx, "op-prewarm-attempt", "n1", "temporary")
	if err != nil || !terminal {
		t.Fatalf("attempt 2 terminal=%v err=%v", terminal, err)
	}
	targets, err := s.ListPrewarmTargets(ctx, "op-prewarm-attempt")
	if err != nil || len(targets) != 1 || targets[0].Status != "FAILED" || targets[0].Attempts != 2 {
		t.Fatalf("targets=%+v err=%v", targets, err)
	}
}

// v1.4-C：pin 的 GC roots 按节点计算——node pin 只保护该节点，pool pin 只
// 保护池内节点，过期 pin 不保护，在途 prewarm 目标保护。
func TestPinnedDigestsForNodeScope(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-pin-scope-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "pin-scope-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_pins WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_prewarm_targets WHERE operation_id LIKE 'op-pin%'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM nodes WHERE id IN ('pin-n1','pin-n2','pin-n3')`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedNode(t, s, "pin-n1", "pool-a")
	seedNode(t, s, "pin-n2", "pool-a")
	seedNode(t, s, "pin-n3", "pool-b")

	digestA := testDigest(0)
	if _, err := s.CreateImagePin(ctx, ImagePin{ID: "pin-1", ProjectID: project, ImageDigest: digestA,
		Selector: "node:pin-n1", Owner: "test", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	digestB := testDigest(1)
	if _, err := s.CreateImagePin(ctx, ImagePin{ID: "pin-2", ProjectID: project, ImageDigest: digestB,
		Selector: "node_pool:pool-a", Owner: "test", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	// 过期 pin 不保护（先有效创建，再过期）。
	digestC := testDigest(2)
	if _, err := s.CreateImagePin(ctx, ImagePin{ID: "pin-3", ProjectID: project, ImageDigest: digestC,
		Selector: "node:pin-n3", Owner: "test", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE image_pins SET expires_at=now()-interval '1 second' WHERE id='pin-3'`); err != nil {
		t.Fatal(err)
	}

	n1, err := s.PinnedDigestsForNode(ctx, "pin-n1", "pool-a")
	if err != nil {
		t.Fatal(err)
	}
	if !n1[digestA] || !n1[digestB] {
		t.Fatalf("n1 roots missing pinned digests: %v", n1)
	}
	n2, err := s.PinnedDigestsForNode(ctx, "pin-n2", "pool-a")
	if err != nil {
		t.Fatal(err)
	}
	if n2[digestA] || !n2[digestB] {
		t.Fatalf("n2 must see only pool pin: %v", n2)
	}
	n3, err := s.PinnedDigestsForNode(ctx, "pin-n3", "pool-b")
	if err != nil {
		t.Fatal(err)
	}
	if n3[digestA] || n3[digestB] || n3[digestC] {
		t.Fatalf("n3 must have no roots (its pin expired): %v", n3)
	}

	// 在途 prewarm 目标是保护对象；FAILED 目标不保护。
	digestD := testDigest(3)
	rawD := []byte(fmt.Sprintf(`{"image_ref":"r/app@%s","digest":"%s","targets":["pin-n3"]}`, digestD, digestD))
	if _, err := s.CreatePrewarmAndEnqueue(ctx, digestD, "", EnqueueOperationParams{
		OperationID: "op-pin-1", ProjectID: project, Kind: "image_prewarm", Request: rawD,
	}, []string{"pin-n3"}, 4); err != nil {
		t.Fatal(err)
	}
	n3, err = s.PinnedDigestsForNode(ctx, "pin-n3", "pool-b")
	if err != nil {
		t.Fatal(err)
	}
	if !n3[digestD] {
		t.Fatalf("in-flight prewarm digest must protect node: %v", n3)
	}
	if err := s.SetPrewarmTargetStatus(ctx, "op-pin-1", "pin-n3", "FAILED", "boom"); err != nil {
		t.Fatal(err)
	}
	n3, err = s.PinnedDigestsForNode(ctx, "pin-n3", "pool-b")
	if err != nil {
		t.Fatal(err)
	}
	if n3[digestD] {
		t.Fatalf("failed prewarm target must not protect node: %v", n3)
	}
}

// v1.4-C：pinned bytes 记账 = Σ size_mib × 当前 selector 命中节点数。
func TestPinnedBytesForProject(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-pin-bytes-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "pin-bytes-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_pins WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM image_sizes WHERE digest LIKE 'sha256:%'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM nodes WHERE id IN ('pb-n1','pb-n2')`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedNode(t, s, "pb-n1", "pool-b")
	seedNode(t, s, "pb-n2", "pool-b")

	digest := testDigest(10)
	if err := s.UpsertImageSize(ctx, digest, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateImagePin(ctx, ImagePin{ID: "pin-b1", ProjectID: project, ImageDigest: digest,
		Selector: "node_pool:pool-b", Owner: "t", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	// node pin upsert（同 project+digest+selector 幂等）。
	digest2 := testDigest(11)
	if err := s.UpsertImageSize(ctx, digest2, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateImagePin(ctx, ImagePin{ID: "pin-b2", ProjectID: project, ImageDigest: digest2,
		Selector: "node:pb-n1", Owner: "t", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.PinnedBytesForProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100*2+10*1 {
		t.Fatalf("pinned bytes = %d, want 210", got)
	}
	// 过期 pin 立即退出记账。
	if _, err := s.pool.Exec(ctx, `UPDATE image_pins SET expires_at=now()-interval '1 second' WHERE id='pin-b1'`); err != nil {
		t.Fatal(err)
	}
	got, err = s.PinnedBytesForProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if got != 10 {
		t.Fatalf("pinned bytes after expiry = %d, want 10", got)
	}
}

func repeatHex(seed, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + (seed+i)%6)
	}
	return string(out)
}

func testDigest(seed int) string { return "sha256:" + repeatHex(seed, 64) }
