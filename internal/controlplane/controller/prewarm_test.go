// prewarm_test.go：v1.4-C prewarm 派发回归（PG store + 可注入 puller）。
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakePuller struct {
	calls   int
	sizeMib uint64
	err     error // 每次调用返回同一错误（瞬时/永久由调用方给 codes）
}

func (f *fakePuller) PullImage(ctx context.Context, imageRef string) (*pb.PullImageResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &pb.PullImageResponse{ImageRef: imageRef, Digest: "sha256:" + digestHex, SizeMib: f.sizeMib}, nil
}

const digestHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func newPrewarmController(t *testing.T) (*Controller, *store.Store) {
	t.Helper()
	s := testStoreController(t)
	c := &Controller{store: s, metrics: metrics.New(),
		cfg: Config{AgentRPCTimeout: 5 * time.Second}}
	return c, s
}

// testStoreController 跑迁移并返回 store（controller 包内 PG-gated）。
func testStoreController(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run controller store tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool)
}

func enqueueTestPrewarm(t *testing.T, c *Controller, project, opID string, nodeIDs []string) store.Operation {
	t.Helper()
	ctx := context.Background()
	digest := "sha256:" + digestHex
	request := map[string]any{"image_ref": "registry.local/app@" + digest, "digest": digest, "targets": nodeIDs}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	op, err := c.store.CreatePrewarmAndEnqueue(ctx, digest, "", store.EnqueueOperationParams{
		OperationID: opID, ProjectID: project, Kind: "image_prewarm", Request: raw,
	}, nodeIDs, 4)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func TestDispatchPrewarmCompletesAndRecordsSizes(t *testing.T) {
	c, _ := newPrewarmController(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-prewarm-%d", os.Getpid())
	ensureControllerProject(t, c, project)
	op := enqueueTestPrewarm(t, c, project, "op-prewarm-ok", []string{"node-a", "node-b"})

	pullers := map[string]*fakePuller{
		"node-a": {sizeMib: 42},
		"node-b": {sizeMib: 42},
	}
	if err := c.dispatchPrewarm(ctx, op, func(nodeID string) prewarmPuller { return pullers[nodeID] }); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	targets, err := c.store.ListPrewarmTargets(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %d", len(targets))
	}
	for _, target := range targets {
		if target.Status != "SUCCEEDED" {
			t.Fatalf("target %s status = %s", target.NodeID, target.Status)
		}
	}
	// 重复派发（op 已终态，这里验证再次 dispatch 不重复拉取已完成节点）。
	if err := c.dispatchPrewarm(ctx, op, func(nodeID string) prewarmPuller { return pullers[nodeID] }); err != nil {
		t.Fatalf("re-dispatch: %v", err)
	}
	if pullers["node-a"].calls != 1 || pullers["node-b"].calls != 1 {
		t.Fatalf("completed targets must not be re-pulled: a=%d b=%d", pullers["node-a"].calls, pullers["node-b"].calls)
	}
	size, known, err := c.store.GetImageSize(ctx, "sha256:"+digestHex)
	if err != nil || !known || size != 42 {
		t.Fatalf("image size = %d known=%v err=%v", size, known, err)
	}
}

func TestDispatchPrewarmRetriesTransientAndFailsPermanent(t *testing.T) {
	c, _ := newPrewarmController(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-prewarm2-%d", os.Getpid())
	ensureControllerProject(t, c, project)
	op := enqueueTestPrewarm(t, c, project, "op-prewarm-mixed", []string{"node-a", "node-b", "node-c"})

	pullers := map[string]*fakePuller{
		"node-a": {sizeMib: 10},
		"node-b": {err: status.Error(codes.Unavailable, "agent restarting")}, // 瞬时
		"node-c": {err: status.Error(codes.NotFound, "manifest unknown")},    // 确定性
	}
	resolve := func(nodeID string) prewarmPuller { return pullers[nodeID] }

	// 第一轮：a 成功、b 瞬时失败、c 终态失败 → op 未完成。
	if err := c.dispatchPrewarm(ctx, op, resolve); err == nil {
		t.Fatal("dispatch with outstanding transient target must not complete")
	}
	targets, _ := c.store.ListPrewarmTargets(ctx, op.ID)
	byNode := map[string]string{}
	for _, target := range targets {
		byNode[target.NodeID] = target.Status
	}
	if byNode["node-a"] != "SUCCEEDED" || byNode["node-b"] != "PENDING" || byNode["node-c"] != "FAILED" {
		t.Fatalf("statuses after first pass: %v", byNode)
	}
	// 第二轮：b 恢复 → op 完成；a 不重复拉取。
	pullers["node-b"] = &fakePuller{sizeMib: 10}
	if err := c.dispatchPrewarm(ctx, op, resolve); err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	targets, _ = c.store.ListPrewarmTargets(ctx, op.ID)
	succeeded := 0
	for _, target := range targets {
		if target.Status == "SUCCEEDED" {
			succeeded++
		}
	}
	if succeeded != 2 {
		t.Fatalf("succeeded targets = %d, want 2 (a,b; c stays FAILED)", succeeded)
	}
	if pullers["node-a"].calls != 1 {
		t.Fatalf("node-a re-pulled %d times", pullers["node-a"].calls)
	}
}

func TestDispatchPrewarmUnreachableNodeStaysPending(t *testing.T) {
	c, _ := newPrewarmController(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-prewarm3-%d", os.Getpid())
	ensureControllerProject(t, c, project)
	op := enqueueTestPrewarm(t, c, project, "op-prewarm-unreach", []string{"node-x"})

	err := c.dispatchPrewarm(ctx, op, func(string) prewarmPuller { return nil })
	if err == nil {
		t.Fatal("unreachable node must keep the operation retryable")
	}
	targets, _ := c.store.ListPrewarmTargets(ctx, op.ID)
	if targets[0].Status != "PENDING" {
		t.Fatalf("unreachable node status = %s, want PENDING", targets[0].Status)
	}
	var terminal bool
	_ = c.store.Pool().QueryRow(ctx, `SELECT status='SUCCEEDED' FROM operations WHERE id=$1`, op.ID).Scan(&terminal)
	if terminal {
		t.Fatal("operation must not be terminal while a node is outstanding")
	}
}

func ensureControllerProject(t *testing.T, c *Controller, project string) {
	t.Helper()
	if err := c.store.EnsureProject(context.Background(), project, "prewarm-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		stmts := []string{
			`DELETE FROM image_prewarm_targets WHERE operation_id IN (SELECT id FROM operations WHERE project_id=$1)`,
			`DELETE FROM image_pins WHERE project_id=$1`,
			`DELETE FROM image_sizes`,
			`DELETE FROM operations WHERE project_id=$1`,
			`DELETE FROM projects WHERE id=$1`,
		}
		for _, q := range stmts {
			_, _ = c.store.Pool().Exec(ctx, q, project)
		}
	})
}
