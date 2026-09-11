// idempotency_conflict_test.go：review 2026-09-10 的幂等键回归。
//
// 语义：同一 (project_id, idempotency_key) 携带不同请求体必须返回
// ErrRequestConflict，任何路径都不得静默返回“赢家”的结果。
// 预检与 INSERT 之间存在并发窗口（DO NOTHING 不报错），因此插入后的重读
// 必须再次比对；本测试同时覆盖顺序路径与并发路径。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
)

func TestEnqueueCreateSameKeyDifferentRequestRejected(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-idem-" + suffix
	if err := s.EnsureProject(ctx, project, "idem"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	enqueue := func(body string) (Operation, error) {
		return s.EnsureAppAndEnqueueCreate(ctx, project, "app-idem", "idem.local", "img:1",
			1, 512, 1024, 80, "m-idem-"+suffix, "dep-idem", "exec-idem",
			"op-idem-"+suffix, 1, 0, []byte(body), nil)
	}

	first, err := enqueue(`{"generation":"1","note":"a"}`)
	if err != nil {
		t.Fatal(err)
	}
	// 同 key 同 body：幂等返回同一 operation。
	again, err := enqueue(`{"note":"a","generation":"1"}`) // 键序不同，语义相同
	if err != nil {
		t.Fatalf("same request must be idempotent: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("idempotent replay returned %s, want %s", again.ID, first.ID)
	}
	// 同 key 不同 body：必须 409 语义（ErrRequestConflict），不得返回旧结果。
	if _, err := enqueue(`{"generation":"1","note":"b"}`); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("different request with same key: err = %v, want ErrRequestConflict", err)
	}
}

// TestEnqueueOperationConflictAfterPrecheck 确定性地复现 TOCTOU：
//
//  1. 外部事务 tx1 插入 (project, key) 行但不提交（制造“已入队但不可见”）；
//  2. EnqueueOperation 的预检在 READ COMMITTED 下看不到 tx1 的行 → 通过；
//  3. 它的 INSERT ... ON CONFLICT DO NOTHING 阻塞在 tx1 的 transactionid 上；
//  4. 提交 tx1 后，DO NOTHING 静默完成，重读拿到 tx1 的行。
//
// 修复前：返回 tx1 的 request + nil error（静默错结果）；
// 修复后：重读比对 → ErrRequestConflict。
func TestEnqueueOperationConflictAfterPrecheck(t *testing.T) {
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run idempotency race test")
	}
	ctx := context.Background()

	// 专用连接池 + application_name，用于在 pg_stat_activity 中精确识别
	// 正在等待锁的后端（不同包测试可能并行，不能用全局 Lock 计数）。
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "fp-idem-race-test"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	s := New(pool)

	suffix := fmt.Sprint(os.Getpid())
	project := "p-idem-race-" + suffix
	if err := s.EnsureProject(ctx, project, "idem-race"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close) // LIFO：先跑 cleanupProject，再关池。
	t.Cleanup(func() { cleanupProject(t, s, project) })
	opID := "op-idem-race-" + suffix

	// tx1 抢占用独立连接（与 s 的池不同后端）。
	tx1, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx1.Rollback(context.Background()) }()
	if _, err := tx1.Exec(ctx, `
		INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
			kind, idempotency_key, status, request, dispatch_node_id)
		VALUES($1,$2,$3,$4,1,'snapshot_create',$1,'PENDING',$5::jsonb,'node-x')`,
		opID, project, "m-idem-race-"+suffix, "exec-idem-race", `{"client":"winner"}`); err != nil {
		t.Fatal(err)
	}

	type result struct {
		op  Operation
		err error
	}
	done := make(chan result, 1)
	go func() {
		op, err := s.EnqueueOperation(ctx, EnqueueOperationParams{
			OperationID: opID, ProjectID: project,
			MachineID: "m-idem-race-" + suffix, ExecutionID: "exec-idem-race",
			Generation: 1, Kind: "snapshot_create",
			Request: []byte(`{"client":"loser"}`), DispatchNodeID: "node-x",
		})
		done <- result{op: op, err: err}
	}()

	// 等到 goroutine 确实阻塞在 tx1 的 transactionid 上（可观察的确定性同步点）。
	waitForLockWait(t, ctx, pool, opID)
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	got := <-done
	if got.err == nil {
		t.Fatalf("concurrent different request must be rejected, got op request %s", got.op.Request)
	}
	if !errors.Is(got.err, ErrRequestConflict) {
		t.Fatalf("err = %v, want ErrRequestConflict", got.err)
	}
}

// waitForLockWait 轮询 pg_stat_activity，直到专属于本测试的后端进入 Lock 等待。
// 若未观察到，测试失败（说明同步点没建立，而不是默默跳过）。
func waitForLockWait(t *testing.T, ctx context.Context, pool *pgxpool.Pool, opID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name='fp-idem-race-test'
			  AND wait_event_type='Lock' AND state='active'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("enqueue %s never blocked on tx1 lock; race window not established", opID)
}

func TestEnqueueCreateConcurrentSameKeyDifferentBodyNeverSilentlyWins(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-idem-conc-" + suffix
	if err := s.EnsureProject(ctx, project, "idem-conc"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	// 首次创建（app/machine 均不存在）时没有行锁串行化，是最容易命中
	// “预检双双通过、DO NOTHING 静默收敛”的窗口。所有 goroutine 使用同一
	// app/machine/opID，只有 request body 不同。
	const workers = 8
	bodies := make([]string, workers)
	for i := range bodies {
		bodies[i] = fmt.Sprintf(`{"generation":"1","client":%d}`, i)
	}
	start := make(chan struct{})
	type result struct {
		op  Operation
		err error
		i   int
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			op, err := s.EnsureAppAndEnqueueCreate(ctx, project, "app-idem-conc", "idem-conc.local", "img:1",
				1, 512, 1024, 80, "m-idem-conc-"+suffix, "dep-idem-conc", "exec-idem-conc",
				"op-idem-conc-"+suffix, 1, 0, []byte(bodies[i]), nil)
			results <- result{op: op, err: err, i: i}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for r := range results {
		switch {
		case r.err == nil:
			successes++
			if !jsonEqual(r.op.Request, []byte(bodies[r.i])) {
				t.Fatalf("worker %d received op %s with request %s, want its own body %s",
					r.i, r.op.ID, r.op.Request, bodies[r.i])
			}
		case errors.Is(r.err, ErrRequestConflict):
			// 合法拒绝。
		default:
			t.Fatalf("worker %d unexpected error: %v", r.i, r.err)
		}
	}
	if successes == 0 {
		t.Fatal("at least one enqueue must succeed")
	}
}
