// dispatch_r2_test.go：R2 评审加固的行为测试——
//   - 有界并发派发：挂死 op 不拖累其他 machine；同 machine 串行；
//   - 生命周期 fenced 派发：fence 漂移 → SUPERSEDED 且零 RPC；
//   - secrets 主密钥 fail-closed：Secrets==nil 下的 secret-bearing create
//     直接 FAILED，不发起任何 agent RPC。
package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/nodemanager"
	"github.com/zhu327/firepaas/internal/controlplane/reservations"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	"github.com/zhu327/firepaas/internal/scheduler"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ---------------------------------------------------------------------------
// 有界并发派发（纯函数，无 PG）
// ---------------------------------------------------------------------------

func newLockController() *Controller {
	return &Controller{machineLocks: map[string]*machineDispatchLock{}}
}

// 一个挂死的操作不得拖累其他 machine：4 worker 下全部快 op 必须在挂死
// op 完成之前处理完毕（串行语义下会等挂死 op 结束才开始）。
func TestDispatchBoundedHungOpDoesNotBlockOtherMachines(t *testing.T) {
	const hang = 500 * time.Millisecond
	ops := []store.Operation{{ID: "op-hang", MachineID: "m-hang"}}
	for i := 0; i < 8; i++ {
		ops = append(ops, store.Operation{ID: fmt.Sprintf("op-fast-%d", i), MachineID: fmt.Sprintf("m-fast-%d", i)})
	}
	c := newLockController()
	var mu sync.Mutex
	muHangDone := false
	var fastDoneAt []time.Time
	process := func(ctx context.Context, op store.Operation) {
		mu.Lock()
		h := hang
		if op.MachineID != "m-hang" {
			h = 10 * time.Millisecond
		}
		mu.Unlock()
		time.Sleep(h)
		mu.Lock()
		if op.MachineID == "m-hang" {
			muHangDone = true
		} else if !muHangDone {
			fastDoneAt = append(fastDoneAt, time.Now())
		}
		mu.Unlock()
	}
	start := time.Now()
	dispatchBounded(context.Background(), ops, 4, c.lockMachine, process)
	elapsed := time.Since(start)
	mu.Lock()
	defer mu.Unlock()
	if len(fastDoneAt) != 8 {
		t.Fatalf("all 8 fast ops must complete before the hung op finished, got %d", len(fastDoneAt))
	}
	// 串行耗时 ≥ hang + 8×10ms；并发上界 ≈ hang（挂死 op 占据一个 worker）。
	if elapsed >= hang+8*10*time.Millisecond {
		t.Fatalf("dispatch took %v; serial semantics would take >= %v", elapsed, hang+8*10*time.Millisecond)
	}
}

// 同一 machine 的 op 必须串行：第二个同机 op 只能在第一个释放锁后开始。
func TestDispatchBoundedSerializesSameMachine(t *testing.T) {
	ops := []store.Operation{
		{ID: "op-a", MachineID: "m-1"},
		{ID: "op-b", MachineID: "m-1"},
		{ID: "op-c", MachineID: "m-2"},
	}
	c := newLockController()
	var mu sync.Mutex
	var aDone, bStart time.Time
	process := func(ctx context.Context, op store.Operation) {
		mu.Lock()
		defer mu.Unlock()
		// 避免持锁睡眠的权限问题：处理闭包内短暂占用即可。
		switch op.ID {
		case "op-a":
			mu.Unlock()
			time.Sleep(150 * time.Millisecond)
			mu.Lock()
			aDone = time.Now()
		case "op-b":
			bStart = time.Now()
		}
	}
	dispatchBounded(context.Background(), ops, 3, c.lockMachine, process)
	mu.Lock()
	defer mu.Unlock()
	if aDone.IsZero() || bStart.IsZero() {
		t.Fatal("both ops must have been processed")
	}
	if bStart.Before(aDone) {
		t.Fatalf("same-machine op b started %v before op a finished %v", bStart, aDone)
	}
}

// ---------------------------------------------------------------------------
// PG-gated：fence 漂移与 secrets fail-closed
// ---------------------------------------------------------------------------

// testPGStore：跑过迁移的真实 PG store；未设置 FIREPAAS_TEST_POSTGRES 时跳过。
func testPGStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run controller dispatch tests")
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
	return store.New(pool), pool
}

// seedR2App 造最小 project+app 行。
func seedR2App(t *testing.T, s *store.Store, ctx context.Context, project, appID string) {
	t.Helper()
	if err := s.EnsureProject(ctx, project, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
		VALUES($1,$2,$3,'img',1,512,1,1) ON CONFLICT (id) DO NOTHING`,
		appID, project, appID+".test"); err != nil {
		t.Fatal(err)
	}
}

// insertR2Machine 按显式 execution/generation 造 machine 行。
func insertR2Machine(t *testing.T, s *store.Store, ctx context.Context,
	appID, machineID, deploymentID, executionID string, generation int64, nodeID string,
) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
			desired_state, generation, current_execution_id, requested_vcpu,
			requested_mem_mib, image_ref, node_id, ingress_port)
		VALUES($1,$2,$3,0,$4,'CREATED',$5,$6,1,512,'img',$7,8080)`,
		machineID, appID, deploymentID, machineID+".test", generation, executionID, nodeID); err != nil {
		t.Fatal(err)
	}
}

// insertR2OpInFlight 造一条 CLAIMED 状态的 op（模拟 reconcile 已领取后的在途行）。
func insertR2OpInFlight(t *testing.T, s *store.Store, ctx context.Context,
	project, opID, machineID, executionID string, generation int64, kind string, request []byte,
) {
	t.Helper()
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO operations(id, project_id, machine_id, execution_id, generation,
			kind, idempotency_key, status, request, claimed_at)
		VALUES($1,$2,$3,$4,$5,$6,$1,'CLAIMED',$7::jsonb, now())`,
		opID, project, machineID, executionID, generation, kind, string(request)); err != nil {
		t.Fatal(err)
	}
}

func opStatus(t *testing.T, s *store.Store, ctx context.Context, opID string) (status, errText string) {
	t.Helper()
	if err := s.Pool().QueryRow(ctx,
		`SELECT status, coalesce(error,'') FROM operations WHERE id=$1`, opID).
		Scan(&status, &errText); err != nil {
		t.Fatal(err)
	}
	return status, errText
}

// P0#3（fenced lifecycle 派发）：op 携带的 fence（execution, generation）已
// 落后于 machine 当前 fence 时，op 置 SUPERSEDED 终态，且绝不发起 agent RPC
// （无节点客户端的情况下，若走到派发必然报错回退 CLAIMED/PENDING——完整的
// SUPERSEDED 收敛本身就证明零 RPC）。
func TestProcessLifecycleFenceDriftSuperseded(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "t-lifefence-p" + sfx
	appID := "t-lifefence-app" + sfx
	machineID := "t-lifefence-m" + sfx
	opID := "t-lifefence-op" + sfx
	seedR2App(t, s, ctx, project, appID)
	// 当前 fence：(exec-2, gen 2)；在途 op 针对 (exec-1, gen 1)。
	insertR2Machine(t, s, ctx, appID, machineID, "dep-lifefence"+sfx, "e2-"+sfx, 2, "node-x")
	req := fmt.Sprintf(`{"machine_id":%q,"execution_id":"e1-%s"}`, machineID, sfx)
	insertR2OpInFlight(t, s, ctx, project, opID, machineID, "e1-"+sfx, 1, "pause", []byte(req))

	nm, err := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	defer nm.Close()
	c := &Controller{
		store: s, nodes: nm, metrics: metrics.New(),
		cfg: Config{AgentRPCTimeout: 100 * time.Millisecond},
	}

	op := store.Operation{
		ID: opID, ProjectID: project, MachineID: machineID,
		ExecutionID: "e1-" + sfx, Generation: 1, Kind: "pause",
		Request: []byte(req),
	}
	if err := c.processLifecycle(ctx, op); err != nil {
		t.Fatalf("fence drift must converge as SUPERSEDED without RPC, got error: %v", err)
	}
	status, errText := opStatus(t, s, ctx, opID)
	if status != "SUPERSEDED" {
		t.Fatalf("op status = %q (err=%q), want SUPERSEDED", status, errText)
	}
	// machine 行未被旧代写回。
	m, err := s.GetMachine(ctx, machineID)
	if err != nil || m == nil {
		t.Fatalf("machine lost: %v", err)
	}
	if m.CurrentExecutionID != "e2-"+sfx || m.Generation != 2 {
		t.Fatalf("machine fence altered: exec=%q gen=%d", m.CurrentExecutionID, m.Generation)
	}
}

// P0（secrets fail-closed）：cfg.Secrets==nil 且 deployment 带 secret_refs
// 时 op 直接 FAILED，不发起任何 placement / agent RPC。
func TestProcessCreateSecretsFailClosed(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "t-secfence-p" + sfx
	appID := "t-secfence-app" + sfx
	depID := "dep-secfence" + sfx
	machineID := "t-secfence-m" + sfx
	opID := "t-secfence-op" + sfx
	seedR2App(t, s, ctx, project, appID)
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, vcpu, mem_mib, port, env, status, secret_refs)
		VALUES($1,$2,1,'img',1,512,8080,'{}'::jsonb,'ACTIVE',$3::jsonb)`,
		depID, appID, `{"DB_PASSWORD":{"secret":"db-pass"}}`); err != nil {
		t.Fatal(err)
	}
	insertR2Machine(t, s, ctx, appID, machineID, depID, "e1-"+sfx, 1, "")

	req := &pb.CreateMachineRequest{
		MachineId: machineID, Generation: 1, OperationId: opID,
		Spec: &pb.MachineSpec{
			ProjectId: project, AppId: appID, DeploymentId: depID,
			ExecutionId: "e1-" + sfx, ImageRef: "img", Vcpu: 1, MemMib: 512,
		},
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	insertR2OpInFlight(t, s, ctx, project, opID, machineID, "e1-"+sfx, 1, "create", raw)

	nm, err2 := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err2 != nil {
		t.Fatal(err2)
	}
	defer nm.Close()
	c := newR2Controller(t, nm, s)

	op := store.Operation{
		ID: opID, ProjectID: project, MachineID: machineID,
		ExecutionID: "e1-" + sfx, Generation: 1, Kind: "create",
		Request: raw,
	}
	if err := c.processCreate(ctx, op); err != nil {
		t.Fatalf("fail-closed must converge the op, not requeue: %v", err)
	}
	status, errText := opStatus(t, s, ctx, opID)
	if status != "FAILED" {
		t.Fatalf("op status = %q (err=%q), want FAILED", status, errText)
	}
	if !strings.Contains(errText, "FIREPAAS_SECRETS_MASTER_KEY") {
		t.Fatalf("failure reason must name the missing master key, got %q", errText)
	}
	// “零 agent RPC”结构性保证：若 had reached placement/RPC，无节点投影
	// 下失败文案会是 placement/no candidates 这类，而非上面的明确拒绝理由，
	// 且 op 不会进入终态（会 requeue）。
}

// 对照组：deployment 不带 secret_refs 时 fail-closed 守卫不触发
// （失败理由必须来自后续 placement 阶段，而非 master key 提示）。
func TestProcessCreateSecretlessNotBlocked(t *testing.T) {
	s, _ := testPGStore(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "t-secpass-p" + sfx
	appID := "t-secpass-app" + sfx
	depID := "dep-secpass" + sfx
	machineID := "t-secpass-m" + sfx
	opID := "t-secpass-op" + sfx
	seedR2App(t, s, ctx, project, appID)
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, vcpu, mem_mib, port, env, status, secret_refs)
		VALUES($1,$2,1,'img',1,512,8080,'{}'::jsonb,'ACTIVE','{}'::jsonb)`,
		depID, appID); err != nil {
		t.Fatal(err)
	}
	insertR2Machine(t, s, ctx, appID, machineID, depID, "e1-"+sfx, 1, "")
	req := &pb.CreateMachineRequest{
		MachineId: machineID, Generation: 1, OperationId: opID,
		Spec: &pb.MachineSpec{
			ProjectId: project, AppId: appID, DeploymentId: depID,
			ExecutionId: "e1-" + sfx, ImageRef: "img", Vcpu: 1, MemMib: 512,
		},
	}
	raw, err := protojson.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	insertR2OpInFlight(t, s, ctx, project, opID, machineID, "e1-"+sfx, 1, "create", raw)

	nm2, err := nodemanager.New(nodemanager.Config{NomadAddr: "http://127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	defer nm2.Close()
	c := newR2Controller(t, nm2, s)
	op := store.Operation{
		ID: opID, ProjectID: project, MachineID: machineID,
		ExecutionID: "e1-" + sfx, Generation: 1, Kind: "create",
		Request: raw,
	}
	// 不触发 fail-closed：进入既有 placement 路径（无节点 → 本轮失败 requeue）。
	_ = c.processCreate(ctx, op)
	status, errText := opStatus(t, s, ctx, opID)
	if status == "FAILED" && strings.Contains(errText, "FIREPAAS_SECRETS_MASTER_KEY") {
		t.Fatalf("secret-less deployment must not hit the fail-closed guard, got %q", errText)
	}
}

// newR2Controller 装配最小可用 controller：cfg.Secrets 恒为 nil（master key
// 缺失场景），其他依赖齐全以避免空指针（placement 需要 resv/placer）。
func newR2Controller(t *testing.T, nm *nodemanager.Manager, s *store.Store) *Controller {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = rdb.Close() })
	placer := scheduler.New(scheduler.DefaultBestOfKConfig(), scheduler.Options{})
	return New(s, catalog.New(rdb), nm, reservations.New(rdb, time.Second), placer,
		metrics.New(), Config{Secrets: nil, AgentRPCTimeout: 100 * time.Millisecond})
}

// ---------------------------------------------------------------------------
// 旧代 delete 收敛（真机验收发现的 reap 活锁）
// ---------------------------------------------------------------------------

func TestDeleteErrorConverges(t *testing.T) {
	pre := status.Error(codes.FailedPrecondition, "delete execution mismatch: ...")
	internal := status.Error(codes.Internal, "staged delete incomplete")
	machineDeleted := &store.Machine{DesiredState: "DELETED", CurrentExecutionID: "exec-new"}
	machineLive := &store.Machine{DesiredState: "CREATED", CurrentExecutionID: "exec-new"}
	cases := []struct {
		name string
		err  error
		m    *store.Machine
		exec string
		want bool
	}{
		{"superseded-ish: machine moved on + desired DELETED", pre, machineDeleted, "exec-old", true},
		// D3 验收发现：reap 旧代但 machine 仍存活时也要收敛——agent fencing
		// 已证明派发节点上无此 execution（op 不会触及当前代）；其他节点
		// 若持副本，由按节点 pin 的对账 reaps 另行清理。不收敛=活锁+阻塞 evacuate。
		{"non-current execution reap: machine live 也收敛", pre, machineLive, "exec-old", true},
		{"current execution 的 mismatch（污染）: 不收敛", pre, machineDeleted, "exec-new", false},
		{"Internal 阶段失败: 重试不收敛", internal, machineDeleted, "exec-old", false},
		// machine 行已 purge：op 是唯一证据；fencing mismatch 证明派发节点无此
		// execution；不收敛则 op 永久循环（D3）。
		{"nil machine（行已 purge）: 收敛", pre, nil, "exec-old", true},
	}
	for _, tc := range cases {
		if got := deleteErrorConverges(tc.err, tc.m, tc.exec); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}
