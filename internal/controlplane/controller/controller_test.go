package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequiredFeaturesForDeploymentInvalidEgressFailsClosed(t *testing.T) {
	got := requiredFeaturesForDeployment(&store.Deployment{EgressPolicy: []byte(`{"mode":`)})
	if len(got) != 1 || got[0] != "egress.invalid-policy" {
		t.Fatalf("required features = %v", got)
	}
}

func TestIsPermanentAgentError(t *testing.T) {
	permanent := []codes.Code{
		codes.InvalidArgument,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.NotFound,
	}
	for _, c := range permanent {
		if !isPermanentAgentError(status.Error(c, "x")) {
			t.Errorf("code %v should be permanent", c)
		}
	}
	transient := []codes.Code{
		codes.Unavailable, // agent 暂时不可达：应 requeue
		codes.DeadlineExceeded,
		codes.Internal, // 未知内部错误：保守重试
	}
	for _, c := range transient {
		if isPermanentAgentError(status.Error(c, "x")) {
			t.Errorf("code %v should be transient", c)
		}
	}
	// 非 gRPC 错误（如网络层错误包装）按暂时性处理。
	if isPermanentAgentError(errPlain{}) {
		t.Error("plain error should be transient")
	}
}

func TestSecretLeaseConfirmsCreate(t *testing.T) {
	now := time.Now()
	op := store.Operation{
		ID:          "op-1",
		ProjectID:   "p-1",
		MachineID:   "m-1",
		ExecutionID: "e-1",
		Generation:  2,
		Request:     []byte(`{"x":1}`),
	}
	lease := &store.SecretLease{
		ProjectID: "p-1", MachineID: "m-1", ExecutionID: "e-1", Generation: 2,
		OperationID: "op-1", RequestHash: hashRequestPayload(op.Request),
		State: store.SecretLeaseDelivered, ExpiresAt: now.Add(time.Minute),
	}
	if !secretLeaseConfirmsCreate(lease, op, now) {
		t.Fatal("strictly bound delivered lease should confirm create")
	}
	for _, mutate := range []func(*store.SecretLease){
		func(l *store.SecretLease) { l.MachineID = "other" },
		func(l *store.SecretLease) { l.State = store.SecretLeaseClaimed },
		func(l *store.SecretLease) { l.ExpiresAt = now.Add(-time.Second) },
	} {
		copy := *lease
		mutate(&copy)
		if secretLeaseConfirmsCreate(&copy, op, now) {
			t.Fatalf("invalid lease confirmed create: %+v", copy)
		}
	}
}

type errPlain struct{}

func (errPlain) Error() string { return "plain" }

// P1-3：R3 尾部决策表——只有 ACK 丢失与清理完成才换代重建；
// create FAILED 走退避重派；其余不动作。
func TestRecreateAction(t *testing.T) {
	const (
		grace   = 30 * time.Second
		backoff = 10 * time.Second
	)
	cases := []struct {
		name         string
		kind, status string
		since        time.Duration
		want         string
	}{
		{"recent create success waits (normal init)", "create", "SUCCEEDED", 5 * time.Second, actionWait},
		{"stale create success recreates (ack lost)", "create", "SUCCEEDED", 31 * time.Second, actionRecreate},
		{"recent create failure waits (backoff)", "create", "FAILED", 5 * time.Second, actionWait},
		{"stale create failure retries same exec", "create", "FAILED", 11 * time.Second, actionRetryCreate},
		{"reap done recreates", "delete", "SUCCEEDED", time.Second, actionRecreate},
		{"failed delete none (resurrect path owns it)", "delete", "FAILED", 10 * time.Minute, actionNone},
		{"in-flight state none (defensive)", "create", "CLAIMED", time.Hour, actionNone},
		{"unknown history none", "", "", time.Hour, actionNone},
	}
	for _, tc := range cases {
		if got := recreateAction(tc.kind, tc.status, tc.since, grace, backoff); got != tc.want {
			t.Errorf("%s: recreateAction(%s,%s,%v) = %s, want %s",
				tc.name, tc.kind, tc.status, tc.since, got, tc.want)
		}
	}
}

// P1-3：退避序列 base·2^(n-1) 封顶 max，首次重派不放大（不影Ⅱ 2 分钟验收）。
func TestNewNodeStaleAfterDefaultsToThreeSyncIntervals(t *testing.T) {
	c := New(nil, nil, nil, nil, nil, nil, Config{SyncInterval: 7 * time.Second})
	if got, want := c.cfg.NodeStaleAfter, 21*time.Second; got != want {
		t.Fatalf("NodeStaleAfter=%v, want %v", got, want)
	}

	c = New(nil, nil, nil, nil, nil, nil, Config{
		SyncInterval:   7 * time.Second,
		NodeStaleAfter: time.Minute,
	})
	if got := c.cfg.NodeStaleAfter; got != time.Minute {
		t.Fatalf("explicit NodeStaleAfter=%v, want 1m", got)
	}
}

func TestCreateRetryDelay(t *testing.T) {
	c := &Controller{cfg: Config{CreateRetryBase: 10 * time.Second, CreateRetryMax: 5 * time.Minute}}
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 10 * time.Second},
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
		{5, 160 * time.Second},
		{6, 5 * time.Minute}, // 320s > cap
		{100, 5 * time.Minute},
	}
	for _, tc := range cases {
		if got := c.createRetryDelay(tc.attempts); got != tc.want {
			t.Errorf("attempts=%d: delay=%v, want %v", tc.attempts, got, tc.want)
		}
	}
}

// TestRestartExitClassAllows 覆盖 v1.2-D（ADR-0026）exit class 决策：
// ON_FAILURE 不重启正常退出，ALWAYS 重启正常退出。
func TestRestartExitClassAllows(t *testing.T) {
	zero, nonzero := int32(0), int32(1)
	cases := []struct {
		mode     string
		exitCode *int32
		want     bool
	}{
		{"NEVER", &zero, false},
		{"NEVER", &nonzero, false},
		{"ON_FAILURE", nil, false},
		{"ON_FAILURE", &zero, false},
		{"ON_FAILURE", &nonzero, true},
		{"ALWAYS", nil, true},
		{"ALWAYS", &zero, true},
	}
	for _, c := range cases {
		if got := restartExitClassAllows(c.mode, c.exitCode); got != c.want {
			t.Errorf("restartExitClassAllows(%s, %v) = %v, want %v", c.mode, c.exitCode, got, c.want)
		}
	}
}

func TestMachineQuotaIncludesCurrentOperationWithoutExtraIncrement(t *testing.T) {
	if machineQuotaExceeded(1, 1) {
		t.Fatal("current create is already included in usage and must fit limit 1")
	}
	if machineQuotaExceeded(1, 1) {
		t.Fatal("restart replacement has zero machine increment")
	}
	if !machineQuotaExceeded(2, 1) {
		t.Fatal("usage above limit must be rejected")
	}
}

// rolloutHoldsTarget 纯函数化 PREPARING/CUTOVER/ROLLING_BACK 的 owner 判定
// （v1.2-D review 修复：active rollout 负责 target replica，其死亡走 rollout
// create retry，不消耗 restart attempts）。
func TestRolloutHoldsTarget(t *testing.T) {
	cases := []struct {
		name     string
		status   string
		depGen   int64
		from, to int64
		want     bool
	}{
		// PREPARING：to-generation 是 rollout 的责任（修复点）。
		{"preparing holds target", "PREPARING", 2, 1, 2, true},
		{"preparing releases source", "PREPARING", 1, 1, 2, false},
		// CUTOVER：旧代被 hold，新代（target）继续由 rollout 编排。
		{"cutover holds old gen", "CUTOVER", 1, 1, 2, true},
		{"cutover releases target", "CUTOVER", 2, 1, 2, false},
		// ROLLING_BACK：新代被 hold，回滚目标（from）继续由 rollout 编排。
		{"rollback holds new gen", "ROLLING_BACK", 2, 1, 2, true},
		{"rollback releases from gen", "ROLLING_BACK", 1, 1, 2, false},
		{"complete releases all", "COMPLETE", 2, 1, 2, false},
	}
	for _, tc := range cases {
		got := rolloutHoldDecision(tc.status, tc.depGen, tc.from, tc.to)
		if got != tc.want {
			t.Errorf("%s: rolloutHoldDecision(%s, dep=%d, %d->%d) = %v, want %v",
				tc.name, tc.status, tc.depGen, tc.from, tc.to, got, tc.want)
		}
	}
}

func TestRolloutOwnsReplacement(t *testing.T) {
	cases := []struct {
		status        string
		dep, from, to int64
		want          bool
	}{
		{"PREPARING", 2, 1, 2, true},
		{"PREPARING", 1, 1, 2, false},
		{"CUTOVER", 2, 1, 2, true},
		{"CUTOVER", 1, 1, 2, false},
		{"ROLLING_BACK", 1, 1, 2, true},
		{"ROLLING_BACK", 2, 1, 2, false},
	}
	for _, tc := range cases {
		if got := rolloutOwnsReplacement(tc.status, tc.dep, tc.from, tc.to); got != tc.want {
			t.Errorf("rolloutOwnsReplacement(%s,%d)=%v, want %v", tc.status, tc.dep, got, tc.want)
		}
	}
}

// P1#10：失联/挂起节点不再串行饿死整轮 observed sync——有界并发抓取使
// N 个挂起节点的整轮开销 ≈ 单节点超时（旧串行为 ~timeout×N），且不阻断
// 健康节点观测的合并（per-node failure isolation）。
func TestFetchNodeObservationsIsolatesHangingNodes(t *testing.T) {
	views := []nodeView{
		{agentID: "hang-1", nomadID: "hang-1"},
		{agentID: "ok-1", nomadID: "ok-1"},
		{agentID: "hang-2", nomadID: "hang-2"},
		{agentID: "ok-2", nomadID: "ok-2"},
		{agentID: "no-client", nomadID: "no-client"},
	}
	hanging := map[string]bool{"hang-1": true, "hang-2": true}
	const timeout = 300 * time.Millisecond
	start := time.Now()
	outcomes := fetchNodeObservations(context.Background(), views,
		func(v nodeView) *agentclient.Client {
			if v.agentID == "no-client" {
				return nil
			}
			return &agentclient.Client{}
		},
		func(ctx context.Context, v nodeView, _ *agentclient.Client) ([]*pb.Machine, error) {
			if hanging[v.agentID] {
				<-ctx.Done() // 模拟挂起节点：直到自己的 10s 超时
				return nil, ctx.Err()
			}
			return []*pb.Machine{{MachineId: "m-" + v.agentID}}, nil
		},
		timeout, nodeObservedListParallel)
	elapsed := time.Since(start)

	// 串行实现 ≥ 2×timeout（两个挂起节点依次各耗一个超时）；有界并发 ≈ timeout。
	if elapsed >= 2*timeout {
		t.Fatalf("concurrent fetch took %v; serial behaviour would be >= %v", elapsed, 2*timeout)
	}
	if len(outcomes) != len(views) {
		t.Fatalf("outcomes = %d, want %d", len(outcomes), len(views))
	}
	// 结果按 view 原序合并；健康节点的观测保留，挂起节点各自隔离为失败。
	for i, want := range []struct {
		id        string
		machines  int
		err       bool
		noClientH bool
	}{
		{"hang-1", 0, true, false},
		{"ok-1", 1, false, false},
		{"hang-2", 0, true, false},
		{"ok-2", 1, false, false},
		{"no-client", 0, false, true},
	} {
		o := outcomes[i]
		if o.view.agentID != want.id {
			t.Fatalf("outcome %d view = %s, want %s (order must be preserved)", i, o.view.agentID, want.id)
		}
		if len(o.machines) != want.machines || (o.err != nil) != want.err || o.noClient != want.noClientH {
			t.Fatalf("outcome %d (%s) = machines %d, err %v, noClient %v",
				i, want.id, len(o.machines), o.err, o.noClient)
		}
		if want.machines == 1 && o.machines[0].MachineId != "m-"+want.id {
			t.Fatalf("outcome %d machine = %s", i, o.machines[0].MachineId)
		}
	}
}

// P1#10：并发上限必须生效（信号量约束），节点数超过上限时按批推进。
func TestFetchNodeObservationsBoundsParallelism(t *testing.T) {
	const nodeCount, maxParallel = 12, 4
	views := make([]nodeView, nodeCount)
	for i := range views {
		views[i] = nodeView{agentID: fmt.Sprintf("n-%02d", i), nomadID: fmt.Sprintf("n-%02d", i)}
	}
	var mu sync.Mutex
	active, maxActive := 0, 0
	outcomes := fetchNodeObservations(context.Background(), views,
		func(nodeView) *agentclient.Client { return &agentclient.Client{} },
		func(ctx context.Context, v nodeView, _ *agentclient.Client) ([]*pb.Machine, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
			return nil, nil
		},
		timeoutForTest, maxParallel)
	mu.Lock()
	defer mu.Unlock()
	if maxActive > maxParallel {
		t.Fatalf("max concurrent lists = %d, want <= %d", maxActive, maxParallel)
	}
	if len(outcomes) != nodeCount {
		t.Fatalf("outcomes = %d, want %d", len(outcomes), nodeCount)
	}
}

const timeoutForTest = time.Second
