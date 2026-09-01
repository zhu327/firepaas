package controller

import (
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/controlplane/store"
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
