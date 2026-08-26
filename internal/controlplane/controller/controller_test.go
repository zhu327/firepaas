package controller

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
