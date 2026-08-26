package controller

import (
	"testing"

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
