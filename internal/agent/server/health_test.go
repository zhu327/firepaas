// health_test.go：review 2026-09-10 的 readiness fail-closed 回归。
//
// 声明了探针但无法编码（EXEC 未实现 / target 非法）时，旧实现把 healthTag
// 置为 "0"（= UNCONFIGURED），controller 视为 RUNNING 即 READY——未执行任何
// 探针就把 backend 发布进路由，ADR-0008 的唯一就绪来源失效。新实现必须在
// create 时 InvalidArgument 拒绝。
package server

import (
	"context"
	"testing"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateMachineRejectsUnsupportedHealthCheck(t *testing.T) {
	cases := []struct {
		name string
		hc   *pb.HealthCheckSpec
	}{
		{
			name: "exec probe not implemented",
			hc:   &pb.HealthCheckSpec{Type: pb.HealthCheckSpec_EXEC, Target: "cat /tmp/ready"},
		},
		{
			name: "http target without scheme",
			hc:   &pb.HealthCheckSpec{Type: pb.HealthCheckSpec_HTTP, Target: "127.0.0.1:8080/healthz"},
		},
		{
			name: "tcp target wrong scheme",
			hc:   &pb.HealthCheckSpec{Type: pb.HealthCheckSpec_TCP, Target: "http://127.0.0.1:80"},
		},
		{
			name: "unknown probe type",
			hc:   &pb.HealthCheckSpec{Type: pb.HealthCheckSpec_Type(99), Target: "x"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _ := newTestServer(t)
			req := createReq("m-health-reject", 1, "op-health-reject")
			req.Spec.HealthCheck = tc.hc
			_, err := srv.CreateMachine(context.Background(), req)
			if code := status.Code(err); code != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s (err=%v)", code, codes.InvalidArgument, err)
			}
		})
	}
}

// 合法探针与“空消息（TYPE_UNSPECIFIED）”仍按旧语义处理：前者编码进 tag，
// 后者等同未声明（UNCONFIGURED）。
func TestCreateMachineAcceptsValidOrEmptyHealthCheck(t *testing.T) {
	cases := []struct {
		name string
		hc   *pb.HealthCheckSpec
	}{
		{name: "nil probe"},
		{
			name: "http probe",
			hc: &pb.HealthCheckSpec{
				Type: pb.HealthCheckSpec_HTTP, Target: "http://127.0.0.1:8080/healthz",
				IntervalSeconds: 2, TimeoutSeconds: 1, UnhealthyThreshold: 3,
			},
		},
		{name: "empty message = unspecified", hc: &pb.HealthCheckSpec{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _ := newTestServer(t)
			req := createReq("m-health-accept", 1, "op-health-accept")
			req.Spec.HealthCheck = tc.hc
			resp, err := srv.CreateMachine(context.Background(), req)
			if err != nil {
				t.Fatalf("create with %s: %v", tc.name, err)
			}
			if resp.GetMachine().GetMachineId() != "m-health-accept" {
				t.Fatalf("unexpected machine: %+v", resp.GetMachine())
			}
		})
	}
}
