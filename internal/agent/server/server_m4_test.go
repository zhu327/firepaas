// M4（ADR-0006/0010）服务端不变量测试。
package server

import (
	"context"
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/security/redact"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// M4.5：Pause/Resume 的服务端纪律——幂等重放、fencing、execution 绑定。
func TestPauseResumeMachine(t *testing.T) {
	srv, _, _ := newTestServer(t)
	ctx := context.Background()

	// 前置：创建一台机器（generation 1，execution exec-p1）。
	if _, err := srv.CreateMachine(ctx, createReq("m-pause", 1, "op-pause-create")); err != nil {
		t.Fatal(err)
	}

	pause := func(gen uint64, exec, opID string) (*pb.Machine, error) {
		return srv.PauseMachine(ctx, &pb.PauseMachineRequest{Operation: &pb.MachineOperationRequest{
			MachineId: "m-pause", ExecutionId: exec, Generation: gen, OperationId: opID,
		}})
	}
	resume := func(gen uint64, exec, opID string) (*pb.Machine, error) {
		return srv.ResumeMachine(ctx, &pb.ResumeMachineRequest{Operation: &pb.MachineOperationRequest{
			MachineId: "m-pause", ExecutionId: exec, Generation: gen, OperationId: opID,
		}})
	}

	// 参数校验：缺 execution → InvalidArgument。
	if _, err := pause(1, "", "op-bad"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing execution: want InvalidArgument, got %v", err)
	}
	// execution 不匹配（机器是 exec-op-pause-create）→ Internal（adapter 拒绝）。
	if _, err := pause(1, "exec-wrong", "op-wrong"); err == nil {
		t.Fatal("stale execution pause must fail")
	}
	// 正常 pause：状态 → PAUSED。
	m, err := pause(1, "exec-op-pause-create", "op-pause-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.GetState() != pb.MachineState_PAUSED {
		t.Fatalf("pause state = %v, want PAUSED", m.GetState())
	}
	// 幂等：同 opID 重放返回原结果。
	m2, err := pause(1, "exec-op-pause-create", "op-pause-1")
	if err != nil || m2.GetState() != pb.MachineState_PAUSED {
		t.Fatalf("pause replay: %v %v", m2, err)
	}
	// fencing：旧 generation pause 被拒。
	if _, err := pause(0, "exec-op-pause-create", "op-pause-stale"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale generation pause: want FailedPrecondition, got %v", err)
	}
	// resume：状态 → RUNNING。
	m, err = resume(1, "exec-op-pause-create", "op-resume-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.GetState() != pb.MachineState_RUNNING {
		t.Fatalf("resume state = %v, want RUNNING", m.GetState())
	}
	// resume 幂等重放。
	if _, err := resume(1, "exec-op-pause-create", "op-resume-1"); err != nil {
		t.Fatalf("resume replay: %v", err)
	}
}

// M4：create 缺少 execution-bound proxy credential 必须被拒绝。
func TestCreateRequiresProxyCredential(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := createReq("m-cred-1", 1, "op-cred-1")
	req.ProxyCredential = "" // 显式去掉默认值
	_, err := srv.CreateMachine(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

// M4：secret_env / proxy_credential 不参与 request hash —— 控制面重派时
// 重新解析引用值、现算凭证，同一 operation_id 的幂等重放不能被破坏。
func TestSnapshotRequestHashIgnoresProxyCredential(t *testing.T) {
	for _, pair := range [][2]proto.Message{
		{&pb.ForkSnapshotRequest{MachineId: "m", ExecutionId: "e", Generation: 1, OperationId: "o", SnapshotId: "s", ProxyCredential: "old"},
			&pb.ForkSnapshotRequest{MachineId: "m", ExecutionId: "e", Generation: 1, OperationId: "o", SnapshotId: "s", ProxyCredential: "new"}},
		{&pb.RestoreSnapshotRequest{MachineId: "m", ExecutionId: "e", Generation: 1, OperationId: "o", SnapshotId: "s", ProxyCredential: "old"},
			&pb.RestoreSnapshotRequest{MachineId: "m", ExecutionId: "e", Generation: 1, OperationId: "o", SnapshotId: "s", ProxyCredential: "new"}},
	} {
		if hashRequest(pair[0]) != hashRequest(pair[1]) {
			t.Fatal("one-way snapshot credential changed idempotency hash")
		}
	}
}

func TestRequestHashIgnoresOneWayFields(t *testing.T) {
	base := createReq("m-hash-1", 1, "op-hash-1")
	a := hashRequest(base)
	mod := &pb.CreateMachineRequest{
		MachineId: base.MachineId, Generation: base.Generation,
		OperationId: base.OperationId, Spec: base.Spec,
		SecretEnv:       map[string]string{"DB_PASS": "hunter2"},
		ProxyCredential: "tok-a",
	}
	b := hashRequest(mod)
	if a != b {
		t.Fatalf("hash must ignore one-way fields: %s vs %s", a, b)
	}
	// 但真正影响语义的字段变化仍然改变 hash。
	c := hashRequest(createReq("m-hash-1", 2, "op-hash-1"))
	if c == a {
		t.Fatal("generation change must change hash")
	}
}

// M4：字段黑名单覆盖单向下发字段与密文字段。
func TestRedactBlacklist(t *testing.T) {
	for _, k := range []string{"secret_env", "SecretEnv", "proxy_credential",
		"traffic_token", "value_ciphertext", "dek_wrapped", "authorization"} {
		if !redact.IsSensitive(k) {
			t.Fatalf("%q must be sensitive", k)
		}
	}
	m := redact.RedactMap(map[string]any{
		"secret_env": map[string]string{"k": "v"},
		"machine_id": "m",
	})
	v, _ := m["secret_env"].(string)
	if !strings.Contains(v, "REDACTED") {
		t.Fatalf("secret_env not redacted: %v", m["secret_env"])
	}
	if m["machine_id"] != "m" {
		t.Fatalf("normal field must pass through")
	}
}
