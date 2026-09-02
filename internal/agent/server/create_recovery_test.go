package server

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/instances"
	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// createRecoveryHarness 组装带共享 fake 实例仓的 server（崩溃窗口测试用：
// ledger/creds 都可从同一目录重开，模拟 agent 重启）。
type createRecoveryHarness struct {
	dir        string
	fake       *fakeInstances
	ledgerPath string
	fencesPath string
	credsPath  string
}

func newCreateRecoveryHarness(t *testing.T) *createRecoveryHarness {
	t.Helper()
	dir := t.TempDir()
	return &createRecoveryHarness{
		dir:        dir,
		fake:       &fakeInstances{byName: map[string]*instances.Instance{}},
		ledgerPath: filepath.Join(dir, "ledger.json"),
		fencesPath: filepath.Join(dir, "fences.json"),
		credsPath:  filepath.Join(dir, "credentials.json"),
	}
}

// open 按当前目录内容重建 server（等价 agent 重启后重装配）。
func (h *createRecoveryHarness) open(t *testing.T) (*Server, *state.Creds) {
	t.Helper()
	ledger, err := state.Open(h.ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	fences, err := state.OpenFences(h.fencesPath)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := state.OpenCreds(h.credsPath)
	if err != nil {
		t.Fatal(err)
	}
	adapter := machine.New(h.fake, fakeImages{}, nil, nil)
	provider := info.New("test-node", "test", "test", "compute", "v1", "10.100.0.0/16", h.dir, nil, nil)
	return New(adapter, ledger, fences, provider,
		WithCreds(creds), WithCredentialRequired(true)), creds
}

// createCalls 返回 fake 累计 CreateInstance 调用数。
func (h *createRecoveryHarness) createCalls() int {
	h.fake.mu.Lock()
	defer h.fake.mu.Unlock()
	return h.fake.createCalls
}

func createInstanceTags(req *pb.CreateMachineRequest, generation uint64) map[string]string {
	return map[string]string{
		"firepaas/machine_id":   req.GetMachineId(),
		"firepaas/project_id":   req.GetSpec().GetProjectId(),
		"firepaas/app_id":       req.GetSpec().GetAppId(),
		"firepaas/execution_id": req.GetSpec().GetExecutionId(),
		"firepaas/generation":   strconv.FormatUint(generation, 10),
		"firepaas/port":         "8080",
	}
}

// TestCreateMachineCrashWindowAdoptsExistingInstance 模拟“Effect 已创建实例
// 但 Complete 未落盘”的崩溃：重启后同 operation_id 重试必须经 inventory
// 认领实例（不重跑 CreateInstance），返回真实 machine 状态，并补全
// credential 与 ledger 完成。
func TestCreateMachineCrashWindowAdoptsExistingInstance(t *testing.T) {
	h := newCreateRecoveryHarness(t)
	req := createReq("m-crash", 1, "op-crash-1")
	// 预置：hypeman inventory 里的实例（上次 Effect 产物）。
	h.fake.byName["m-crash"] = &instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id:        "internal-crash",
			Name:      "m-crash",
			Image:     req.GetSpec().GetImageRef(),
			Tags:      createInstanceTags(req, 1),
			IP:        "10.100.0.9",
			CreatedAt: time.Now(),
		},
		State: instances.StateRunning,
	}
	// 预置：未完成的 durable claim（含 request_hash）。
	srv, _ := h.open(t)
	if _, _, err := srv.ledger.Begin(state.Record{
		OperationID: req.GetOperationId(),
		MachineID:   req.GetMachineId(),
		ExecutionID: req.GetSpec().GetExecutionId(),
		Generation:  req.GetGeneration(),
		Kind:        "machine.create",
		RequestHash: hashRequest(req),
	}); err != nil {
		t.Fatal(err)
	}

	out, err := srv.CreateMachine(context.Background(), req)
	if err != nil {
		t.Fatalf("crash-window retry must succeed by adoption: %v", err)
	}
	if got := h.createCalls(); got != 0 {
		t.Fatalf("adoption must not re-run CreateInstance, got %d calls", got)
	}
	if out.GetMachine().GetSlotIp() != "10.100.0.9" {
		t.Fatalf("adopted machine must reflect actual instance state: %+v", out.GetMachine())
	}
	// ledger 完成 + credential 已补写（:5107 可用）。
	if _, complete, _ := srv.ledger.Check(req.GetOperationId(), hashRequest(req)); !complete {
		t.Fatal("adopted create not completed in ledger")
	}
	freshCreds, err := state.OpenCreds(h.credsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !freshCreds.Verify("m-crash", req.GetSpec().GetExecutionId(), "test-execution-credential") {
		t.Fatal("adoption must persist the proxy credential")
	}
	// fence 高水位已推进：更旧的 generation 被拒。
	if err := srv.fences.Check("m-crash", 0); err == nil {
		t.Fatal("adoption must advance the generation fence")
	}
}

// TestCreateMachineCrashWindowRerunsEffectWhenInstanceMissing：未完成 claim
// + inventory 无实例（Effect 从未生效）→ 重试在同一 claim 下干净重跑
// Effect，返回成功（不再出现“撞同名 → 永久 FAILED”）。
func TestCreateMachineCrashWindowRerunsEffectWhenInstanceMissing(t *testing.T) {
	h := newCreateRecoveryHarness(t)
	req := createReq("m-rerun", 1, "op-rerun-1")
	srv, _ := h.open(t)
	if _, _, err := srv.ledger.Begin(state.Record{
		OperationID: req.GetOperationId(),
		MachineID:   req.GetMachineId(),
		ExecutionID: req.GetSpec().GetExecutionId(),
		Generation:  req.GetGeneration(),
		Kind:        "machine.create",
		RequestHash: hashRequest(req),
	}); err != nil {
		t.Fatal(err)
	}

	out, err := srv.CreateMachine(context.Background(), req)
	if err != nil {
		t.Fatalf("retry with orphaned claim must re-run the effect: %v", err)
	}
	if got := h.createCalls(); got != 1 {
		t.Fatalf("effect must run exactly once on retry, got %d", got)
	}
	if out.GetMachine().GetMachineId() != "m-rerun" {
		t.Fatalf("re-run result: %+v", out.GetMachine())
	}
	if _, complete, _ := srv.ledger.Check(req.GetOperationId(), hashRequest(req)); !complete {
		t.Fatal("re-run create not completed in ledger")
	}
}

// TestCreateMachineReplayRestoresCredential：creds.json 丢失 + agent 重启后，
// 重放已完成的 create 必须补写验证材料，恢复 :5107 可用（否则该 machine 的
// 流量永久 403）。
func TestCreateMachineReplayRestoresCredential(t *testing.T) {
	h := newCreateRecoveryHarness(t)
	req := createReq("m-replay", 1, "op-replay-1")
	srv, _ := h.open(t)
	if _, err := srv.CreateMachine(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := h.createCalls(); got != 1 {
		t.Fatalf("first create calls = %d", got)
	}
	// 模拟 creds.json 损坏/丢失 + agent 重启（ledger/实例仓不变）。
	if err := os.Remove(h.credsPath); err != nil {
		t.Fatal(err)
	}
	srv2, creds2 := h.open(t)
	if creds2.Verify("m-replay", req.GetSpec().GetExecutionId(), "test-execution-credential") {
		t.Fatal("credential should be gone before replay")
	}
	out, err := srv2.CreateMachine(context.Background(), req)
	if err != nil {
		t.Fatalf("completed create replay must succeed: %v", err)
	}
	if got := h.createCalls(); got != 1 {
		t.Fatalf("replay must not re-run CreateInstance, got %d calls", got)
	}
	if out.GetMachine().GetMachineId() != "m-replay" {
		t.Fatalf("replay result: %+v", out.GetMachine())
	}
	freshCreds, err := state.OpenCreds(h.credsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !freshCreds.Verify("m-replay", req.GetSpec().GetExecutionId(), "test-execution-credential") {
		t.Fatal("replay must re-persist the credential so :5107 serves the machine")
	}
}
