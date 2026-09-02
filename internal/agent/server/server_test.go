package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"
	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- 测试替身（实现 machine.InstanceManager / machine.ImageManager 子集）----

type fakeInstances struct {
	mu           sync.Mutex
	byName       map[string]*instances.Instance
	nextID       int
	createCalls  int
	standbyCalls int
	restoreCalls int
}

func (f *fakeInstances) CreateInstance(
	_ context.Context,
	req instances.CreateInstanceRequest,
) (*instances.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.nextID++
	inst := &instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id:          fmt.Sprintf("internal-%d", f.nextID),
			Name:        req.Name,
			Image:       req.Image,
			Size:        req.Size,
			OverlaySize: req.OverlaySize,
			Vcpus:       req.Vcpus,
			Env:         req.Env,
			Tags:        req.Tags,
			IP:          "10.100.0.2",
			CreatedAt:   time.Now(),
		},
		State: instances.StateInitializing,
	}
	f.byName[req.Name] = inst
	return inst, nil
}

func (f *fakeInstances) ListInstances(
	_ context.Context,
	_ *instances.ListInstancesFilter,
) ([]instances.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]instances.Instance, 0, len(f.byName))
	for _, inst := range f.byName {
		out = append(out, *inst)
	}
	return out, nil
}

func (f *fakeInstances) GetInstance(_ context.Context, idOrName string) (*instances.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, inst := range f.byName {
		if inst.Id == idOrName || inst.Name == idOrName {
			cp := *inst
			return &cp, nil
		}
	}
	return nil, instances.ErrNotFound
}

// M4.5：standby/restore 替身（记录调用并按状态机迁移）。
func (f *fakeInstances) StandbyInstance(
	_ context.Context,
	id string,
	_ instances.StandbyInstanceRequest,
) (*instances.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.standbyCalls++
	inst := f.byID(id)
	if inst == nil {
		return nil, instances.ErrNotFound
	}
	inst.State = instances.StateStandby
	return inst, nil
}

func (f *fakeInstances) RestoreInstance(_ context.Context, id string) (*instances.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreCalls++
	inst := f.byID(id)
	if inst == nil {
		return nil, instances.ErrNotFound
	}
	inst.State = instances.StateRunning
	return inst, nil
}

func (f *fakeInstances) byID(id string) *instances.Instance {
	if inst, ok := f.byName[id]; ok {
		return inst // test ids double as internal ids
	}
	for _, inst := range f.byName {
		if inst.Id == id {
			return inst
		}
	}
	return nil
}

func (f *fakeInstances) DeleteInstance(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, inst := range f.byName {
		if inst.Id == id || inst.Name == id {
			delete(f.byName, name)
			return nil
		}
	}
	return instances.ErrNotFound
}

type fakeImages struct {
	deleteErr error
}

func (fakeImages) CreateImage(context.Context, images.CreateImageRequest) (*images.Image, error) {
	return nil, nil
}
func (fakeImages) WaitForReady(context.Context, string) error { return nil }
func (fakeImages) GetImage(context.Context, string) (*images.Image, error) {
	return &images.Image{Name: "x", SizeBytes: ptr(int64(1 << 20))}, nil
}

func (fakeImages) ListImages(context.Context) ([]images.Image, error) {
	return []images.Image{{
		Name:      "docker.io/library/nginx:alpine@sha256:" + strings.Repeat("a", 64),
		Status:    images.StatusReady,
		SizeBytes: ptr(int64(1 << 20)),
	}}, nil
}
func (f fakeImages) DeleteImage(context.Context, string) error { return f.deleteErr }

func ptr[T any](v T) *T { return &v }

func newTestServer(t *testing.T) (*Server, *state.Ledger, *state.Fences) {
	t.Helper()
	dir := t.TempDir()
	ledger, err := state.Open(filepath.Join(dir, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	fences, err := state.OpenFences(filepath.Join(dir, "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := machine.New(&fakeInstances{byName: map[string]*instances.Instance{}}, fakeImages{}, nil, nil)
	provider := info.New("test-node", "test", "test", "compute", "v1.14.2", "10.100.0.0/16", dir, nil, nil)
	creds, err := state.OpenCreds(filepath.Join(dir, "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	return New(adapter, ledger, fences, provider,
		WithCreds(creds), WithCredentialRequired(true)), ledger, fences
}

func TestDeleteImageNotFoundIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	ledger, _ := state.Open(filepath.Join(dir, "ledger.json"))
	fences, _ := state.OpenFences(filepath.Join(dir, "fences.json"))
	adapter := machine.New(&fakeInstances{byName: map[string]*instances.Instance{}}, fakeImages{}, nil, nil)
	srv := New(adapter, ledger, fences, info.New("node", "test", "test", "compute", "v1", "", dir, nil, nil))
	if _, err := srv.DeleteImage(context.Background(), &pb.DeleteImageRequest{ImageRef: "repo/missing@sha256:" + strings.Repeat("b", 64)}); err != nil {
		t.Fatalf("DeleteImage missing = %v, want success", err)
	}
}

func TestDeleteImageRejectsLiveReference(t *testing.T) {
	dir := t.TempDir()
	ledger, _ := state.Open(filepath.Join(dir, "ledger.json"))
	fences, _ := state.OpenFences(filepath.Join(dir, "fences.json"))
	ref := "docker.io/library/nginx:alpine@sha256:" + strings.Repeat("a", 64)
	inst := &fakeInstances{
		byName: map[string]*instances.Instance{
			"m1": {StoredMetadata: instances.StoredMetadata{Name: "m1", Image: ref}},
		},
	}
	srv := New(machine.New(inst, fakeImages{}, nil, nil), ledger, fences,
		info.New("node", "test", "test", "compute", "v1", "", dir, nil, nil))
	_, err := srv.DeleteImage(context.Background(), &pb.DeleteImageRequest{ImageRef: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteImage live status = %v, want FailedPrecondition", status.Code(err))
	}
}

func createReq(machineID string, generation uint64, opID string) *pb.CreateMachineRequest {
	return &pb.CreateMachineRequest{
		MachineId:   machineID,
		Generation:  generation,
		OperationId: opID,
		Spec: &pb.MachineSpec{
			ProjectId:    "p",
			AppId:        "a",
			DeploymentId: "d",
			ExecutionId:  "exec-" + opID,
			ImageRef:     "docker.io/library/nginx:alpine",
			Vcpu:         1,
			MemMib:       256,
			Env:          map[string]string{"PORT": "8080"},
		},
		ProxyCredential: "test-execution-credential",
	}
}

func deleteReq(machineID string, generation uint64, opID, executionID string) *pb.DeleteMachineRequest {
	return &pb.DeleteMachineRequest{
		MachineId:   machineID,
		ExecutionId: executionID,
		Generation:  generation,
		OperationId: opID,
	}
}

// P0-1：secret_env 值不得出现在响应、ListMachines 与持久化 ledger 结果中
// （ADR-0013 不变量 3）。
func TestCreateMachineDoesNotLeakSecretEnv(t *testing.T) {
	srv, ledger, _ := newTestServer(t)
	req := createReq("m-secret", 1, "op-secret-1")
	req.SecretEnv = map[string]string{"API_KEY": "super-secret-value", "DB_PASSWORD": "hunter2"}

	_, err := srv.CreateMachine(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("secret_env without safe injection API: want InvalidArgument, got %v", err)
	}
	// fail-closed 请求不写入幂等账本，且不能把值持久化在结果中。
	_, ok, err := ledger.Check("op-secret-1", hashRequest(req))
	if err != nil || ok {
		t.Fatalf("rejected secret request must not enter ledger: ok=%v err=%v", ok, err)
	}
}

// P0-2：generation fencing——旧 generation 的变更被拒绝；
// 删除后旧 generation 的 re-create 也被拒绝；幂等重放不受影响。
func TestDeleteRejectsStaleExecution(t *testing.T) {
	srv, _, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := srv.CreateMachine(ctx, createReq("m-exec", 1, "op-create")); err != nil {
		t.Fatal(err)
	}
	// 确定性 mismatch（R2 加固）：FailedPrecondition 让控制面一次判决终态
	// （取代早期无信息量的 Internal + 无限重试活锁）。
	if _, err := srv.DeleteMachine(ctx, deleteReq("m-exec", 1, "op-delete", "wrong-execution")); status.Code(
		err,
	) != codes.FailedPrecondition {
		t.Fatalf("stale execution delete: want FailedPrecondition, got %v", err)
	}
	list, err := srv.ListMachines(ctx, &pb.ListMachinesRequest{})
	if err != nil || len(list.Machines) != 1 {
		t.Fatalf("stale delete removed machine: list=%+v err=%v", list, err)
	}
}

func TestGenerationFencing(t *testing.T) {
	srv, _, _ := newTestServer(t)
	ctx := context.Background()

	// gen=2 create 成功，高水位推进到 2。
	if _, err := srv.CreateMachine(ctx, createReq("m-fence", 2, "op-a")); err != nil {
		t.Fatalf("create gen=2: %v", err)
	}
	// 旧 generation delete 被拒绝。
	if _, err := srv.DeleteMachine(ctx, deleteReq("m-fence", 1, "op-b", "exec-op-a")); status.Code(
		err,
	) != codes.FailedPrecondition {
		t.Fatalf("stale delete: want FailedPrecondition, got %v", err)
	}
	// machine 存活期间，已记录操作的重放不受 fence 影响（幂等性优先）。
	if _, err := srv.CreateMachine(ctx, createReq("m-fence", 2, "op-a")); err != nil {
		t.Fatalf("replay of recorded op must not hit fence while machine exists: %v", err)
	}
	// 同代 delete 成功。
	if _, err := srv.DeleteMachine(ctx, deleteReq("m-fence", 2, "op-c", "exec-op-a")); err != nil {
		t.Fatalf("same-gen delete: %v", err)
	}
	// 删除后旧 generation re-create 被拒绝（高水位保留）。
	if _, err := srv.CreateMachine(ctx, createReq("m-fence", 1, "op-d")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale recreate after delete: want FailedPrecondition, got %v", err)
	}
	// 更高 generation re-create 成功。
	if _, err := srv.CreateMachine(ctx, createReq("m-fence", 3, "op-e")); err != nil {
		t.Fatalf("higher-gen recreate: %v", err)
	}
	// 旧 create 的重放在 machine 删除换代后被拒绝：delete 触发的 ledger prune
	// （P2-5）移除了旧 create 记录，fence 指共其不返回已删 VM 的陈旧“成功”。
	if _, err := srv.CreateMachine(ctx, createReq("m-fence", 2, "op-a")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replay of create from deleted generation: want FailedPrecondition, got %v", err)
	}
}

// P0-2：fence 拒绝属于永久错误——不得推进高水位、不得留下 ledger 记录，
// 这样控制器重试会得到同样的 FailedPrecondition（op 落 FAILED）。
func TestStaleCreateHasNoSideEffects(t *testing.T) {
	srv, ledger, fences := newTestServer(t)
	ctx := context.Background()

	if _, err := srv.CreateMachine(ctx, createReq("m-se", 2, "op-se-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.CreateMachine(ctx, createReq("m-se", 1, "op-se-stale")); status.Code(
		err,
	) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
	// 被拒的 stale 操作不留 ledger 记录（重试继续被 fence 拒绝）。
	if _, ok, _ := ledger.Check("op-se-stale", hashRequest(createReq("m-se", 1, "op-se-stale"))); ok {
		t.Fatal("rejected stale create must not be recorded in ledger")
	}
	// 高水位不被降低。
	if err := fences.Check("m-se", 1); err == nil {
		t.Fatal("fence high-water mark must remain")
	}
}
