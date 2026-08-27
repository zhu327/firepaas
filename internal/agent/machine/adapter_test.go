package machine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"

	pb "github.com/example/firepaas/shared/gen/agent/v1"
)

type fakeInstances struct {
	created *instances.Instance
	listed  []instances.Instance
	deleted string
	err     error
}

func (f *fakeInstances) CreateInstance(_ context.Context, req instances.CreateInstanceRequest) (*instances.Instance, error) {
	if f.err != nil {
		return nil, f.err
	}
	inst := &instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id:          "internal-1",
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
	f.created = inst
	return inst, nil
}

func (f *fakeInstances) ListInstances(_ context.Context, _ *instances.ListInstancesFilter) ([]instances.Instance, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.listed != nil {
		return f.listed, nil
	}
	if f.created != nil {
		return []instances.Instance{*f.created}, nil
	}
	return nil, nil
}

func (f *fakeInstances) GetInstance(_ context.Context, idOrName string) (*instances.Instance, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.created != nil && (f.created.Id == idOrName || f.created.Name == idOrName) {
		return f.created, nil
	}
	return nil, instances.ErrNotFound
}

func (f *fakeInstances) DeleteInstance(_ context.Context, id string) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = id
	return nil
}

// M4.5：standby/restore 替身（按状态机迁移，供 autoresume 断言）。
// 语义对齐 hypeman：Standby/Restore 只接受**内部 ID**（loadMetadata 按目录
// 名加载，传 name 会 ErrNotFound）——曾经的替身两者都收，把
// "autoresume 传了 name"的真机 bug 藏到了单测盲区。
func (f *fakeInstances) StandbyInstance(_ context.Context, id string, _ instances.StandbyInstanceRequest) (*instances.Instance, error) {
	if f.created == nil || f.created.Id != id {
		return nil, instances.ErrNotFound
	}
	f.created.State = instances.StateStandby
	return f.created, nil
}

func (f *fakeInstances) RestoreInstance(_ context.Context, id string) (*instances.Instance, error) {
	if f.created == nil || f.created.Id != id {
		return nil, instances.ErrNotFound
	}
	f.created.State = instances.StateRunning
	return f.created, nil
}

type fakeImages struct{ err error }

func (f *fakeImages) CreateImage(context.Context, images.CreateImageRequest) (*images.Image, error) {
	return nil, f.err
}
func (f *fakeImages) WaitForReady(context.Context, string) error { return f.err }

func validCreateRequest() *pb.CreateMachineRequest {
	return &pb.CreateMachineRequest{
		MachineId:   "m1-test",
		Generation:  1,
		OperationId: "op-1",
		SecretEnv:   map[string]string{"SECRET_TOKEN": "s3cr3t"},
		Spec: &pb.MachineSpec{
			ProjectId:    "p1",
			AppId:        "a1",
			DeploymentId: "d1",
			ExecutionId:  "e1",
			ImageRef:     "docker.io/library/nginx:alpine",
			Vcpu:         1,
			MemMib:       512,
			Env:          map[string]string{"PORT": "8080"},
		},
	}
}

func TestAdapterCreateMapping(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	m, err := a.Create(context.Background(), validCreateRequest())
	if err != nil {
		t.Fatal(err)
	}
	if m.MachineId != "m1-test" {
		t.Fatalf("machine_id = %s, want m1-test", m.MachineId)
	}
	if m.Spec.ExecutionId != "e1" || m.Readiness != pb.MachineReadiness_UNCONFIGURED {
		t.Fatalf("unexpected machine: %+v", m)
	}
	// secret 值必须进入 hypeman 请求（VM 启动配置），但不得出现在回显 Machine。
	if got := im.created.Env["SECRET_TOKEN"]; got != "s3cr3t" {
		t.Fatalf("secret env not merged into hypeman request: %q", got)
	}
	if _, ok := m.Spec.Env["SECRET_TOKEN"]; ok {
		t.Fatal("secret env leaked into response Machine.Spec.Env (ADR-0013 invariant 3)")
	}
	if got := m.Spec.Env["PORT"]; got != "8080" {
		t.Fatalf("non-secret env lost in echo: PORT=%q", got)
	}
	if im.created.Tags[tagSecretKeys] != "SECRET_TOKEN" {
		t.Fatalf("secret keys tag = %q, want SECRET_TOKEN", im.created.Tags[tagSecretKeys])
	}
	if im.created.Name != "m1-test" || im.created.Vcpus != 1 || im.created.Size != 512*1024*1024 {
		t.Fatalf("unexpected hypeman request mapping: %+v", im.created)
	}
}

func TestAdapterListDoesNotEchoSecrets(t *testing.T) {
	im := &fakeInstances{listed: []instances.Instance{
		{StoredMetadata: instances.StoredMetadata{
			Id: "i1", Name: "m1", Image: "img",
			Env:  map[string]string{"PORT": "8080", "API_KEY": "s3cr3t"},
			Tags: map[string]string{tagProject: "p1", tagExecution: "e1", tagSecretKeys: "API_KEY"},
		}, State: instances.StateRunning},
	}}
	a := New(im, &fakeImages{}, nil, nil)
	got, err := a.List(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 machine, got %d", len(got))
	}
	if _, ok := got[0].Spec.Env["API_KEY"]; ok {
		t.Fatal("ListMachines echoed secret env (ADR-0013 invariant 3)")
	}
	if got[0].Spec.Env["PORT"] != "8080" {
		t.Fatal("non-secret env lost in list echo")
	}
}

func TestRedactSecretEnv(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		tag  string
		want map[string]string
	}{
		{"no secret tag", map[string]string{"A": "1"}, "", map[string]string{"A": "1"}},
		{"single secret", map[string]string{"A": "1", "S": "x"}, "S", map[string]string{"A": "1"}},
		{"multiple secrets", map[string]string{"A": "1", "S1": "x", "S2": "y"}, "S1,S2", map[string]string{"A": "1"}},
		{"all secrets", map[string]string{"S": "x"}, "S", map[string]string{}},
		{"empty env", map[string]string{}, "S", map[string]string{}},
		{"nil env", nil, "S", nil},
		{"unknown key in tag", map[string]string{"A": "1"}, "NOPE", map[string]string{"A": "1"}},
	}
	for _, tc := range cases {
		got := redactSecretEnv(tc.env, tc.tag)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for k, v := range tc.want {
			if got[k] != v {
				t.Errorf("%s: key %s = %q, want %q", tc.name, k, got[k], v)
			}
		}
		// 原 env 不得被修改（hypeman 侧数据不动）。
		if tc.env != nil {
			if _, ok := tc.env["S"]; ok && len(tc.env) == 0 {
				t.Errorf("%s: input env mutated", tc.name)
			}
		}
	}
}

func TestAdapterListProjectFilter(t *testing.T) {
	im := &fakeInstances{listed: []instances.Instance{
		{StoredMetadata: instances.StoredMetadata{Id: "i1", Name: "m1", Image: "img", Tags: map[string]string{tagProject: "p1", tagExecution: "e1"}}, State: instances.StateRunning},
		{StoredMetadata: instances.StoredMetadata{Id: "i2", Name: "m2", Image: "img", Tags: map[string]string{tagProject: "p2", tagExecution: "e2"}}, State: instances.StateStopped},
	}}
	a := New(im, &fakeImages{}, nil, nil)
	got, err := a.List(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MachineId != "m1" {
		t.Fatalf("unexpected filter result: %+v", got)
	}
}

func TestAdapterDeleteResolvesNameToInternalID(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(context.Background(), "m1-test"); err != nil {
		t.Fatal(err)
	}
	if im.deleted != "internal-1" {
		t.Fatalf("deleted %q, want internal-1", im.deleted)
	}
}

// M4.5 autoresume：GetEndpoint 遇 Standby 实例时同步唤醒并返回新地址。
func TestGetEndpointAutoResumesStandby(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	// 转 standby（模拟 scale-to-zero）。
	if _, err := a.Pause(context.Background(), "m1-test", "e1"); err != nil {
		t.Fatal(err)
	}
	if im.created.State != instances.StateStandby {
		t.Fatalf("state = %s, want Standby", im.created.State)
	}
	ip, port, err := a.GetEndpoint(context.Background(), "m1-test", "e1")
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if im.created.State != instances.StateRunning {
		t.Fatalf("autoresume did not run: state=%s", im.created.State)
	}
	if ip == "" || port == 0 {
		t.Fatalf("endpoint after wake: ip=%q port=%d", ip, port)
	}
}

// 关闭 autoresume 时，standby 实例的 GetEndpoint 必须失败（不静默黑转发）。
func TestGetEndpointAutoResumeDisabled(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	a.SetAutoResume(false)
	if _, err := a.Pause(context.Background(), "m1-test", "e1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.GetEndpoint(context.Background(), "m1-test", "e1"); err == nil {
		t.Fatal("standby without autoresume must error")
	}
}

// P3-18：Pause/Resume 的 execution 绑定校验——旧代操作不得误停/误启新代。
func TestPauseResumeExecutionBinding(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	// execution 不匹配 → 拒绝。
	if _, err := a.Pause(context.Background(), "m1-test", "wrong-exec"); err == nil {
		t.Fatal("pause with stale execution must be rejected")
	}
	if im.created.State == instances.StateStandby {
		t.Fatal("rejected pause must not change state")
	}
	// 匹配 → 成功。
	if _, err := a.Pause(context.Background(), "m1-test", "e1"); err != nil {
		t.Fatalf("pause with current execution: %v", err)
	}
	if _, err := a.Resume(context.Background(), "m1-test", "wrong-exec"); err == nil {
		t.Fatal("resume with stale execution must be rejected")
	}
	if im.created.State != instances.StateStandby {
		t.Fatal("rejected resume must not change state")
	}
	if _, err := a.Resume(context.Background(), "m1-test", "e1"); err != nil {
		t.Fatalf("resume with current execution: %v", err)
	}
	if im.created.State != instances.StateRunning {
		t.Fatalf("resume must restore Running, got %s", im.created.State)
	}
}

// P2-9：实例不存在时 Pause/Resume 返回 ErrMachineNotFound（而非
// ErrImageNotFound 的镜像语义）。
func TestPauseResumeMachineNotFound(t *testing.T) {
	a := New(&fakeInstances{}, &fakeImages{}, nil, nil)
	_, err := a.Pause(context.Background(), "no-such", "e1")
	if !errors.Is(err, ErrMachineNotFound) {
		t.Fatalf("pause missing machine: want ErrMachineNotFound, got %v", err)
	}
	_, err = a.Resume(context.Background(), "no-such", "e1")
	if !errors.Is(err, ErrMachineNotFound) {
		t.Fatalf("resume missing machine: want ErrMachineNotFound, got %v", err)
	}
}
