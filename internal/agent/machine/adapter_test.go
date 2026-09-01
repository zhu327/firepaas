package machine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kernel/hypeman/lib/images"
	"github.com/kernel/hypeman/lib/instances"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

type fakeInstances struct {
	created *instances.Instance
	got     *instances.Instance // GetInstance 固定返回（endpoint 测试用）
	listed  []instances.Instance
	deleted string
	err     error
}

func (f *fakeInstances) CreateInstance(
	_ context.Context,
	req instances.CreateInstanceRequest,
) (*instances.Instance, error) {
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
			AutoStandby: req.AutoStandby, // v1.1：透传策略供断言
			CreatedAt:   time.Now(),
		},
		State: instances.StateInitializing,
	}
	f.created = inst
	return inst, nil
}

func (f *fakeInstances) ListInstances(
	_ context.Context,
	_ *instances.ListInstancesFilter,
) ([]instances.Instance, error) {
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
	if f.got != nil {
		return f.got, nil
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
func (f *fakeInstances) StandbyInstance(
	_ context.Context,
	id string,
	_ instances.StandbyInstanceRequest,
) (*instances.Instance, error) {
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

type fakeImages struct {
	err     error
	list    []images.Image
	deleted string
}

func (f *fakeImages) CreateImage(context.Context, images.CreateImageRequest) (*images.Image, error) {
	return nil, f.err
}
func (f *fakeImages) WaitForReady(context.Context, string) error { return f.err }

func (f *fakeImages) GetImage(context.Context, string) (*images.Image, error) {
	if f.err != nil {
		return nil, f.err
	}
	sz := int64(50 << 20) // 50MiB：低于默认上限，高于本文件超限测试自设阈值
	return &images.Image{Name: "docker.io/library/nginx:alpine", SizeBytes: &sz}, nil
}

func (f *fakeImages) ListImages(context.Context) ([]images.Image, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.list != nil {
		return f.list, nil
	}
	sz := int64(50 << 20)
	return []images.Image{
		{
			Name:   "docker.io/library/nginx:alpine@sha256:" + strings.Repeat("a", 64),
			Status: images.StatusReady, SizeBytes: &sz,
		},
	}, nil
}

func (f *fakeImages) DeleteImage(_ context.Context, name string) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = name
	return nil
}

func validCreateRequest() *pb.CreateMachineRequest {
	return &pb.CreateMachineRequest{
		MachineId:   "m1-test",
		Generation:  1,
		OperationId: "op-1",
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

func TestDeleteImageRejectsLiveInstanceReference(t *testing.T) {
	ref := "repo/app@sha256:live"
	ims := &fakeImages{list: []images.Image{{Name: ref, Digest: "sha256:live", Status: images.StatusReady}}}
	inst := &fakeInstances{
		listed: []instances.Instance{{StoredMetadata: instances.StoredMetadata{Name: "m1", Image: ref}}},
	}
	err := New(inst, ims, nil, nil).DeleteImage(context.Background(), "sha256:live")
	if err == nil || !strings.Contains(err.Error(), "referenced by live instance") {
		t.Fatalf("DeleteImage error = %v, want live reference rejection", err)
	}
	if ims.deleted != "" {
		t.Fatalf("deleted live image %q", ims.deleted)
	}
}

func TestDeleteImageDeletesResolvedDigest(t *testing.T) {
	ref := "repo/app@sha256:unused"
	ims := &fakeImages{list: []images.Image{{Name: ref, Digest: "sha256:unused", Status: images.StatusReady}}}
	if err := New(&fakeInstances{}, ims, nil, nil).DeleteImage(context.Background(), "sha256:unused"); err != nil {
		t.Fatal(err)
	}
	if ims.deleted != ref {
		t.Fatalf("deleted %q, want %q", ims.deleted, ref)
	}
}

type blockingPullImages struct {
	fakeImages
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingPullImages) CreateImage(ctx context.Context, _ images.CreateImageRequest) (*images.Image, error) {
	f.once.Do(func() { close(f.started) })
	select {
	case <-f.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestActivePullHoldsImageDeletionLock(t *testing.T) {
	ref := "repo/app@sha256:" + strings.Repeat("a", 64)
	ims := &blockingPullImages{
		fakeImages: fakeImages{
			list: []images.Image{{Name: ref, Digest: imageDigestFromName(ref), Status: images.StatusReady}},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	a := New(&fakeInstances{}, ims, nil, nil)
	pullDone := make(chan error, 1)
	go func() { pullDone <- a.EnsureImage(context.Background(), ref) }()
	<-ims.started
	if a.imageUseMu.TryLock() {
		a.imageUseMu.Unlock()
		t.Fatal("active pull did not protect image from deletion")
	}
	close(ims.release)
	if err := <-pullDone; err != nil {
		t.Fatalf("pull: %v", err)
	}
	if err := a.DeleteImage(context.Background(), ref); err != nil {
		t.Fatalf("delete after pull: %v", err)
	}
}

type blockingCreateInstances struct {
	fakeInstances
	started chan struct{}
	release chan struct{}
}

func (f *blockingCreateInstances) CreateInstance(
	ctx context.Context,
	req instances.CreateInstanceRequest,
) (*instances.Instance, error) {
	close(f.started)
	select {
	case <-f.release:
		return f.fakeInstances.CreateInstance(ctx, req)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCreateHoldsImageDeletionLockUntilReferencePublication(t *testing.T) {
	ref := "docker.io/library/nginx:alpine@sha256:" + strings.Repeat("a", 64)
	ims := &fakeImages{}
	inst := &blockingCreateInstances{started: make(chan struct{}), release: make(chan struct{})}
	a := New(inst, ims, nil, nil)
	createDone := make(chan error, 1)
	go func() {
		req := validCreateRequest()
		req.Spec.ImageRef = ref
		_, err := a.Create(context.Background(), req)
		createDone <- err
	}()
	<-inst.started
	if a.imageUseMu.TryLock() {
		a.imageUseMu.Unlock()
		t.Fatal("create did not protect image before instance reference publication")
	}
	close(inst.release)
	if err := <-createDone; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := a.DeleteImage(context.Background(), ref); err == nil ||
		!strings.Contains(err.Error(), "referenced by live instance") {
		t.Fatalf("delete after create = %v, want live reference rejection", err)
	}
	if ims.deleted != "" {
		t.Fatalf("deleted image used by new instance: %q", ims.deleted)
	}
}

func TestCachedImageDigestsNewestFirst(t *testing.T) {
	old, newest := time.Now().Add(-time.Hour), time.Now()
	img := &fakeImages{list: []images.Image{
		{Name: "repo/old@sha256:old", Status: images.StatusReady, CreatedAt: old},
		{Name: "repo/new@sha256:new", Status: images.StatusReady, CreatedAt: newest},
	}}
	got := New(&fakeInstances{}, img, nil, nil).CachedImageDigests(context.Background(), 1)
	if len(got) != 1 || got[0] != "sha256:new" {
		t.Fatalf("cache truncation = %v, want newest digest", got)
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
	if got := m.Spec.Env["PORT"]; got != "8080" {
		t.Fatalf("env mapping: PORT=%q", got)
	}
	if im.created.Name != "m1-test" || im.created.Vcpus != 1 || im.created.Size != 512*1024*1024 {
		t.Fatalf("unexpected hypeman request mapping: %+v", im.created)
	}
}

func TestAdapterCreateRejectsSecretEnvWithoutPersistingIt(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	req := validCreateRequest()
	req.SecretEnv = map[string]string{"SECRET_TOKEN": "s3cr3t"}
	_, err := a.Create(context.Background(), req)
	if !errors.Is(err, ErrSecretEnvInjectionUnsupported) {
		t.Fatalf("Create error = %v, want ErrSecretEnvInjectionUnsupported", err)
	}
	if im.created != nil {
		t.Fatal("CreateInstance must not be called with secret_env")
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
		{
			StoredMetadata: instances.StoredMetadata{
				Id:    "i1",
				Name:  "m1",
				Image: "img",
				Tags:  map[string]string{tagProject: "p1", tagExecution: "e1"},
			},
			State: instances.StateRunning,
		},
		{
			StoredMetadata: instances.StoredMetadata{
				Id:    "i2",
				Name:  "m2",
				Image: "img",
				Tags:  map[string]string{tagProject: "p2", tagExecution: "e2"},
			},
			State: instances.StateStopped,
		},
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
	if err := a.Delete(context.Background(), "m1-test", "e1"); err != nil {
		t.Fatal(err)
	}
	if im.deleted != "internal-1" {
		t.Fatalf("deleted %q, want internal-1", im.deleted)
	}
}

func TestAdapterDeleteRequiresMatchingExecution(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	if _, err := a.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(context.Background(), "m1-test", "stale-execution"); err == nil {
		t.Fatal("delete with stale execution must fail")
	}
	if im.deleted != "" {
		t.Fatal("delete with stale execution must not call hypeman")
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

// M5.1：解包大小超限 = 永久错误（ErrImageTooBig）。
func TestEnsureImageReadySizeLimit(t *testing.T) {
	im := &fakeInstances{}
	img := &fakeImages{}
	a := New(im, img, nil, nil)
	// 上限 1MiB，fake 镜像 50MiB。
	a.SetMaxUnpackMib(1)
	err := a.ensureImageReady(context.Background(), "docker.io/library/nginx:alpine")
	if !errors.Is(err, ErrImageTooBig) {
		t.Fatalf("err = %v, want ErrImageTooBig", err)
	}
	// 上限调高后通过。
	a.SetMaxUnpackMib(1000)
	if err := a.ensureImageReady(context.Background(), "docker.io/library/nginx:alpine"); err != nil {
		t.Fatalf("within limit must pass: %v", err)
	}
}

// M5 评审决策：secret_env 默认 fail-closed；opt-in（unsafe-persisted-env）
// 恢复 M4 注入语义。
func TestSecretEnvInjectionPolicy(t *testing.T) {
	im := &fakeInstances{}
	img := &fakeImages{}
	req := validCreateRequest()
	req.SecretEnv = map[string]string{"TOKEN": "v"}

	// 默认拒绝。
	a := New(im, img, nil, nil)
	if _, err := a.Create(context.Background(), req); !errors.Is(err, ErrSecretEnvInjectionUnsupported) {
		t.Fatalf("default must fail closed, got %v", err)
	}
	// opt-in 放行（值进入 hypeman Env）。
	a.SetSecretInjection(SecretInjectionUnsafePersistedEnv)
	if _, err := a.Create(context.Background(), req); err != nil {
		t.Fatalf("opt-in injection must pass: %v", err)
	}
	if v, ok := im.created.Env["TOKEN"]; !ok || v != "v" {
		t.Fatalf("secret not injected into env: %+v", im.created.Env)
	}
}

// v1.1（ADR-0017）：auto_standby 策略翻译——enabled 时下发 hypeman Policy，
// 未声明/关闭时不下发（行为与 M5 完全一致）。
func TestAdapterCreateTranslatesAutoStandby(t *testing.T) {
	im := &fakeInstances{}
	a := New(im, &fakeImages{}, nil, nil)
	req := validCreateRequest()
	req.Spec.AutoStandby = &pb.AutoStandbyPolicy{Enabled: true, IdleTimeoutSeconds: 120}
	if _, err := a.Create(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if im.created.AutoStandby == nil || !im.created.AutoStandby.Enabled ||
		im.created.AutoStandby.IdleTimeout != "2m0s" {
		t.Fatalf("auto_standby translation: %+v", im.created.AutoStandby)
	}

	// 未声明 → 不下发。
	im2 := &fakeInstances{}
	a2 := New(im2, &fakeImages{}, nil, nil)
	if _, err := a2.Create(context.Background(), validCreateRequest()); err != nil {
		t.Fatal(err)
	}
	if im2.created.AutoStandby != nil {
		t.Fatalf("unset policy must not be sent, got %+v", im2.created.AutoStandby)
	}

	// 显式关闭 → 不下发。
	im3 := &fakeInstances{}
	a3 := New(im3, &fakeImages{}, nil, nil)
	req3 := validCreateRequest()
	req3.Spec.AutoStandby = &pb.AutoStandbyPolicy{Enabled: false}
	if _, err := a3.Create(context.Background(), req3); err != nil {
		t.Fatal(err)
	}
	if im3.created.AutoStandby != nil {
		t.Fatalf("disabled policy must not be sent, got %+v", im3.created.AutoStandby)
	}
}

// v1.1（ADR-0017）：非法策略（idle_timeout < 5s）拒绝。
func TestAdapterCreateRejectsInvalidAutoStandby(t *testing.T) {
	a := New(&fakeInstances{}, &fakeImages{}, nil, nil)
	req := validCreateRequest()
	req.Spec.AutoStandby = &pb.AutoStandbyPolicy{Enabled: true, IdleTimeoutSeconds: 2}
	if _, err := a.Create(context.Background(), req); !errors.Is(err, ErrInvalidAutoStandby) {
		t.Fatalf("error = %v, want ErrInvalidAutoStandby", err)
	}
}

// v1.1（ADR-0022）：services tag 编解码 + GetEndpointForPort 白名单。
func TestAdapterServicesTagRoundTrip(t *testing.T) {
	spec := &pb.MachineSpec{
		Network:  &pb.NetworkSpec{IngressPort: 80},
		Services: []*pb.ServiceSpec{{Name: "http", InternalPort: 80}, {Name: "grpc", InternalPort: 8081}},
	}
	tag, err := encodeServicesTag(spec)
	if err != nil {
		t.Fatal(err)
	}
	if tag == "" {
		t.Fatal("multi-service spec must encode a tag")
	}
	decoded := decodeServicesTag(tag)
	if len(decoded) != 1 || decoded[0].Name != "grpc" || decoded[0].Port != 8081 {
		t.Fatalf("decoded = %+v (primary must stay in tagPort)", decoded)
	}

	// 单端口 spec：无附加 service → 空 tag。
	single := &pb.MachineSpec{Network: &pb.NetworkSpec{IngressPort: 8080}}
	if tag, err := encodeServicesTag(single); err != nil || tag != "" {
		t.Fatalf("single-port spec must produce empty tag, got %q err=%v", tag, err)
	}
}

func TestGetEndpointForPortWhitelist(t *testing.T) {
	running := instances.Instance{
		StoredMetadata: instances.StoredMetadata{
			Id: "i1", Name: "m1", IP: "10.100.0.5",
			Tags: map[string]string{
				tagExecution: "e1",
				tagPort:      "80",
				tagServices:  encode(t, []svcJSON{{Name: "grpc", Port: 8081}}),
			},
		},
		State: instances.StateRunning,
	}
	im := &fakeInstances{got: &running}
	a := New(im, &fakeImages{}, nil, nil)

	// 主端口（wantPort=0 = 旧行为）。
	if _, port, err := a.GetEndpointForPort(context.Background(), "m1", "e1", 0); err != nil || port != 80 {
		t.Fatalf("primary endpoint: port=%d err=%v", port, err)
	}
	// 附加端口命中。
	if _, port, err := a.GetEndpointForPort(context.Background(), "m1", "e1", 8081); err != nil || port != 8081 {
		t.Fatalf("declared service port: port=%d err=%v", port, err)
	}
	// 未声明端口拒绝。
	if _, _, err := a.GetEndpointForPort(context.Background(), "m1", "e1", 9999); !errors.Is(err, ErrPortNotAllowed) {
		t.Fatalf("undeclared port error = %v, want ErrPortNotAllowed", err)
	}
}

func encode(t *testing.T, list []svcJSON) string {
	t.Helper()
	tag, err := encodeServicesTag(&pb.MachineSpec{
		Network: &pb.NetworkSpec{IngressPort: 80},
		Services: []*pb.ServiceSpec{
			{Name: "http", InternalPort: 80},
			{Name: list[0].Name, InternalPort: uint32(list[0].Port)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return tag
}
