package machine

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	guestpb "github.com/kernel/hypeman/lib/guest"
	"github.com/kernel/hypeman/lib/hypervisor"
	"github.com/kernel/hypeman/lib/instances"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

// ---------------------------------------------------------------------------
// v1.2-B（ADR-0024）：one-shot secret 通道测试。
// 用真实 gRPC guest server（本地 TCP 顶替 vsock）驱动 DeliverSecretFiles
// 的完整路径：tmpfs 目录创建、0400 文件写入、marker 最后写入。
// ---------------------------------------------------------------------------

// recordingGuestServer 记录 CopyToGuest 完成的写入（路径 + 内容 + 顺序）。
type recordingGuestServer struct {
	guestpb.UnimplementedGuestServiceServer
	mu    sync.Mutex
	files map[string][]byte
	order []string
	fail  bool
}

func (s *recordingGuestServer) CopyToGuest(stream guestpb.GuestService_CopyToGuestServer) error {
	var path string
	var content []byte
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch f := msg.GetRequest().(type) {
		case *guestpb.CopyToGuestRequest_Start:
			path = f.Start.Path
			if f.Start.IsDir {
				s.record(path, nil)
			}
		case *guestpb.CopyToGuestRequest_Data:
			content = append(content, f.Data...)
		}
	}
	if path != "" {
		s.record(path, content)
	}
	if s.fail {
		return status.Error(codes.Unavailable, "injected failure")
	}
	return stream.SendAndClose(&guestpb.CopyToGuestResponse{Success: true, BytesWritten: int64(len(content))})
}

func (s *recordingGuestServer) record(path string, content []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files == nil {
		s.files = map[string][]byte{}
	}
	s.files[path] = content
	s.order = append(s.order, path)
}

func (s *recordingGuestServer) snapshot() (map[string][]byte, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	files := make(map[string][]byte, len(s.files))
	for k, v := range s.files {
		files[k] = v
	}
	return files, append([]string(nil), s.order...)
}

// tcpVsockDialer 把 vsock 顶替为本地 TCP（连到 recordingGuestServer）。
type tcpVsockDialer struct{ addr string }

func (d *tcpVsockDialer) Key() string { return d.addr }
func (d *tcpVsockDialer) DialVsock(context.Context, int) (net.Conn, error) {
	return net.Dial("tcp", d.addr)
}

// vsockFakeInstances 在 fakeInstances 之上提供 GetVsockDialer，并捕获
// 原始 CreateInstanceRequest（fake 不回填 SecretDelivery 到 Instance）。
type vsockFakeInstances struct {
	fakeInstances
	guest  *recordingGuestServer
	addr   string
	gotReq *instances.CreateInstanceRequest
}

func (f *vsockFakeInstances) CreateInstance(
	ctx context.Context,
	req instances.CreateInstanceRequest,
) (*instances.Instance, error) {
	cp := req
	f.gotReq = &cp
	return f.fakeInstances.CreateInstance(ctx, req)
}

func (f *vsockFakeInstances) GetVsockDialer(context.Context, string) (hypervisor.VsockDialer, error) {
	if f.guest.fail {
		return nil, errors.New("vsock unavailable")
	}
	return &tcpVsockDialer{addr: f.addr}, nil
}

// newGuestTestEnv 启动真实 gRPC guest server（TCP 顶替 vsock）。
func newGuestTestEnv(t *testing.T) *vsockFakeInstances {
	t.Helper()
	srv := &recordingGuestServer{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	guestpb.RegisterGuestServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return &vsockFakeInstances{guest: srv, addr: lis.Addr().String()}
}

func secretCreateRequest() *pb.CreateMachineRequest {
	req := validCreateRequest()
	req.SecretEnv = map[string]string{"API_TOKEN": "canary-value"}
	req.SecretLeaseId = "sdl-test-exec1"
	return req
}

func TestOneShotDeliveryWritesTmpfsAndMarkerLast(t *testing.T) {
	fake := newGuestTestEnv(t)
	a := New(fake, &fakeImages{}, nil, nil)
	a.SetSecretInjection(SecretInjectionOneShot)

	req := secretCreateRequest()
	m, err := a.Create(context.Background(), req)
	if err != nil {
		t.Fatalf("oneshot create: %v", err)
	}

	// secret 值不得进入 hypeman Env（不落 metadata/config disk）。
	if _, ok := fake.created.Env["API_TOKEN"]; ok {
		t.Fatal("secret leaked into hypeman Env")
	}
	// release gate policy 必须下发。
	sd := fake.gotReq.SecretDelivery
	if sd == nil || sd.Dir != secretDir || sd.MarkerFile != secretMarkerFile || !sd.ExportAsEnv {
		t.Fatalf("SecretDelivery policy: %+v", sd)
	}
	// lease id 入 tag。
	if fake.created.Tags[tagSecretLease] != "sdl-test-exec1" {
		t.Fatalf("lease tag: %q", fake.created.Tags[tagSecretLease])
	}
	// guest 收到：目录、0400 文件、marker 最后。
	files, order := fake.guest.snapshot()
	if string(files[secretDir+"/API_TOKEN"]) != "canary-value" {
		t.Fatalf("secret file content: %q", files[secretDir+"/API_TOKEN"])
	}
	if len(order) < 3 {
		t.Fatalf("expected dir+file+marker writes, got %v", order)
	}
	if last := order[len(order)-1]; last != secretMarkerFile {
		t.Fatalf("marker must be written last, order=%v", order)
	}
	// 投递完成 = DELIVERED（ProgramStartedAt 未到）。
	if m.GetSecretDeliveryState() != pb.SecretDeliveryState_SECRET_DELIVERY_DELIVERED {
		t.Fatalf("delivery state: %v", m.GetSecretDeliveryState())
	}
	// 明文生命周期收口：请求内值被清零。
	if req.SecretEnv["API_TOKEN"] != "" {
		t.Fatal("secret plaintext not zeroed after delivery")
	}
}

func TestOneShotDeliveryFailureDestroysInstance(t *testing.T) {
	fake := newGuestTestEnv(t)
	fake.guest.fail = true
	a := New(fake, &fakeImages{}, nil, nil)
	a.SetSecretInjection(SecretInjectionOneShot)

	if _, err := a.Create(context.Background(), secretCreateRequest()); err == nil {
		t.Fatal("failing guest delivery must fail create")
	}
	if fake.deleted != fake.created.Id {
		t.Fatalf("failed delivery must destroy instance, deleted=%q", fake.deleted)
	}
}

func TestOneShotRejectsAutoStandby(t *testing.T) {
	fake := newGuestTestEnv(t)
	a := New(fake, &fakeImages{}, nil, nil)
	a.SetSecretInjection(SecretInjectionOneShot)
	req := secretCreateRequest()
	req.Spec.AutoStandby = &pb.AutoStandbyPolicy{Enabled: true, IdleTimeoutSeconds: 60}
	if _, err := a.Create(context.Background(), req); !errors.Is(err, ErrSecretSnapshotForbidden) {
		t.Fatalf("secret+auto-standby must be rejected, got %v", err)
	}
}

func TestPauseForbiddenForSecretExecution(t *testing.T) {
	fake := newGuestTestEnv(t)
	a := New(fake, &fakeImages{}, nil, nil)
	a.SetSecretInjection(SecretInjectionOneShot)
	req := secretCreateRequest()
	m, err := a.Create(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Pause(context.Background(), m.MachineId, m.ExecutionId); !errors.Is(
		err,
		ErrSecretSnapshotForbidden,
	) {
		t.Fatalf("pause must be forbidden for secret execution, got %v", err)
	}
}

func TestMapMachineSecretDeliveryStates(t *testing.T) {
	base := func() *instances.Instance {
		return &instances.Instance{
			StoredMetadata: instances.StoredMetadata{
				Id: "i-1", Name: "m1",
				Tags: map[string]string{tagExecution: "exec-1"},
			},
			State: instances.StateInitializing,
		}
	}
	inst := base()
	if got := mapMachine(inst).GetSecretDeliveryState(); got != pb.SecretDeliveryState_SECRET_DELIVERY_NONE {
		t.Fatalf("no lease tag: want NONE, got %v", got)
	}
	inst = base()
	inst.Tags[tagSecretLease] = "sdl-1"
	if got := mapMachine(inst).GetSecretDeliveryState(); got != pb.SecretDeliveryState_SECRET_DELIVERY_DELIVERED {
		t.Fatalf("lease without program start: want DELIVERED, got %v", got)
	}
	now := time.Now()
	inst.ProgramStartedAt = &now
	if got := mapMachine(inst).GetSecretDeliveryState(); got != pb.SecretDeliveryState_SECRET_DELIVERY_ACKED {
		t.Fatalf("lease + program started: want ACKED, got %v", got)
	}
}
