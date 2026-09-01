package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/info"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/state"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"github.com/kernel/hypeman/lib/hypervisor"
	"github.com/kernel/hypeman/lib/instances"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// logsFakeInstances 实现 machine.InstanceManager + machine.logStreamProvider。
type logsFakeInstances struct {
	listed []instances.Instance
	lines  []string
}

func (f *logsFakeInstances) CreateInstance(context.Context, instances.CreateInstanceRequest) (*instances.Instance, error) {
	return nil, errors.New("not implemented")
}
func (f *logsFakeInstances) ListInstances(context.Context, *instances.ListInstancesFilter) ([]instances.Instance, error) {
	return f.listed, nil
}
func (f *logsFakeInstances) GetInstance(_ context.Context, idOrName string) (*instances.Instance, error) {
	for i := range f.listed {
		if f.listed[i].Id == idOrName || f.listed[i].Name == idOrName {
			cp := f.listed[i]
			return &cp, nil
		}
	}
	return nil, instances.ErrNotFound
}
func (f *logsFakeInstances) DeleteInstance(context.Context, string) error { return nil }
func (f *logsFakeInstances) StandbyInstance(context.Context, string, instances.StandbyInstanceRequest) (*instances.Instance, error) {
	return nil, nil
}
func (f *logsFakeInstances) RestoreInstance(context.Context, string) (*instances.Instance, error) {
	return nil, nil
}
func (f *logsFakeInstances) StreamInstanceLogs(context.Context, string, int, bool, instances.LogSource) (<-chan string, error) {
	ch := make(chan string, len(f.lines))
	for _, l := range f.lines {
		ch <- l
	}
	close(ch)
	return ch, nil
}
func (f *logsFakeInstances) GetVsockDialer(context.Context, string) (hypervisor.VsockDialer, error) {
	return nil, errors.New("no vsock")
}

func runtimeServer(t *testing.T) *Server {
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
	fake := &logsFakeInstances{listed: []instances.Instance{{
		StoredMetadata: instances.StoredMetadata{
			Id: "internal-1", Name: "m1",
			Tags: map[string]string{"firepaas/execution_id": "exec-1"},
		},
		State: instances.StateRunning,
	}}}
	adapter := machine.New(fake, fakeImages{}, nil, nil)
	ip := info.New("node-1", "test", "test", "compute", "v1", "", "/tmp", nil, nil)
	return New(adapter, ledger, fences, ip, WithCredentialRequired(false))
}

// ---- gRPC 流测试替身 ----

type streamRecorder struct {
	chunks []*pb.LogChunk
	ctx    context.Context
}

func (s *streamRecorder) Send(chunk *pb.LogChunk) error {
	s.chunks = append(s.chunks, chunk)
	return nil
}
func (s *streamRecorder) SetHeader(metadata.MD) error  { return nil }
func (s *streamRecorder) SendHeader(metadata.MD) error { return nil }
func (s *streamRecorder) SetTrailer(metadata.MD)       {}
func (s *streamRecorder) Context() context.Context     { return s.ctx }
func (s *streamRecorder) SendMsg(any) error            { return nil }
func (s *streamRecorder) RecvMsg(any) error            { return nil }

func TestStreamLogsStreamsChunks(t *testing.T) {
	// 用可注入 lines 的替身重建 server（Adapter 持有私有实例管理器）。
	dir := t.TempDir()
	ledger, _ := state.Open(filepath.Join(dir, "ledger.json"))
	fences, _ := state.OpenFences(filepath.Join(dir, "fences.json"))
	lf := &logsFakeInstances{
		listed: []instances.Instance{{
			StoredMetadata: instances.StoredMetadata{
				Id: "internal-1", Name: "m1",
				Tags: map[string]string{"firepaas/execution_id": "exec-1"},
			},
			State: instances.StateRunning,
		}},
		lines: []string{"hello\n", "world\n"},
	}
	adapter := machine.New(lf, fakeImages{}, nil, nil)
	ip := info.New("node-1", "test", "test", "compute", "v1", "", "/tmp", nil, nil)
	srv := New(adapter, ledger, fences, ip, WithCredentialRequired(false))

	rec := &streamRecorder{ctx: context.Background()}
	err := srv.StreamLogs(&pb.StreamLogsRequest{MachineId: "m1", ExecutionId: "exec-1", Follow: false}, rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(rec.chunks))
	}
	if string(rec.chunks[1].Data) != "world\n" {
		t.Fatalf("want second chunk, got %q", rec.chunks[1].Data)
	}
}

func TestStreamLogsRejectsCursorAndMissingFence(t *testing.T) {
	s := runtimeServer(t)
	rec := &streamRecorder{ctx: context.Background()}
	err := s.StreamLogs(&pb.StreamLogsRequest{MachineId: "m1", ExecutionId: "exec-1", Cursor: "c1"}, rec)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented for cursor, got %v", err)
	}
	err = s.StreamLogs(&pb.StreamLogsRequest{MachineId: "", ExecutionId: ""}, rec)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for missing fence, got %v", err)
	}
}

func TestStreamLogsRejectsStaleExecution(t *testing.T) {
	s := runtimeServer(t)
	rec := &streamRecorder{ctx: context.Background()}
	err := s.StreamLogs(&pb.StreamLogsRequest{MachineId: "m1", ExecutionId: "exec-old"}, rec)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition for stale execution, got %v", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "execution mismatch") {
		t.Fatalf("want execution mismatch message, got %v", err)
	}
}

// execFakeStream 提供 Exec 双向流的最小替身（一帧 open 后 EOF）。
type execFakeStream struct {
	frames []*pb.ExecInput
	recvN  int
	sent   []*pb.ExecOutput
	ctx    context.Context
}

func (s *execFakeStream) Send(msg *pb.ExecOutput) error { s.sent = append(s.sent, msg); return nil }
func (s *execFakeStream) Recv() (*pb.ExecInput, error) {
	if s.recvN >= len(s.frames) {
		return nil, errors.New("EOF")
	}
	f := s.frames[s.recvN]
	s.recvN++
	return f, nil
}
func (s *execFakeStream) SetHeader(metadata.MD) error  { return nil }
func (s *execFakeStream) SendHeader(metadata.MD) error { return nil }
func (s *execFakeStream) SetTrailer(metadata.MD)       {}
func (s *execFakeStream) Context() context.Context     { return s.ctx }
func (s *execFakeStream) SendMsg(any) error            { return nil }
func (s *execFakeStream) RecvMsg(any) error            { return nil }

func TestExecRequiresOperationID(t *testing.T) {
	s := runtimeServer(t)
	stream := &execFakeStream{ctx: context.Background(), frames: []*pb.ExecInput{{Frame: &pb.ExecInput_Open{Open: &pb.ExecOpen{
		MachineId: "m1", ExecutionId: "exec-1", Command: []string{"true"},
	}}}}}
	if err := s.Exec(stream); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for missing operation_id, got %v", err)
	}
}

func TestExecRejectsReusedOperationIDBeforeDial(t *testing.T) {
	s := runtimeServer(t)
	open := &pb.ExecOpen{MachineId: "m1", ExecutionId: "exec-1", OperationId: "op-1", Command: []string{"true"}}
	raw := []byte(`{"status":"created"}`)
	if err := s.ledger.Put(open.OperationId, open.MachineId, hashRequest(open), raw); err != nil {
		t.Fatal(err)
	}
	stream := &execFakeStream{ctx: context.Background(), frames: []*pb.ExecInput{{Frame: &pb.ExecInput_Open{Open: open}}}}
	if err := s.Exec(stream); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("want AlreadyExists for reused operation_id, got %v", err)
	}
}

func TestExecRequiresOpenFirstFrame(t *testing.T) {
	s := runtimeServer(t)
	stream := &execFakeStream{ctx: context.Background(),
		frames: []*pb.ExecInput{{Frame: &pb.ExecInput_Stdin{Stdin: []byte("x")}}}}
	err := s.Exec(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

// copyToFakeStream 提供 CopyTo client-streaming 替身。
type copyToFakeStream struct {
	frames   []*pb.CopyToInput
	recvN    int
	ctx      context.Context
	response *pb.CopyToResponse
}

func (s *copyToFakeStream) Recv() (*pb.CopyToInput, error) {
	if s.recvN >= len(s.frames) {
		return nil, io.EOF
	}
	f := s.frames[s.recvN]
	s.recvN++
	return f, nil
}
func (s *copyToFakeStream) SendAndClose(resp *pb.CopyToResponse) error { s.response = resp; return nil }
func (s *copyToFakeStream) SetHeader(metadata.MD) error                { return nil }
func (s *copyToFakeStream) SendHeader(metadata.MD) error               { return nil }
func (s *copyToFakeStream) SetTrailer(metadata.MD)                     {}
func (s *copyToFakeStream) Context() context.Context                   { return s.ctx }
func (s *copyToFakeStream) SendMsg(any) error                          { return nil }
func (s *copyToFakeStream) RecvMsg(any) error                          { return nil }

func TestCopyToRejectsPathTraversal(t *testing.T) {
	s := runtimeServer(t)
	stream := &copyToFakeStream{ctx: context.Background(),
		frames: []*pb.CopyToInput{{Frame: &pb.CopyToInput_Open{Open: &pb.CopyToOpen{
			MachineId: "m1", ExecutionId: "exec-1", Generation: 1, OperationId: "op-copy-path", Path: "/etc/../../etc/passwd",
		}}}}}
	err := s.CopyTo(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for traversal, got %v", err)
	}
}

func TestCopyToRequiresMutationFence(t *testing.T) {
	s := runtimeServer(t)
	stream := &copyToFakeStream{ctx: context.Background(), frames: []*pb.CopyToInput{{Frame: &pb.CopyToInput_Open{Open: &pb.CopyToOpen{
		MachineId: "m1", ExecutionId: "exec-1", Path: "/tmp/file",
	}}}}}
	if err := s.CopyTo(stream); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for missing generation/operation_id, got %v", err)
	}
}

func TestCopyToRejectsStaleGenerationBeforeGuestWrite(t *testing.T) {
	s := runtimeServer(t)
	if err := s.fences.Advance("m1", 2, "exec-1"); err != nil {
		t.Fatal(err)
	}
	stream := &copyToFakeStream{ctx: context.Background(), frames: []*pb.CopyToInput{
		{Frame: &pb.CopyToInput_Open{Open: &pb.CopyToOpen{MachineId: "m1", ExecutionId: "exec-1", Generation: 1, OperationId: "op-stale", Path: "/tmp/file"}}},
		{Frame: &pb.CopyToInput_Data{Data: []byte("content")}},
	}}
	if err := s.CopyTo(stream); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition for stale generation, got %v", err)
	}
}

func TestCopyToRejectsRequestHashConflict(t *testing.T) {
	s := runtimeServer(t)
	open := &pb.CopyToOpen{MachineId: "m1", ExecutionId: "exec-1", Generation: 1, OperationId: "op-copy", Path: "/tmp/file"}
	digest := sha256.New()
	_, _ = digest.Write([]byte(hashRequest(open)))
	_, _ = digest.Write([]byte("old"))
	if err := s.ledger.Put(open.OperationId, open.MachineId, hex.EncodeToString(digest.Sum(nil)), []byte(`{"bytes_written":3}`)); err != nil {
		t.Fatal(err)
	}
	stream := &copyToFakeStream{ctx: context.Background(), frames: []*pb.CopyToInput{
		{Frame: &pb.CopyToInput_Open{Open: open}}, {Frame: &pb.CopyToInput_Data{Data: []byte("new")}},
	}}
	if err := s.CopyTo(stream); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("want AlreadyExists for changed content, got %v", err)
	}
}

func TestCopyFromRejectsRelativePath(t *testing.T) {
	s := runtimeServer(t)
	err := s.CopyFrom(&pb.CopyFromRequest{MachineId: "m1", ExecutionId: "exec-1", Path: "relative/path"},
		&copyFromRecorder{ctx: context.Background()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for relative path, got %v", err)
	}
}

type copyFromRecorder struct {
	ctx context.Context
}

func (s *copyFromRecorder) Send(*pb.CopyFromResponse) error { return nil }
func (s *copyFromRecorder) SetHeader(metadata.MD) error     { return nil }
func (s *copyFromRecorder) SendHeader(metadata.MD) error    { return nil }
func (s *copyFromRecorder) SetTrailer(metadata.MD)          {}
func (s *copyFromRecorder) Context() context.Context        { return s.ctx }
func (s *copyFromRecorder) SendMsg(any) error               { return nil }
func (s *copyFromRecorder) RecvMsg(any) error               { return nil }
