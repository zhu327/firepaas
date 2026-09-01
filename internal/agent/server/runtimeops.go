// runtimeops.go：v1.2-C（ADR-0025）受控运行时通道的 gRPC 实现。
// StreamLogs（serial console）、Exec（双向流）、CopyTo/CopyFrom（单文件）。
//
// 安全边界：
//   - 全部会话绑定 machine+execution；旧 execution 立即拒绝；
//   - 并发会话、总字节、帧大小、空闲与总时长受 Server 的 runtime limits 约束；
//   - 审计只记录摘要（命令/路径摘要、字节、耗时、结果），不记录内容。
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	guestpb "github.com/kernel/hypeman/lib/guest"
	"github.com/zhu327/firepaas/internal/agent/machine"
	"github.com/zhu327/firepaas/internal/agent/mutation"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runtimeLimits 是 guest 运维通道的会话/资源约束（v1.2-C）。
type runtimeLimits struct {
	maxSessions int
	maxBytes    int64
	maxDuration time.Duration
	idleTimeout time.Duration
}

func defaultRuntimeLimits() runtimeLimits {
	return runtimeLimits{
		maxSessions: 16,
		maxBytes:    100 << 20, // 100 MiB（v1.2-C 验收边界）
		maxDuration: 15 * time.Minute,
		idleTimeout: 60 * time.Second,
	}
}

// WithRuntimeLimits 覆盖运维通道限制（0 值取默认）。
func WithRuntimeLimits(maxSessions int, maxBytes int64, maxDuration, idleTimeout time.Duration) Option {
	return func(s *Server) {
		if maxSessions > 0 {
			s.runtimeLimits.maxSessions = maxSessions
		}
		if maxBytes > 0 {
			s.runtimeLimits.maxBytes = maxBytes
		}
		if maxDuration > 0 {
			s.runtimeLimits.maxDuration = maxDuration
		}
		if idleTimeout > 0 {
			s.runtimeLimits.idleTimeout = idleTimeout
		}
	}
}

// sessionSem 限制并发 guest 会话数（v1.2-C：100 次建立/断开后资源回到基线）。
type sessionSem chan struct{}

func (s *Server) acquire(context.Context) (release func(), err error) {
	select {
	case s.runtimeSem <- struct{}{}:
		return func() { <-s.runtimeSem }, nil
	default:
		return nil, status.Error(codes.ResourceExhausted, "runtime session limit reached")
	}
}

// runtimeBoundary applies the common total-duration and idle limits. Every
// successful data transfer must call touch; cancel stops both timers.
func (s *Server) runtimeBoundary(parent context.Context) (context.Context, context.CancelFunc, func()) {
	durationCtx, durationCancel := context.WithTimeout(parent, s.runtimeLimits.maxDuration)
	ctx, cancel := context.WithCancel(durationCtx)
	reset := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(s.runtimeLimits.idleTimeout)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reset:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.runtimeLimits.idleTimeout)
			case <-timer.C:
				cancel()
				return
			}
		}
	}()
	stop := func() {
		cancel()
		durationCancel()
	}
	touch := func() {
		select {
		case reset <- struct{}{}:
		default:
		}
	}
	return ctx, stop, touch
}

// watchExecution terminates an established runtime session as soon as the
// machine no longer resolves to the execution captured at session creation.
func (s *Server) watchExecution(ctx context.Context, cancel context.CancelFunc, machineID, executionID string) {
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkCtx, checkCancel := context.WithTimeout(ctx, time.Second)
				err := s.machines.VerifyExecution(checkCtx, machineID, executionID)
				checkCancel()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
}

// mapGuestOpError 把 adapter 错误映射为 gRPC code（action-time capability /
// stale execution 语义，ADR-0023/0025）。
func mapGuestOpError(err error) error {
	switch {
	case errors.Is(err, machine.ErrStaleExecution):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, machine.ErrGuestOpsUnsupported):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, machine.ErrMachineNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// StreamLogs 实现 MachineService.StreamLogs。cursor 在 v1.2 未交付（计划
// 裁剪顺序允许）；传递非空 cursor 直接拒绝，不静默忽略语义。
func (s *Server) StreamLogs(req *pb.StreamLogsRequest, stream pb.MachineService_StreamLogsServer) error {
	if req == nil || req.MachineId == "" || req.ExecutionId == "" {
		return status.Error(codes.InvalidArgument, "machine_id and execution_id are required")
	}
	if req.Cursor != "" {
		return status.Error(codes.Unimplemented, "log cursor resume is not supported in v1.2")
	}
	release, err := s.acquire(stream.Context())
	if err != nil {
		return err
	}
	defer release()

	ctx, cancel, touch := s.runtimeBoundary(stream.Context())
	defer cancel()
	s.watchExecution(ctx, cancel, req.MachineId, req.ExecutionId)

	// 非奉 follow 请求默认取尾部 1000 行（全量 serial console 是无界 dump；
	// tail=true 从当前尾部开始 = 忽略历史，语义与 follow 一致）。
	tail := 1000
	if req.Tail {
		tail = 0
	}
	start := time.Now()
	ch, err := s.machines.StreamLogs(ctx, req.MachineId, req.ExecutionId, tail, req.Follow)
	if err != nil {
		s.auditRuntime("logs", req.MachineId, req.ExecutionId, "", 0, time.Since(start), err)
		return mapGuestOpError(err)
	}
	var sent int64
	for {
		select {
		case <-ctx.Done():
			s.auditRuntime("logs", req.MachineId, req.ExecutionId, "", sent, time.Since(start), ctx.Err())
			if stream.Context().Err() != nil {
				return nil
			}
			return status.Error(codes.DeadlineExceeded, "log stream duration limit reached")
		case line, ok := <-ch:
			if !ok {
				s.auditRuntime("logs", req.MachineId, req.ExecutionId, "", sent, time.Since(start), nil)
				return nil
			}
			if sent+int64(len(line)) > s.runtimeLimits.maxBytes {
				s.auditRuntime("logs", req.MachineId, req.ExecutionId, "", sent, time.Since(start),
					errors.New("log byte limit reached"))
				return status.Errorf(codes.ResourceExhausted,
					"log stream exceeds %d bytes", s.runtimeLimits.maxBytes)
			}
			touch()
			if err := stream.Send(&pb.LogChunk{Data: []byte(line)}); err != nil {
				return err
			}
			sent += int64(len(line))
		}
	}
}

// execSender 串行化对同一 gRPC stream 的并发 Send（gRPC 流非线程安全）。
type execSender struct {
	stream pb.MachineService_ExecServer
	mu     sync.Mutex
}

func (w *execSender) send(msg *pb.ExecOutput) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stream.Send(msg)
}

// execWriter 把 guest stdout/stderr 写回调 gRPC 流（同时喂醒空闲看门狗）。
type execWriter struct {
	sender   *execSender
	isStderr bool
	onWrite  func()
	budget   *byteBudget
}

type byteBudget struct {
	mu    sync.Mutex
	used  int64
	limit int64
}

func (b *byteBudget) add(n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+int64(n) > b.limit {
		return status.Error(codes.ResourceExhausted, "runtime stream byte limit exceeded")
	}
	b.used += int64(n)
	return nil
}

func (b *byteBudget) total() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func (w *execWriter) Write(p []byte) (int, error) {
	if err := w.budget.add(len(p)); err != nil {
		return 0, err
	}
	// 逐帧发送（不缓冲聚合）：延迟优先，也避免单帧过大。
	var out pb.ExecOutput
	if w.isStderr {
		out = pb.ExecOutput{Frame: &pb.ExecOutput_Stderr{Stderr: append([]byte(nil), p...)}}
	} else {
		out = pb.ExecOutput{Frame: &pb.ExecOutput_Stdout{Stdout: append([]byte(nil), p...)}}
	}
	if err := w.sender.send(&out); err != nil {
		return 0, err
	}
	if w.onWrite != nil {
		w.onWrite()
	}
	return len(p), nil
}

// Exec 实现 MachineService.Exec（双向流）。第一帧必须是 open。
// v1.2 不承诺 reattach/续传：客户端断开（stream context 取消）即终止会话。
// 约束：总时长 ≤ maxDuration；任何方向无流量的空闲时长 ≤ idleTimeout。
func (s *Server) Exec(stream pb.MachineService_ExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "first frame must be open: "+err.Error())
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first frame must be open")
	}
	if open.MachineId == "" || open.ExecutionId == "" || open.OperationId == "" || len(open.Command) == 0 {
		return status.Error(
			codes.InvalidArgument,
			"open.machine_id, open.execution_id, open.operation_id and open.command are required",
		)
	}
	requestHash := hashRequest(open)

	release, err := s.acquire(stream.Context())
	if err != nil {
		return err
	}
	defer release()

	created, _ := json.Marshal(map[string]string{"status": "created", "execution_id": open.ExecutionId})
	claimed, claimErr := mutation.ClaimExec(
		s.mutations,
		mutation.Identity{OperationID: open.OperationId, MachineID: open.MachineId, RequestHash: requestHash},
		created,
	)
	if claimErr != nil {
		return status.Error(codes.AlreadyExists, claimErr.Error())
	}
	if !claimed {
		return status.Error(codes.AlreadyExists, "exec operation already created; sessions cannot be reattached")
	}

	durationCtx, durationCancel := context.WithTimeout(stream.Context(), s.runtimeLimits.maxDuration)
	defer durationCancel()
	dialer, err := s.machines.GuestDialer(durationCtx, open.MachineId, open.ExecutionId)
	if err != nil {
		s.auditRuntime("exec", open.MachineId, open.ExecutionId,
			commandDigest(open.Command), 0, 0, err)
		return mapGuestOpError(err)
	}

	sender := &execSender{stream: stream}
	budget := &byteBudget{limit: s.runtimeLimits.maxBytes}
	stdinR, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()
	resizeCh := make(chan *guestpb.WindowSize, 8)

	// 空闲看门狗（ADR-0025 §6）：任何方向的活动都会重置；超时取消会话。
	idleReset := make(chan struct{}, 1)
	touchIdle := func() {
		select {
		case idleReset <- struct{}{}:
		default:
		}
	}
	execCtx, cancel := context.WithCancel(durationCtx)
	defer cancel()
	s.watchExecution(execCtx, cancel, open.MachineId, open.ExecutionId)
	go func() {
		timer := time.NewTimer(s.runtimeLimits.idleTimeout)
		defer timer.Stop()
		for {
			select {
			case <-execCtx.Done():
				return
			case <-idleReset:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.runtimeLimits.idleTimeout)
			case <-timer.C:
				_ = sender.send(&pb.ExecOutput{Frame: &pb.ExecOutput_Error{
					Error: fmt.Sprintf("exec session idle timeout (%s)", s.runtimeLimits.idleTimeout),
				}})
				cancel()
				return
			}
		}
	}()

	// 输入转发 goroutine：stdin/resize/signal 帧 → guest 流桥接。
	go func() {
		defer func() { _ = stdinW.Close() }()
		for {
			in, rerr := stream.Recv()
			if rerr != nil {
				return
			}
			touchIdle()
			switch f := in.Frame.(type) {
			case *pb.ExecInput_Stdin:
				if berr := budget.add(len(f.Stdin)); berr != nil {
					_ = sender.send(&pb.ExecOutput{Frame: &pb.ExecOutput_Error{Error: berr.Error()}})
					cancel()
					return
				}
				if _, werr := stdinW.Write(f.Stdin); werr != nil {
					return
				}
			case *pb.ExecInput_Resize:
				select {
				case resizeCh <- &guestpb.WindowSize{Rows: f.Resize.Rows, Cols: f.Resize.Cols}:
				default: // resize 只影响观感，丢弃积压帧
				}
			case *pb.ExecInput_Signal:
				// 上游缺口（ADR-0025 §4）：hypeman guest 通道只有 PID-1
				// Shutdown RPC，无 exec 子进程 signal。不能借道 Shutdown——
				// 那会杀死整个 VM。明确拒绝并保留契约字段。
				_ = sender.send(&pb.ExecOutput{Frame: &pb.ExecOutput_Error{
					Error: fmt.Sprintf("signal %d not supported: hypeman guest channel has no per-exec signal RPC (upstream gap, see docs/v1.2-implementation-notes.md)", f.Signal),
				}})
				_ = stdinW.Close()
				return
			case *pb.ExecInput_Open:
				// 会话建立后不允许第二个 open 帧。
				_ = sender.send(&pb.ExecOutput{Frame: &pb.ExecOutput_Error{
					Error: "duplicate open frame",
				}})
				_ = stdinW.Close()
				return
			}
		}
	}()

	start := time.Now()
	opts := guestpb.ExecOptions{
		Command:    open.Command,
		Stdin:      stdinR,
		Stdout:     &execWriter{sender: sender, onWrite: touchIdle, budget: budget},
		TTY:        open.Tty,
		Env:        open.Env,
		Cwd:        open.WorkingDir,
		ResizeChan: resizeCh,
	}
	if open.Tty {
		opts.Stderr = opts.Stdout // TTY 下 stderr 合并进 stdout（终端渲染语义）
	} else {
		opts.Stderr = &execWriter{sender: sender, isStderr: true, onWrite: touchIdle, budget: budget}
	}
	// 总时长限制覆盖 dial 与执行；空闲超时由看门狗单独取消。
	exit, execErr := guestpb.ExecIntoInstance(execCtx, dialer, opts)
	_ = stdinW.Close()
	cancel() // 停止看门狗
	if execErr != nil {
		if stream.Context().Err() == nil {
			_ = sender.send(&pb.ExecOutput{Frame: &pb.ExecOutput_Error{Error: execErr.Error()}})
		}
		s.auditRuntime("exec", open.MachineId, open.ExecutionId,
			commandDigest(open.Command), budget.total(), time.Since(start), execErr)
		return nil // 已把错误作为会话级帧发出；流按正常结束
	}
	_ = sender.send(&pb.ExecOutput{Frame: &pb.ExecOutput_ExitCode{ExitCode: int32(exit.Code)}})
	s.auditRuntime("exec", open.MachineId, open.ExecutionId,
		commandDigest(open.Command), budget.total(), time.Since(start), nil)
	return nil
}

// CopyTo 实现 MachineService.CopyTo（client-streaming）：第一帧 open + 数据帧。
// 只接受普通文件（guest agent 最终检查），单流 ≤ runtimeLimits.maxBytes。
func (s *Server) CopyTo(stream pb.MachineService_CopyToServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "first frame must be open: "+err.Error())
	}
	open := first.GetOpen()
	if open == nil || open.MachineId == "" || open.ExecutionId == "" || open.Generation == 0 || open.OperationId == "" {
		return status.Error(
			codes.InvalidArgument,
			"first frame must be open with machine_id/execution_id/generation/operation_id",
		)
	}
	if err := machine.ValidateGuestFilePath(open.Path); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	release, err := s.acquire(stream.Context())
	if err != nil {
		return err
	}
	defer release()

	ctx, cancel, touch := s.runtimeBoundary(stream.Context())
	defer cancel()
	start := time.Now()
	payload, err := os.CreateTemp("", "firepaas-copy-to-*")
	if err != nil {
		return status.Error(codes.Internal, "create upload spool: "+err.Error())
	}
	defer func() { _ = payload.Close(); _ = os.Remove(payload.Name()) }()
	digest := sha256.New()
	_, _ = digest.Write([]byte(hashRequest(open)))

	type copyToRecv struct {
		in  *pb.CopyToInput
		err error
	}
	recvCh := make(chan copyToRecv)
	go func() {
		for {
			in, rerr := stream.Recv()
			select {
			case recvCh <- copyToRecv{in: in, err: rerr}:
			case <-ctx.Done():
				return
			}
			if rerr != nil {
				return
			}
		}
	}()

	var total int64
	for {
		var received copyToRecv
		select {
		case <-ctx.Done():
			return status.Error(codes.DeadlineExceeded, "copy stream time limit reached")
		case received = <-recvCh:
		}
		in, rerr := received.in, received.err
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			s.auditRuntime(
				"cp-to",
				open.MachineId,
				open.ExecutionId,
				pathDigest(open.Path),
				total,
				time.Since(start),
				rerr,
			)
			return status.Error(codes.Internal, rerr.Error())
		}
		if in.GetOpen() != nil {
			return status.Error(codes.InvalidArgument, "open frame must only appear first")
		}
		data := in.GetData()
		touch()
		total += int64(len(data))
		if total > s.runtimeLimits.maxBytes {
			s.auditRuntime("cp-to", open.MachineId, open.ExecutionId, pathDigest(open.Path), total, time.Since(start),
				errors.New("size limit exceeded"))
			return status.Error(codes.ResourceExhausted, "copy size limit exceeded")
		}
		if len(data) > 0 {
			_, _ = digest.Write(data)
			if _, err := payload.Write(data); err != nil {
				return status.Error(codes.Internal, "spool upload: "+err.Error())
			}
		}
	}
	requestHash := hex.EncodeToString(digest.Sum(nil))
	response, err := mutation.RunCopyTo(
		s.mutations,
		mutation.CopyTo[*pb.CopyToResponse]{Lifecycle: mutation.Lifecycle[*pb.CopyToResponse]{
			Identity: mutation.Identity{
				OperationID: open.OperationId,
				MachineID:   open.MachineId,
				ExecutionID: open.ExecutionId,
				Generation:  open.Generation,
				RequestHash: requestHash,
			},
			Effect: func() (*pb.CopyToResponse, error) {
				dialer, err := s.machines.GuestDialer(ctx, open.MachineId, open.ExecutionId)
				if err != nil {
					return nil, mapGuestOpError(err)
				}
				conn, err := guestpb.GetOrCreateConn(ctx, dialer)
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				gstream, err := guestpb.NewGuestServiceClient(conn).CopyToGuest(ctx)
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				mode := open.Mode
				if mode == 0 {
					mode = 0o644
				}
				if err := gstream.Send(&guestpb.CopyToGuestRequest{Request: &guestpb.CopyToGuestRequest_Start{Start: &guestpb.CopyToGuestStart{Path: open.Path, Mode: mode}}}); err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				if _, err := payload.Seek(0, io.SeekStart); err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				buf := make([]byte, 32*1024)
				for {
					n, readErr := payload.Read(buf)
					if n > 0 {
						if err := gstream.Send(&guestpb.CopyToGuestRequest{Request: &guestpb.CopyToGuestRequest_Data{Data: buf[:n]}}); err != nil {
							return nil, status.Error(codes.Internal, err.Error())
						}
					}
					if readErr == io.EOF {
						break
					}
					if readErr != nil {
						return nil, status.Error(codes.Internal, readErr.Error())
					}
				}
				if err := gstream.Send(&guestpb.CopyToGuestRequest{Request: &guestpb.CopyToGuestRequest_End{End: &guestpb.CopyToGuestEnd{}}}); err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				guestResp, err := gstream.CloseAndRecv()
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
				if !guestResp.Success {
					return nil, status.Error(codes.Internal, guestResp.Error)
				}
				return &pb.CopyToResponse{BytesWritten: uint64(total)}, nil
			},
			Codec: mutation.Codec[*pb.CopyToResponse]{
				Encode: func(v *pb.CopyToResponse) (json.RawMessage, error) { return json.Marshal(v) },
				Decode: func(raw json.RawMessage) (*pb.CopyToResponse, error) {
					var v pb.CopyToResponse
					err := json.Unmarshal(raw, &v)
					return &v, err
				},
			},
		}},
	)
	if err != nil {
		err = mutationError(err)
		s.auditRuntime("cp-to", open.MachineId, open.ExecutionId, pathDigest(open.Path), total, time.Since(start), err)
		return err
	}
	s.auditRuntime("cp-to", open.MachineId, open.ExecutionId, pathDigest(open.Path), total, time.Since(start), nil)
	return stream.SendAndClose(response)
}

// CopyFrom 实现 MachineService.CopyFrom（server-streaming）：首帧 header，
// 随后数据帧。只允许普通文件（不跟随 symlink）。
func (s *Server) CopyFrom(req *pb.CopyFromRequest, stream pb.MachineService_CopyFromServer) error {
	if req == nil || req.MachineId == "" || req.ExecutionId == "" {
		return status.Error(codes.InvalidArgument, "machine_id and execution_id are required")
	}
	if err := machine.ValidateGuestFilePath(req.Path); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	release, err := s.acquire(stream.Context())
	if err != nil {
		return err
	}
	defer release()

	ctx, cancel, touch := s.runtimeBoundary(stream.Context())
	defer cancel()
	s.watchExecution(ctx, cancel, req.MachineId, req.ExecutionId)
	dialer, err := s.machines.GuestDialer(ctx, req.MachineId, req.ExecutionId)
	if err != nil {
		s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), 0, 0, err)
		return mapGuestOpError(err)
	}

	start := time.Now()
	conn, err := guestpb.GetOrCreateConn(ctx, dialer)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	client := guestpb.NewGuestServiceClient(conn)
	gstream, err := client.CopyFromGuest(ctx, &guestpb.CopyFromGuestRequest{
		Path: req.Path, FollowLinks: false,
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var headerSent bool
	var total int64
	for {
		resp, rerr := gstream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			s.auditRuntime(
				"cp-from",
				req.MachineId,
				req.ExecutionId,
				pathDigest(req.Path),
				total,
				time.Since(start),
				rerr,
			)
			return status.Error(codes.Internal, rerr.Error())
		}
		touch()
		switch r := resp.Response.(type) {
		case *guestpb.CopyFromGuestResponse_Header:
			if r.Header.GetSize() < 0 || r.Header.GetSize() > s.runtimeLimits.maxBytes {
				s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), 0, time.Since(start), errors.New("declared size limit exceeded"))
				return status.Error(codes.ResourceExhausted, "copy declared size exceeds limit")
			}
			if r.Header.GetIsDir() {
				s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), 0, time.Since(start),
					errors.New("directory copy not supported in v1.2"))
				return status.Error(codes.InvalidArgument, "path is a directory; v1.2 cp supports single regular files only")
			}
			if r.Header.GetIsSymlink() {
				return status.Error(codes.InvalidArgument, "path is a symlink; symlink follow disabled")
			}
			if err := stream.Send(&pb.CopyFromResponse{Frame: &pb.CopyFromResponse_Header{
				Header: &pb.CopyFromHeader{Path: r.Header.Path, Size: uint64(r.Header.Size), Mode: r.Header.Mode},
			}}); err != nil {
				return err
			}
			headerSent = true
		case *guestpb.CopyFromGuestResponse_Data:
			total += int64(len(r.Data))
			if total > s.runtimeLimits.maxBytes {
				s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), total, time.Since(start),
					errors.New("size limit exceeded"))
				return status.Error(codes.ResourceExhausted, "copy size limit exceeded")
			}
			if err := stream.Send(&pb.CopyFromResponse{Frame: &pb.CopyFromResponse_Data{Data: r.Data}}); err != nil {
				return err
			}
		case *guestpb.CopyFromGuestResponse_End:
			s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), total, time.Since(start), nil)
			if !headerSent {
				return status.Error(codes.NotFound, "file not found in guest")
			}
			return nil
		case *guestpb.CopyFromGuestResponse_Error:
			s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), total, time.Since(start),
				errors.New(r.Error.Message))
			return status.Error(codes.NotFound, "copy error: "+r.Error.Message)
		}
	}
	s.auditRuntime("cp-from", req.MachineId, req.ExecutionId, pathDigest(req.Path), total, time.Since(start),
		errors.New("stream ended without completion marker"))
	return status.Error(codes.Internal, "copy stream ended without completion marker")
}

// auditRuntime 记一条不含内容的运维通道审计（ADR-0025 §7）。
func (s *Server) auditRuntime(kind, machineID, executionID, digest string, bytes int64, took time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	slog.Info("runtime session audit",
		"kind", kind, "machine_id", machineID, "execution_id", executionID,
		"path_or_command_digest", digest, "bytes", bytes,
		"duration_ms", took.Milliseconds(), "result", result)
}

func commandDigest(argv []string) string {
	sum := sha256.Sum256([]byte(joinForDigest(argv)))
	return hex.EncodeToString(sum[:4])
}

func pathDigest(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:4])
}

func joinForDigest(argv []string) string {
	out := ""
	for i, a := range argv {
		if i > 0 {
			out += "\x00"
		}
		out += a
	}
	return out
}
