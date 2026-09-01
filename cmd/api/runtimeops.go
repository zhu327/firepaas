// runtimeops.go：v1.2-C（ADR-0025）REST 运行时通道。
//
//	GET  /v1/machines/{id}/logs?follow=&tail=   → 流式 app 日志（serial console）
//	POST /v1/machines/{id}/exec                → NDJSON 流：stdout/stderr/exit_code
//	PUT  /v1/machines/{id}/files?path=         → 单文件上传（≤100 MiB）
//	GET  /v1/machines/{id}/files?path=         → 单文件下载
//
// 数据路径固定为 client → API → mTLS agent → vsock guest agent；CLI 不直连
// 节点。审计只记录摘要，不记录 stdin/stdout/env/文件正文。
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/agentclient"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runtimeMaxBody 是 exec/cp 请求体的硬上限（v1.2-C：100 MiB 边界文件）。
const runtimeMaxBody = 100 << 20

// runtimeTarget 解析 machine 的 agent 客户端 + 节点能力（fail closed）。
func (a *API) runtimeTarget(r *http.Request) (*agentclient.Client, map[string]bool, error) {
	id := r.PathValue("id")
	if a.rgw == nil {
		return nil, nil, fmt.Errorf("runtime gateway not configured")
	}
	return a.rgw.Get(r.Context(), id)
}

// requireFeature 做 action-time capability 检查（ADR-0023/0025）。
func (a *API) requireFeature(w http.ResponseWriter, feats map[string]bool, feature string) bool {
	if feats[feature] {
		return true
	}
	writeErr(w, 409, "capability not supported by node: "+feature)
	if a.metrics != nil {
		a.metrics.Inc("firepaas_runtime_rejections_total", map[string]string{"reason": "capability_missing"}, 1)
	}
	return false
}

// grpcErrStatus 把 agent gRPC 错误映射为 HTTP 状态码。
func grpcErrStatus(err error) (int, string) {
	c := status.Code(err)
	switch c {
	case codes.NotFound:
		return 404, "machine not found at agent"
	case codes.FailedPrecondition:
		return 409, "stale execution: " + err.Error()
	case codes.AlreadyExists:
		return 409, "operation conflict: " + err.Error()
	case codes.Unimplemented:
		return 501, "guest operations unsupported: " + err.Error()
	case codes.ResourceExhausted:
		return 429, "runtime session limit: " + err.Error()
	case codes.InvalidArgument:
		return 400, err.Error()
	default:
		return 502, "agent error: " + err.Error()
	}
}

// machineLogs 流式返回 app 日志（Content-Type: text/plain，chunked）。
func (a *API) machineLogs(w http.ResponseWriter, r *http.Request) {
	tgt, feats, err := a.runtimeTarget(r)
	if err != nil {
		writeErr(w, 503, err.Error())
		return
	}
	if !a.requireFeature(w, feats, capabilities.GuestLogsV1) {
		return
	}
	// v1.2-E（ADR-0035）：runtime 会话并发（project 维度）。
	releaseSession, streamCtx, okSession := a.acquireRuntimeSession(w, r, r.PathValue("id"))
	if !okSession {
		return
	}
	defer releaseSession()
	m, err := a.store.GetMachine(r.Context(), r.PathValue("id"))
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	follow := r.URL.Query().Get("follow") == "true" || r.URL.Query().Get("follow") == "1"
	tail := r.URL.Query().Get("tail") == "true" || r.URL.Query().Get("tail") == "1"
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		writeErr(w, 400, "log cursor resume is not supported in v1.2")
		return
	}

	stream, err := tgt.Machines.StreamLogs(streamCtx, &pb.StreamLogsRequest{
		MachineId: m.ID, ExecutionId: m.CurrentExecutionID, Follow: follow, Tail: tail,
	})
	if err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(200)
	flusher, _ := w.(http.Flusher)
	var bytes int64
	result := "completed"
	for {
		chunk, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			result = "agent_error"
			slog.Warn("log stream ended", "machine_id", m.ID, "error", rerr)
			break
		}
		if _, werr := w.Write(chunk.Data); werr != nil {
			return
		}
		bytes += int64(len(chunk.Data))
		if flusher != nil {
			flusher.Flush()
		}
	}
	slog.Info("runtime logs audit", "caller", callerName(identFrom(r)),
		"machine_id", m.ID, "execution_id", m.CurrentExecutionID,
		"bytes", bytes, "follow", follow, "tail", tail, "result", result)
}

// execOpen 是 POST /v1/machines/{id}/exec 的请求体。
type execOpen struct {
	OperationID string            `json:"operation_id"`
	Command     []string          `json:"command"`
	Cwd         string            `json:"cwd"`
	Env         map[string]string `json:"env"`
	TTY         bool              `json:"tty"`
	Rows        uint32            `json:"rows"`
	Cols        uint32            `json:"cols"`
	Stdin       string            `json:"stdin"` // base64（可选，非交互 stdin）
}

// machineExec 执行命令并以 NDJSON 流返回 stdout/stderr/exit_code。
// v1.2 不承诺 reattach/续传；客户端断开即终止会话。
func (a *API) machineExec(w http.ResponseWriter, r *http.Request) {
	tgt, feats, err := a.runtimeTarget(r)
	if err != nil {
		writeErr(w, 503, err.Error())
		return
	}
	if !a.requireFeature(w, feats, capabilities.GuestExecV1) {
		return
	}
	// v1.2-E（ADR-0035）：runtime 会话并发（project 维度）。
	releaseSession, streamCtx, okSession := a.acquireRuntimeSession(w, r, r.PathValue("id"))
	if !okSession {
		return
	}
	defer releaseSession()
	m, err := a.store.GetMachine(r.Context(), r.PathValue("id"))
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	var open execOpen
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, runtimeMaxBody)).Decode(&open); err != nil {
		writeErr(w, 400, "bad request: "+err.Error())
		return
	}
	if len(open.Command) == 0 || open.OperationID == "" {
		writeErr(w, 400, "command and operation_id are required")
		return
	}
	if open.TTY && (open.Rows == 0 || open.Cols == 0) {
		open.Rows, open.Cols = 24, 80
	}

	stream, err := tgt.Machines.Exec(streamCtx)
	if err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	req := &pb.ExecInput{Frame: &pb.ExecInput_Open{Open: &pb.ExecOpen{
		MachineId: m.ID, ExecutionId: m.CurrentExecutionID, OperationId: open.OperationID,
		Command: open.Command, Env: open.Env, WorkingDir: open.Cwd, Tty: open.TTY,
	}}}
	if err := stream.Send(req); err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	if open.TTY {
		_ = stream.Send(&pb.ExecInput{Frame: &pb.ExecInput_Resize{
			Resize: &pb.ExecResize{Rows: open.Rows, Cols: open.Cols}}})
	}
	if open.Stdin != "" {
		raw, derr := base64.StdEncoding.DecodeString(open.Stdin)
		if derr != nil {
			_ = stream.CloseSend()
			writeErr(w, 400, "stdin must be base64")
			return
		}
		if err := stream.Send(&pb.ExecInput{Frame: &pb.ExecInput_Stdin{Stdin: raw}}); err != nil {
			_ = stream.CloseSend()
			code, msg := grpcErrStatus(err)
			writeErr(w, code, msg)
			return
		}
	}
	_ = stream.CloseSend()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(200)
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	start := time.Now()
	var outBytes int64
	result := "stream_ended"
	for {
		ev, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			code, msg := grpcErrStatus(rerr)
			result = "agent_error"
			_ = enc.Encode(map[string]any{"error": fmt.Sprintf("(%d) %s", code, msg)})
			break
		}
		switch f := ev.Frame.(type) {
		case *pb.ExecOutput_Stdout:
			outBytes += int64(len(f.Stdout))
			_ = enc.Encode(map[string]any{"stdout": base64.StdEncoding.EncodeToString(f.Stdout)})
		case *pb.ExecOutput_Stderr:
			outBytes += int64(len(f.Stderr))
			_ = enc.Encode(map[string]any{"stderr": base64.StdEncoding.EncodeToString(f.Stderr)})
		case *pb.ExecOutput_ExitCode:
			result = fmt.Sprintf("exit_%d", f.ExitCode)
			_ = enc.Encode(map[string]any{"exit_code": f.ExitCode})
		case *pb.ExecOutput_Error:
			result = "session_error"
			_ = enc.Encode(map[string]any{"error": f.Error})
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	slog.Info("runtime exec audit", "caller", callerName(identFrom(r)),
		"machine_id", m.ID, "execution_id", m.CurrentExecutionID,
		"command_digest", runtimeDigest(strings.Join(open.Command, "\x00")), "tty", open.TTY,
		"bytes", outBytes, "duration_ms", time.Since(start).Milliseconds(), "result", result)
}

// machineFilesPut 上传单个普通文件（请求体 = 文件内容）。
func (a *API) machineFilesPut(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > runtimeMaxBody {
		writeErr(w, http.StatusRequestEntityTooLarge, "file exceeds 100 MiB limit")
		return
	}
	tgt, feats, err := a.runtimeTarget(r)
	if err != nil {
		writeErr(w, 503, err.Error())
		return
	}
	if !a.requireFeature(w, feats, capabilities.GuestCopyV1) {
		return
	}
	// v1.2-E（ADR-0035）：runtime 会话并发（project 维度）。
	releaseSession, streamCtx, okSession := a.acquireRuntimeSession(w, r, r.PathValue("id"))
	if !okSession {
		return
	}
	defer releaseSession()
	m, err := a.store.GetMachine(r.Context(), r.PathValue("id"))
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	path := r.URL.Query().Get("path")
	operationID := r.URL.Query().Get("operation_id")
	if path == "" || operationID == "" {
		writeErr(w, 400, "path and operation_id query parameters are required")
		return
	}
	mode := uint32(0)
	if v := r.URL.Query().Get("mode"); v != "" {
		n, perr := strconv.ParseUint(v, 8, 32)
		if perr != nil {
			writeErr(w, 400, "mode must be octal")
			return
		}
		mode = uint32(n)
	}

	stream, err := tgt.Machines.CopyTo(streamCtx)
	if err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	if err := stream.Send(&pb.CopyToInput{Frame: &pb.CopyToInput_Open{Open: &pb.CopyToOpen{
		MachineId: m.ID, ExecutionId: m.CurrentExecutionID, Path: path, Mode: mode,
		Generation: uint64(m.Generation), OperationId: operationID,
	}}}); err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, rerr := io.ReadFull(r.Body, buf)
		if n > 0 {
			total += int64(n)
			if total > runtimeMaxBody {
				_ = stream.CloseSend()
				writeErr(w, 413, "file exceeds 100 MiB limit")
				return
			}
			if err := stream.Send(&pb.CopyToInput{Frame: &pb.CopyToInput_Data{Data: buf[:n]}}); err != nil {
				code, msg := grpcErrStatus(err)
				writeErr(w, code, msg)
				return
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			_ = stream.CloseSend()
			writeErr(w, 400, "read body: "+rerr.Error())
			return
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	slog.Info("runtime cp audit", "caller", callerName(identFrom(r)),
		"machine_id", m.ID, "execution_id", m.CurrentExecutionID,
		"direction", "upload", "path_digest", runtimeDigest(path),
		"bytes", resp.BytesWritten, "result", "completed")
	writeJSON(w, 200, map[string]any{"machine_id": m.ID, "path": path,
		"bytes_written": resp.BytesWritten})
}

// machineFilesGet 下载单个普通文件（原始字节流）。
func (a *API) machineFilesGet(w http.ResponseWriter, r *http.Request) {
	tgt, feats, err := a.runtimeTarget(r)
	if err != nil {
		writeErr(w, 503, err.Error())
		return
	}
	if !a.requireFeature(w, feats, capabilities.GuestCopyV1) {
		return
	}
	// v1.2-E（ADR-0035）：runtime 会话并发（project 维度）。
	releaseSession, streamCtx, okSession := a.acquireRuntimeSession(w, r, r.PathValue("id"))
	if !okSession {
		return
	}
	defer releaseSession()
	m, err := a.store.GetMachine(r.Context(), r.PathValue("id"))
	if err != nil || m == nil {
		writeErr(w, 404, "machine not found")
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		writeErr(w, 400, "path query parameter is required")
		return
	}

	stream, err := tgt.Machines.CopyFrom(streamCtx, &pb.CopyFromRequest{
		MachineId: m.ID, ExecutionId: m.CurrentExecutionID, Path: path,
	})
	if err != nil {
		code, msg := grpcErrStatus(err)
		writeErr(w, code, msg)
		return
	}
	var total int64
	var headerSent bool
	for {
		chunk, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			if !headerSent {
				code, msg := grpcErrStatus(rerr)
				writeErr(w, code, msg)
				return
			}
			slog.Warn("file download truncated", "machine_id", m.ID, "error", rerr)
			break
		}
		switch f := chunk.Frame.(type) {
		case *pb.CopyFromResponse_Header:
			if f.Header.Size > runtimeMaxBody {
				writeErr(w, http.StatusRequestEntityTooLarge, "file exceeds 100 MiB limit")
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.FormatUint(f.Header.Size, 10))
			w.Header().Set("X-Firepaas-File-Mode", strconv.FormatUint(uint64(f.Header.Mode), 8))
			w.Header().Set("X-Firepaas-File-Path", path)
			w.WriteHeader(200)
			headerSent = true
		case *pb.CopyFromResponse_Data:
			total += int64(len(f.Data))
			if _, werr := w.Write(f.Data); werr != nil {
				return
			}
		}
	}
	if !headerSent {
		writeErr(w, 404, "file not found in guest")
		return
	}
	slog.Info("runtime cp audit", "caller", callerName(identFrom(r)),
		"machine_id", m.ID, "execution_id", m.CurrentExecutionID,
		"direction", "download", "path_digest", runtimeDigest(path),
		"bytes", total, "result", "completed")
}

func runtimeDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}
