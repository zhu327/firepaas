// runtime.go：v1.2-C（ADR-0025）fpctl logs/exec/cp。
// CLI 只走 REST API，不直连 agent/guest。
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// runRuntime 分发 fpctl logs / exec / cp。
func runRuntime(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl logs|exec|cp ...")
	}
	switch args[0] {
	case "logs":
		return runLogs(args[1:])
	case "exec":
		return runExec(args[1:])
	case "cp":
		return runCP(args[1:])
	default:
		return fmt.Errorf("unknown runtime command %q", args[0])
	}
}

func runLogs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	follow := fs.Bool("follow", false, "follow log stream")
	tail := fs.Bool("tail", false, "start from current tail (ignore history)")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return errors.New("usage: fpctl logs <machine_id> [--follow] [--tail]")
	}
	machineID := fs.Arg(0)
	query := "follow=" + strconv.FormatBool(*follow) + "&tail=" + strconv.FormatBool(*tail)
	return doRawStream("GET", "/v1/machines/"+machineID+"/logs?"+query, nil, os.Stdout, nil)
}

type execOutputEvent struct {
	Stdout   *string `json:"stdout"`
	Stderr   *string `json:"stderr"`
	ExitCode *int32  `json:"exit_code"`
	Error    *string `json:"error"`
}

func runExec(args []string) error {
	// 分离 flags 与命令：fpctl exec <machine> [flags] -- cmd [args...]
	var machineID string
	flags := []string{}
	cmd := []string{}
	sep := -1
	for i, a := range args {
		if a == "--" {
			sep = i
			break
		}
	}
	if sep >= 0 {
		machineID = firstOr(args[:sep], "")
		flags = args[1:sep]
		cmd = args[sep+1:]
	} else {
		machineID = firstOr(args[:1], "")
		flags = args[1:]
	}
	if machineID == "" || len(cmd) == 0 {
		return errors.New("usage: fpctl exec <machine_id> [--cwd DIR] [--env K=V] [--tty] [--rows N --cols N] -- cmd [args...]")
	}
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	cwd := fs.String("cwd", "", "working directory")
	var envFlags envFlags
	fs.Var(&envFlags, "env", "env var KEY=VAL (repeatable; values may contain any characters)")
	tty := fs.Bool("tty", false, "allocate pseudo-TTY")
	rows := fs.Uint("rows", 0, "terminal rows (tty)")
	cols := fs.Uint("cols", 0, "terminal cols (tty)")
	_ = fs.Parse(flags)
	env := envFlags.toMap()
	// 非交互 stdin：管道/文件输入整体作为一帧 base64 下发（避免逐字节 RPC）。
	// 上限与 agentd gRPC MaxRecvMsgSize（16MiB）留出帧开销后对齐。
	stdinB64 := ""
	if !isTerminalInput() {
		const stdinMax = 8 << 20
		raw, rerr := io.ReadAll(io.LimitReader(os.Stdin, stdinMax+1))
		if rerr != nil {
			return fmt.Errorf("read stdin: %w", rerr)
		}
		if len(raw) > stdinMax {
			return fmt.Errorf("stdin exceeds %d MiB limit", stdinMax>>20)
		}
		if len(raw) > 0 {
			stdinB64 = base64.StdEncoding.EncodeToString(raw)
		}
	}
	body := map[string]any{"operation_id": newOperationID(), "command": cmd, "cwd": *cwd, "env": env,
		"tty": *tty, "rows": *rows, "cols": *cols, "stdin": stdinB64}
	rawBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := rawRequest("POST", "/v1/machines/"+machineID+"/exec", bytes.NewReader(rawBody), "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST exec: %s (%s)", resp.Status, strings.TrimSpace(string(raw)))
	}
	dec := json.NewDecoder(resp.Body)
	for {
		var ev execOutputEvent
		if err := dec.Decode(&ev); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode exec stream: %w", err)
		}
		switch {
		case ev.Stdout != nil:
			if raw, derr := base64.StdEncoding.DecodeString(*ev.Stdout); derr == nil {
				_, _ = os.Stdout.Write(raw)
			}
		case ev.Stderr != nil:
			if raw, derr := base64.StdEncoding.DecodeString(*ev.Stderr); derr == nil {
				_, _ = os.Stderr.Write(raw)
			}
		case ev.ExitCode != nil:
			if *ev.ExitCode != 0 {
				return exitError{code: int(*ev.ExitCode)}
			}
			return nil
		case ev.Error != nil:
			return errors.New(*ev.Error)
		}
	}
	return errors.New("exec stream ended without exit code")
}

// exitError 让 main 以非零码退出且不打 "fpctl:" 前缀（exec 语义）。
type exitError struct{ code int }

func (e exitError) Error() string { return fmt.Sprintf("exit status %d", e.code) }

func runCP(args []string) error {
	if len(args) < 4 {
		return errors.New("usage: fpctl cp <machine_id> up <local_file> <guest_path> [operation_id] | fpctl cp <machine_id> down <guest_path> <local_file>")
	}
	machineID := args[0]
	switch args[1] {
	case "up":
		if len(args) > 5 {
			return errors.New("usage: fpctl cp <machine_id> up <local_file> <guest_path> [operation_id]")
		}
		local, remote := args[2], args[3]
		f, err := os.Open(local)
		if err != nil {
			return err
		}
		defer f.Close()
		operationID := newOperationID()
		if len(args) == 5 {
			operationID = args[4]
		}
		resp, err := rawRequest("PUT", "/v1/machines/"+machineID+"/files?path="+urlQuery(remote)+"&operation_id="+urlQuery(operationID), f, "application/octet-stream")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 300 {
			return fmt.Errorf("PUT files: %s (%s)", resp.Status, strings.TrimSpace(string(raw)))
		}
		fmt.Println(string(raw))
		return nil
	case "down":
		if len(args) != 4 {
			return errors.New("usage: fpctl cp <machine_id> down <guest_path> <local_file>")
		}
		remote, local := args[2], args[3]
		resp, err := rawRequest("GET", "/v1/machines/"+machineID+"/files?path="+urlQuery(remote), nil, "")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			raw, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("GET files: %s (%s)", resp.Status, strings.TrimSpace(string(raw)))
		}
		return downloadAtomically(resp, local)
	default:
		return fmt.Errorf("unknown cp direction %q (want up|down)", args[1])
	}
}

// ---------------------------------------------------------------------------
// 原始 HTTP 辅助（流式；不复用 do 的整包 JSON 语义）
// ---------------------------------------------------------------------------

func apiBase() string {
	addr := os.Getenv("FP_API_ADDR")
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	return strings.TrimRight(addr, "/")
}

func rawRequest(method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequest(method, apiBase()+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token := os.Getenv("FP_API_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return http.DefaultClient.Do(req)
}

func doRawStream(method, path string, body io.Reader, w io.Writer, headers map[string]string) error {
	resp, err := rawRequest(method, path, body, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: %s (%s)", method, path, resp.Status, strings.TrimSpace(string(raw)))
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

func downloadAtomically(resp *http.Response, local string) error {
	if resp.ContentLength > runtimeMaxDownload {
		return fmt.Errorf("download exceeds %d MiB limit", runtimeMaxDownload>>20)
	}
	dir := filepath.Dir(local)
	tmp, err := os.CreateTemp(dir, ".fpctl-download-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	n, copyErr := io.Copy(tmp, io.LimitReader(resp.Body, runtimeMaxDownload+1))
	closeErr := tmp.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if n > runtimeMaxDownload || (resp.ContentLength >= 0 && n != resp.ContentLength) {
		return fmt.Errorf("download truncated or exceeds limit (received %d bytes)", n)
	}
	if err := os.Rename(tmpName, local); err != nil {
		return err
	}
	fmt.Printf("wrote %d bytes to %s\n", n, local)
	return nil
}

const runtimeMaxDownload int64 = 100 << 20

func newOperationID() string {
	return fmt.Sprintf("exec-%d-%d", time.Now().UnixNano(), os.Getpid())
}

func firstOr(list []string, def string) string {
	if len(list) > 0 {
		return list[0]
	}
	return def
}

func urlQuery(v string) string {
	// 用标准库完整转义（&/+/%/空格等都会破坏 query 语义）；路径合法性
	// 由 API/agent 的白名单校验负责。
	return url.QueryEscape(v)
}

// envFlags 实现 flag.Value：--env KEY=VAL（可重复；值可含任意字符，含逗号）。
type envFlags []string

func (f *envFlags) String() string { return strings.Join(*f, ",") }

func (f *envFlags) Set(v string) error {
	parts := strings.SplitN(v, "=", 2)
	if len(parts) != 2 || parts[0] == "" {
		return fmt.Errorf("bad --env pair %q (want KEY=VAL)", v)
	}
	*f = append(*f, v)
	return nil
}

func (f envFlags) toMap() map[string]string {
	out := make(map[string]string, len(f))
	for _, kv := range f {
		parts := strings.SplitN(kv, "=", 2)
		out[parts[0]] = parts[1]
	}
	return out
}

// isTerminalInput 保守判断 stdin 是否为终端（不可判定时视为管道）。
func isTerminalInput() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
