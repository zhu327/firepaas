// Command fpctl 是 firepaas 的最小 CLI（mvp-plan §7.4：create/deploy/scale/
// status；logs/exec 明确延后）。全部操作通过 REST API，不做任何本地状态。
//
// 用法示例：
//
//	fpctl app create --hostname nginx.firepaas.local --image docker.io/library/nginx:alpine --port 80 --replicas 1
//	fpctl app status <app_id>
//	fpctl app deploy <app_id> --image docker.io/library/nginx:1.27-alpine
//	fpctl app scale <app_id> 3
//	fpctl app rollback <app_id>
//	fpctl app delete <app_id>
//
// 环境：FP_API_ADDR（默认 http://127.0.0.1:8080）、FP_API_TOKEN。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// version 由构建注入（Makefile：-ldflags "-X main.version=..."）。
var version = "dev"

const (
	defaultAPIAddr = "http://127.0.0.1:8080"
	// apiResponseHeaderTimeout 约束普通请求的连接建立/响应头等待，避免
	// API 挂起时 CLI 永久卡死。
	apiResponseHeaderTimeout = 30 * time.Second
	// longRequestHeaderTimeout 用于 logs/exec/cp 流式响应与 wait 长轮询
	// （服务端 wait 上限 5 分钟）：这些请求在首个响应头之前可能长时间
	// 无数据，不能套用 30s 头超时，否则会被误杀。
	longRequestHeaderTimeout = 6 * time.Minute
	errBodySnippetMax        = 300
)

// stdout 是数据输出目标；测试可替换。
var stdout io.Writer = os.Stdout

// apiClient / longClient 是全 CLI 共享的 HTTP 客户端，替换 http.DefaultClient
// （无超时）。两者都不设全局 Timeout，只给 Transport 设置连接/响应头超时：
// 普通调用 30s，流式/长轮询 6 分钟。
var (
	apiClient  = newHTTPClient(apiResponseHeaderTimeout)
	longClient = newHTTPClient(longRequestHeaderTimeout)
)

func newHTTPClient(responseHeaderTimeout time.Duration) *http.Client {
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}}
}

// globalOptions 保存 run() 解析出的全局 flags。
type globalOptions struct {
	addr    string
	token   string
	project string
	json    bool
}

var global globalOptions

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// apiAddr 解析 API 地址：全局 --addr > env FP_API_ADDR > 默认。
func apiAddr() string {
	if global.addr != "" {
		return strings.TrimRight(global.addr, "/")
	}
	if v := os.Getenv("FP_API_ADDR"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return defaultAPIAddr
}

// apiToken 解析 token：全局 --token > env FP_API_TOKEN。
func apiToken() string {
	if global.token != "" {
		return global.token
	}
	return os.Getenv("FP_API_TOKEN")
}

// defaultProject 解析默认 project：全局 --project > env FP_PROJECT > def。
// 直接调用子命令（测试）时 global 为零值，此时回退到 env/def。
func defaultProject(def string) string {
	if global.project != "" {
		return global.project
	}
	if v := os.Getenv("FP_PROJECT"); v != "" {
		return v
	}
	return def
}

// projectFlag 注册 --project flag，缺省值语义与原调用点一致（def 保持 "" 或 "dev"）。
func projectFlag(fs *flag.FlagSet, def string) *string {
	return fs.String("project", defaultProject(def), "project id")
}

// withQuery 拼接 query：空值省略，非空经 url.Values 编码（按键排序）。
func withQuery(base string, params map[string]string) string {
	q := url.Values{}
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	if len(q) == 0 {
		return base
	}
	return base + "?" + q.Encode()
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		var ex exitError
		if errors.As(err, &ex) {
			os.Exit(ex.code)
		}
		fmt.Fprintln(os.Stderr, "fpctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("fpctl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { printUsage(os.Stderr) }
	addr := fs.String("addr", envOr("FP_API_ADDR", defaultAPIAddr), "API 地址（env FP_API_ADDR）")
	token := fs.String("token", os.Getenv("FP_API_TOKEN"), "API token（env FP_API_TOKEN）")
	project := fs.String("project", os.Getenv("FP_PROJECT"), "默认 project id（env FP_PROJECT）")
	jsonOut := fs.Bool("json", false, "机器可读 JSON 输出")
	showVersion := fs.Bool("version", false, "打印版本并退出")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	// 只把显式传入的 flag 写入 global，未传入时让 helper 在请求时读 env，
	// 避免在进程中缓存启动时的 env 值（测试/长期进程会因此过期）。
	global = globalOptions{json: *jsonOut}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "addr":
			global.addr = *addr
		case "token":
			global.token = *token
		case "project":
			global.project = *project
		}
	})
	if *showVersion {
		fmt.Fprintln(os.Stdout, version)
		return nil
	}
	rest := fs.Args()
	if len(rest) == 0 {
		printUsage(os.Stderr)
		return nil
	}
	switch rest[0] {
	case "version":
		fmt.Fprintln(os.Stdout, version)
		return nil
	case "help":
		printUsage(os.Stdout)
		return nil
	case "app":
		return runApp(rest[1:])
	case "secrets":
		return runSecrets(rest[1:])
	case "apikey":
		return runAPIKey(rest[1:])
	case "ops":
		return runOps(rest[1:])
	case "images":
		return runImages(rest[1:])
	case "project":
		return runProject(rest[1:])
	case "nodes":
		return runNodes(rest[1:])
	case "events":
		return runEvents(rest[1:])
	case "machines":
		return runMachines(rest[1:])
	case "wait":
		return runWait(rest[1:])
	case "ttl":
		return runTTL(rest[1:])
	case "snapshot":
		return runSnapshot(rest[1:])
	case "volume":
		return runVolume(rest[1:])
	case "logs", "exec", "cp":
		return runRuntime(rest)
	default:
		return fmt.Errorf("unknown command %q (see fpctl --help)", rest[0])
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `fpctl %s — firepaas CLI

用法：
  fpctl [global flags] <command> [args]

全局 flags（须位于 command 之前）：
  --addr URL      API 地址（默认 %s，env FP_API_ADDR）
  --token TOKEN   API token（env FP_API_TOKEN）
  --project ID    默认 project id（env FP_PROJECT）
  --json          机器可读 JSON 输出
  --version       打印版本并退出
  -h, --help      打印本帮助

命令：
  version                            打印版本
  app create|list|status|deploy|scale|rollback|delete|egress-audit
  secrets set|ls|rm
  apikey create|ls|rm|rotate
  ops ls|show
  images prewarm|coverage|pin|pins|unpin
  project create|ls|show|rm|quota|ratelimits
  nodes ls|drain|ready|capabilities
  events ls|scheduler
  machines ls|show|rm|pause|resume
  wait machine|operation|rollout
  ttl set|reset-restart
  snapshot create|ls|show|rm|schedule-set|schedule-ls|schedule-rm|fork|preflight|rescue
  volume create|ls|show|rm|attach|detach
  logs|exec|cp
`, version, defaultAPIAddr)
}

// repeatable 收集可重复 flag（--secret/--env/--label/--scope/--node），统一替代
// 原 secretFlags/envFlags/stringSliceValue。KV 校验不在 Set 做，由 parseKV/
// toMap/refsJSON 在使用处报错（错误仍为 usage 错误，文案保持原 --env/--secret 前缀）。
type repeatable []string

func (f *repeatable) String() string { return strings.Join(*f, ",") }
func (f *repeatable) Set(v string) error {
	if v == "" {
		return errors.New("empty value")
	}
	*f = append(*f, v)
	return nil
}

func (f repeatable) toMap() (map[string]string, error) {
	out := make(map[string]string, len(f))
	for _, kv := range f {
		k, v, err := parseKV(kv, true)
		if err != nil {
			return nil, fmt.Errorf("bad --env pair %q (want KEY=VAL)", kv)
		}
		out[k] = v
	}
	return out, nil
}

// parseKV 解析 KEY=VAL；allowEmptyVal=false 时拒绝空值（"K=" 报错）。
func parseKV(v string, allowEmptyVal bool) (string, string, error) {
	parts := strings.SplitN(v, "=", 2)
	if len(parts) != 2 || parts[0] == "" || (!allowEmptyVal && parts[1] == "") {
		return "", "", fmt.Errorf("bad pair %q (want KEY=VAL)", v)
	}
	return parts[0], parts[1], nil
}

// refsJSON 把 VAR=NAME[@V] 列表转成 API 的 secret_refs JSON。
// 特例：VAR=（空右侧）→ {"VAR": null}，语义为移除该绑定（服务端
// validateSecretRefs 剔除 null 条目，不进新 deployment，P3-13）。
func refsJSON(specs []string) (map[string]any, error) {
	refs := map[string]any{}
	for _, spec := range specs {
		k, name, err := parseKV(spec, true)
		if err != nil {
			return nil, fmt.Errorf("bad --secret %q (want VAR=NAME[@VERSION])", spec)
		}
		if name == "" {
			refs[k] = nil
			continue
		}
		entry := map[string]any{"secret": name}
		if i := strings.IndexByte(name, '@'); i >= 0 {
			var ver int64
			if _, err := fmt.Sscanf(name[i+1:], "%d", &ver); err != nil || ver < 1 {
				return nil, fmt.Errorf("bad version in --secret %q", spec)
			}
			entry["secret"] = name[:i]
			entry["version"] = ver
		}
		refs[k] = entry
	}
	return refs, nil
}

func runSecrets(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl secrets <set|ls|rm>")
	}
	fs := flag.NewFlagSet("secrets", flag.ExitOnError)
	project := projectFlag(fs, "dev")
	switch args[0] {
	case "set":
		setfs := flag.NewFlagSet("set", flag.ExitOnError)
		var name, value string
		setfs.StringVar(&name, "name", "", "secret name (required)")
		setfs.StringVar(
			&value,
			"value",
			"",
			"secret value; omitted or '-' reads stdin (never echoed; avoids argv/ps leak)",
		)
		project = projectFlag(setfs, "dev")
		_ = setfs.Parse(args[1:])
		if name == "" {
			return errors.New("--name is required")
		}
		// P3-14：value 缺省或 '-' 从 stdin 读（避免 argv/ps/历史泄漏面）。
		if value == "" || value == "-" {
			data, rerr := io.ReadAll(os.Stdin)
			if rerr != nil {
				return fmt.Errorf("read stdin: %w", rerr)
			}
			value = strings.TrimRight(string(data), "\r\n")
		}
		if value == "" {
			return errors.New("--value is required (or pipe via stdin)")
		}
		body := map[string]any{
			"project_id": *project, "name": name, "value": value,
			"created_by": "fpctl",
		}
		out := map[string]any{}
		return do("POST", "/v1/secrets", body, out)
	case "ls":
		_ = fs.Parse(args[1:])
		return do("GET", withQuery("/v1/secrets", map[string]string{"project_id": *project}), nil, nil)
	case "rm":
		rmfs := flag.NewFlagSet("rm", flag.ExitOnError)
		project = projectFlag(rmfs, "dev")
		_ = rmfs.Parse(args[1:])
		if rmfs.NArg() < 1 {
			return errors.New("usage: fpctl secrets rm <name>")
		}
		return do(
			"DELETE",
			withQuery("/v1/secrets/"+url.PathEscape(rmfs.Arg(0)), map[string]string{"project_id": *project}),
			nil,
			nil,
		)
	default:
		return fmt.Errorf("unknown secrets command %q", args[0])
	}
}

func runApp(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl app <create|list|status|deploy|scale|rollback|delete|egress-audit>")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		var hostname, image, appID, nodePool, antiAffinity string
		var hcHTTP, hcTCP string
		var vcpu, mem, port, replicas int64
		var standbyIdle int64
		var hcInterval, hcTimeout, hcThreshold int64
		var sf repeatable
		var services serviceFlags
		var envSpecs, labelSpecs repeatable
		fs.StringVar(&hostname, "hostname", "", "hostname (required)")
		fs.StringVar(&image, "image", "", "image ref (required)")
		fs.StringVar(&appID, "app", "", "app id (default: generated)")
		project := projectFlag(fs, "")
		fs.Int64Var(&vcpu, "vcpu", 1, "vcpus")
		fs.Int64Var(&mem, "mem", 512, "memory MiB")
		fs.Int64Var(&port, "port", 8080, "ingress port")
		fs.Int64Var(&replicas, "replicas", 1, "replicas")
		fs.Int64Var(&standbyIdle, "auto-standby-idle", 0, "auto-standby idle timeout seconds (0 = disabled)")
		fs.Var(&services, "service", "service NAME=PORT (repeatable, v1.1 multi-port)")
		fs.Var(&sf, "secret", "secret binding VAR=NAME[@VERSION] (repeatable)")
		fs.Var(&envSpecs, "env", "env var KEY=VAL (repeatable)")
		fs.Var(&labelSpecs, "label", "placement label KEY=VAL (repeatable)")
		fs.StringVar(&nodePool, "node-pool", "", "target node pool")
		fs.StringVar(&antiAffinity, "anti-affinity", "", "DEPLOYMENT（副本分散到不同节点）| NONE")
		fs.StringVar(&hcHTTP, "health-check-http", "", "HTTP health check URL（与 --health-check-tcp 二选一）")
		fs.StringVar(&hcTCP, "health-check-tcp", "", "TCP health check HOST:PORT（与 --health-check-http 二选一）")
		fs.Int64Var(&hcInterval, "health-check-interval", -1, "health check interval seconds")
		fs.Int64Var(&hcTimeout, "health-check-timeout", -1, "health check timeout seconds")
		fs.Int64Var(&hcThreshold, "health-check-unhealthy-threshold", -1, "health check unhealthy threshold")
		_ = fs.Parse(args[1:])
		if hostname == "" || image == "" {
			return errors.New("--hostname and --image are required")
		}
		body := map[string]any{
			"hostname": hostname, "image": image, "vcpu": vcpu,
			"mem_mib": mem, "port": port, "replicas": replicas,
		}
		put(body, "project_id", *project)
		if len(envSpecs) > 0 {
			env, err := envSpecs.toMap()
			if err != nil {
				return err
			}
			body["env"] = env
		}
		if len(labelSpecs) > 0 {
			labels, err := labelSpecs.toMap()
			if err != nil {
				return err
			}
			body["labels"] = labels
		}
		put(body, "node_pool", nodePool)
		aa, err := normalizeAntiAffinity(antiAffinity)
		if err != nil {
			return err
		}
		put(body, "anti_affinity", aa)
		hc, err := buildHealthCheck(hcHTTP, hcTCP, hcInterval, hcTimeout, hcThreshold)
		if err != nil {
			return err
		}
		putAny(body, "health_check", hc, hc != nil)
		if len(services) > 0 {
			if port != 0 && port != 8080 && port != int64(services[0].InternalPort) {
				return fmt.Errorf("--port conflicts with first --service")
			}
			body["services"] = []serviceBody(services)
			delete(body, "port")
		}
		if standbyIdle > 0 {
			body["auto_standby"] = map[string]any{"enabled": true, "idle_timeout_seconds": standbyIdle}
		}
		put(body, "app_id", appID)
		if len(sf) > 0 {
			refs, err := refsJSON(sf)
			if err != nil {
				return err
			}
			body["secret_refs"] = refs
		}
		return do("POST", "/v1/apps", body, nil)

	case "list":
		_ = flag.NewFlagSet("list", flag.ExitOnError).Parse(args[1:])
		return do("GET", "/v1/apps", nil, nil)

	case "status":
		appID, err := oneArg(args[1:], "fpctl app status <app_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/apps/"+url.PathEscape(appID), nil, nil)

	case "deploy":
		appID, err := oneArg(args[1:], "fpctl app deploy <app_id> --image <ref>")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("deploy", flag.ExitOnError)
		var image, envSpec, strategy string
		var port int64
		var standbyIdle int64
		var services serviceFlags
		fs.StringVar(&image, "image", "", "image ref (default: inherit active deployment)")
		fs.StringVar(&envSpec, "env", "", "env vars KEY=VAL, comma-separated")
		var secretSpecs repeatable
		fs.Var(&secretSpecs, "secret", "secret binding VAR=NAME[@VERSION] (repeatable)")
		fs.Int64Var(&port, "port", 0, "ingress port (0 = inherit)")
		fs.StringVar(&strategy, "strategy", "", "rollout strategy: bluegreen (default) | rolling")
		fs.Int64Var(
			&standbyIdle,
			"auto-standby-idle",
			-1,
			"auto-standby idle timeout seconds (-1 = inherit, 0 = disable)",
		)
		fs.Var(&services, "service", "service NAME=PORT (repeatable, v1.1 multi-port)")
		_ = fs.Parse(args[2:])
		body := map[string]any{}
		if len(secretSpecs) > 0 {
			refs, err := refsJSON(secretSpecs)
			if err != nil {
				return err
			}
			body["secret_refs"] = refs
		}
		put(body, "image", image)
		putAny(body, "port", port, port != 0)
		putAny(body, "services", []serviceBody(services), len(services) > 0)
		put(body, "strategy", strategy)
		if standbyIdle >= 0 {
			if standbyIdle == 0 {
				body["auto_standby"] = map[string]any{"enabled": false}
			} else {
				body["auto_standby"] = map[string]any{"enabled": true, "idle_timeout_seconds": standbyIdle}
			}
		}
		if envSpec != "" {
			env := map[string]string{}
			for _, kv := range strings.Split(envSpec, ",") {
				k, v, err := parseKV(kv, true)
				if err != nil {
					return fmt.Errorf("bad --env pair %q (want KEY=VAL)", kv)
				}
				env[k] = v
			}
			body["env"] = env
		}
		return do("POST", "/v1/apps/"+url.PathEscape(appID)+"/deployments", body, nil)

	case "scale":
		appID, replicas, err := twoArgs(args[1:], "usage: fpctl app scale <app_id> <replicas>")
		if err != nil {
			return err
		}
		var n int
		if _, err := fmt.Sscanf(replicas, "%d", &n); err != nil {
			return fmt.Errorf("bad replicas %q", replicas)
		}
		return do("POST", "/v1/apps/"+url.PathEscape(appID)+"/scale", map[string]any{"replicas": n}, nil)

	case "rollback":
		appID, err := oneArg(args[1:], "fpctl app rollback <app_id>")
		if err != nil {
			return err
		}
		return do("POST", "/v1/apps/"+url.PathEscape(appID)+"/rollback", map[string]any{}, nil)

	case "delete":
		appID, err := oneArg(args[1:], "fpctl app delete <app_id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/apps/"+url.PathEscape(appID), nil, nil)

	case "egress-audit":
		appID, err := oneArg(args[1:], "fpctl app egress-audit <app_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/apps/"+url.PathEscape(appID)+"/egress-audit", nil, nil)

	default:
		return fmt.Errorf("unknown app command %q", args[0])
	}
}

func oneArg(args []string, usage string) (string, error) {
	if len(args) < 1 || args[0] == "" {
		return "", errors.New(usage)
	}
	return args[0], nil
}

func twoArgs(args []string, usage string) (string, string, error) {
	if len(args) < 2 || args[0] == "" || args[1] == "" {
		return "", "", errors.New(usage)
	}
	return args[0], args[1], nil
}

func getByID(args []string, usage, base string) error {
	id, err := oneArg(args, usage)
	if err != nil {
		return err
	}
	return do("GET", base+"/"+url.PathEscape(id), nil, nil)
}

func deleteByID(args []string, usage, base string) error {
	id, err := oneArg(args, usage)
	if err != nil {
		return err
	}
	return do("DELETE", base+"/"+url.PathEscape(id), nil, nil)
}

func postByID(args []string, usage, base, suffix string) error {
	id, err := oneArg(args, usage)
	if err != nil {
		return err
	}
	return do("POST", base+"/"+url.PathEscape(id)+suffix, map[string]any{}, nil)
}

// put 仅在 v 非空时设 body[k]，收敛 create/deploy 的 if x != "" 堆砌。
func put(body map[string]any, k, v string) {
	if v != "" {
		body[k] = v
	}
}

// putAny 在 ok 为真时设 body[k]，用于非字符串（map/slice/int）条件字段。
func putAny(body map[string]any, k string, v any, ok bool) {
	if ok {
		body[k] = v
	}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// do 发起普通（非流式）请求，使用 30s 响应头超时的共享客户端。
// 长轮询/内部读取直调 doRequest（longClient/emit=false），不经此 wrapper。
func do(method, path string, body, out any) error {
	return doRequest(apiClient, method, path, body, out, "", true)
}

func doRequest(client *http.Client, method, path string, body, out any, idemKey string, emit bool) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, apiAddr()+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	if token := apiToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return apiError(method, path, resp, raw)
	}
	if out != nil {
		if emit && global.json {
			printJSON(raw)
			return nil
		}
		return json.Unmarshal(raw, out)
	}
	if emit {
		printJSON(raw)
	}
	return nil
}

// apiError 构造 API 错误：保留状态码；4xx 附带 <=300 字符的响应片段；
// 5xx 不回显 body，避免把内部细节带进客户端错误。
func apiError(method, path string, resp *http.Response, raw []byte) error {
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}
	snippet := boundedSnippet(string(raw), errBodySnippetMax)
	if snippet == "" {
		return fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}
	return fmt.Errorf("%s %s: %s (%s)", method, path, resp.Status, snippet)
}

// boundedSnippet 截断到 max 字节（不切断 rune）并去除首尾空白。
func boundedSnippet(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max || max <= 0 {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// printJSON 输出紧凑 JSON（保留服务端原始 body，兼容非 JSON 文本）。
func printJSON(raw []byte) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return
	}
	_, _ = stdout.Write(trimmed)
	_, _ = io.WriteString(stdout, "\n")
}

// ---------------------------------------------------------------------------
// v1.1：多端口 services 与策略便捷参数
// ---------------------------------------------------------------------------

// serviceBody 是 deploy/create body 的 services 单条（v1.1，ADR-0022）。
type serviceBody struct {
	Name         string `json:"name"`
	InternalPort int    `json:"internal_port"`
}

// serviceFlags 实现 flag.Value：--service NAME=PORT（NAME 省略 = svc-<PORT>）。
type serviceFlags []serviceBody

func (f *serviceFlags) String() string {
	out := ""
	for i, s := range *f {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%s=%d", s.Name, s.InternalPort)
	}
	return out
}

func (f *serviceFlags) Set(v string) error {
	parts := strings.SplitN(v, "=", 2)
	if len(parts) != 2 || parts[1] == "" {
		return fmt.Errorf("bad --service %q (want NAME=PORT)", v)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("bad --service port %q", parts[1])
	}
	name := parts[0]
	if name == "" {
		name = fmt.Sprintf("svc-%d", port)
	}
	*f = append(*f, serviceBody{Name: name, InternalPort: port})
	return nil
}

// normalizeAntiAffinity 只接受服务端认识的值（api/appcommand 仅把
// "DEPLOYMENT" 映射为反亲和，其余归 NONE）；未知值本地拒绝，不发请求。
func normalizeAntiAffinity(raw string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "DEPLOYMENT":
		return "DEPLOYMENT", nil
	case "NONE":
		return "NONE", nil
	default:
		return "", fmt.Errorf("bad --anti-affinity %q (want DEPLOYMENT or NONE)", raw)
	}
}

// buildHealthCheck 把 CLI 的 http/tcp 探针参数映射为 API health_check body。
// 未指定探针时返回 nil（omit），只给了调优 flag 视为用法错误。
func buildHealthCheck(httpURL, tcpAddr string, interval, timeout, threshold int64) (map[string]any, error) {
	if httpURL != "" && tcpAddr != "" {
		return nil, errors.New("--health-check-http and --health-check-tcp are mutually exclusive")
	}
	if httpURL == "" && tcpAddr == "" {
		if interval >= 0 || timeout >= 0 || threshold >= 0 {
			return nil, errors.New("health check tuning flags require --health-check-http or --health-check-tcp")
		}
		return nil, nil
	}
	hc := map[string]any{}
	if httpURL != "" {
		target, err := normalizeHealthCheckHTTP(httpURL)
		if err != nil {
			return nil, err
		}
		hc["type"] = "http"
		hc["target"] = target
	} else {
		target, err := normalizeHealthCheckTCP(tcpAddr)
		if err != nil {
			return nil, err
		}
		hc["type"] = "tcp"
		hc["target"] = target
	}
	if interval < -1 || timeout < -1 || threshold < -1 {
		return nil, errors.New("health check tuning values must be >= 0 (-1 = unset)")
	}
	for _, f := range []struct {
		name  string
		value int64
	}{
		{"interval_seconds", interval},
		{"timeout_seconds", timeout},
		{"unhealthy_threshold", threshold},
	} {
		if f.value >= 0 {
			hc[f.name] = f.value
		}
	}
	return hc, nil
}

// normalizeHealthCheckHTTP 返回 agent 期望的 http(s)://host[:port][/path] target。
func normalizeHealthCheckHTTP(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", errors.New("--health-check-http requires a URL")
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("bad --health-check-http %q (want http(s)://host[:port][/path])", raw)
	}
	return u.String(), nil
}

// normalizeHealthCheckTCP 把 HOST:PORT 归一为 agent 期望的 tcp://HOST:PORT target。
func normalizeHealthCheckTCP(raw string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil || host == "" {
		return "", fmt.Errorf("bad --health-check-tcp %q (want HOST:PORT)", raw)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("bad --health-check-tcp port %q", port)
	}
	return "tcp://" + net.JoinHostPort(host, port), nil
}
