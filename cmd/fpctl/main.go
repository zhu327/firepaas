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
	"net/http"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fpctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl app <create|list|status|deploy|scale|rollback|delete> ...")
	}
	switch args[0] {
	case "app":
		return runApp(args[1:])
	case "secrets":
		return runSecrets(args[1:])
	case "apikey":
		return runAPIKey(args[1:])
	case "ops":
		return runOps(args[1:])
	default:
		return fmt.Errorf("unknown command %q (see fpctl secrets / fpctl app)", args[0])
	}
}

// secretFlags 收集 --secret VAR=NAME[@VERSION]（可重复）。
type secretFlags []string

func (f *secretFlags) String() string { return strings.Join(*f, ",") }
func (f *secretFlags) Set(v string) error {
	if v == "" {
		return errors.New("empty --secret binding")
	}
	*f = append(*f, v)
	return nil
}

// refsJSON 把 VAR=NAME[@V] 列表转成 API 的 secret_refs JSON。
// 特例：VAR=（空右侧）→ {"VAR": null}，语义为移除该绑定（服务端
// validateSecretRefs 剔除 null 条目，不进新 deployment，P3-13）。
func refsJSON(specs []string) (map[string]any, error) {
	refs := map[string]any{}
	for _, spec := range specs {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("bad --secret %q (want VAR=NAME[@VERSION])", spec)
		}
		name := parts[1]
		if name == "" {
			refs[parts[0]] = nil
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
		refs[parts[0]] = entry
	}
	return refs, nil
}

func runSecrets(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl secrets <set|ls|rm> ...")
	}
	fs := flag.NewFlagSet("secrets", flag.ExitOnError)
	project := fs.String("project", "dev", "project id")
	switch args[0] {
	case "set":
		setfs := flag.NewFlagSet("set", flag.ExitOnError)
		var name, value string
		setfs.StringVar(&name, "name", "", "secret name (required)")
		setfs.StringVar(&value, "value", "", "secret value; omitted or '-' reads stdin (never echoed; avoids argv/ps leak)")
		setfs.StringVar(project, "project", "dev", "project id")
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
		body := map[string]any{"project_id": *project, "name": name, "value": value,
			"created_by": "fpctl"}
		out := map[string]any{}
		return do("POST", "/v1/secrets", body, out)
	case "ls":
		_ = fs.Parse(args[1:])
		return do("GET", "/v1/secrets?project_id="+*project, nil, nil)
	case "rm":
		rmfs := flag.NewFlagSet("rm", flag.ExitOnError)
		rmfs.StringVar(project, "project", "dev", "project id")
		_ = rmfs.Parse(args[1:])
		if rmfs.NArg() < 1 {
			return errors.New("usage: fpctl secrets rm <name>")
		}
		return do("DELETE", "/v1/secrets/"+rmfs.Arg(0)+"?project_id="+*project, nil, nil)
	default:
		return fmt.Errorf("unknown secrets command %q", args[0])
	}
}

func runApp(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl app <create|list|status|deploy|scale|rollback|delete> ...")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		var hostname, image, appID string
		var vcpu, mem, port, replicas int64
		var sf secretFlags
		var secretList *secretFlags
		fs.StringVar(&hostname, "hostname", "", "hostname (required)")
		fs.StringVar(&image, "image", "", "image ref (required)")
		fs.StringVar(&appID, "app", "", "app id (default: generated)")
		fs.Int64Var(&vcpu, "vcpu", 1, "vcpus")
		fs.Int64Var(&mem, "mem", 512, "memory MiB")
		fs.Int64Var(&port, "port", 8080, "ingress port")
		fs.Int64Var(&replicas, "replicas", 1, "replicas")
		secretList = &sf
		fs.Var(&sf, "secret", "secret binding VAR=NAME[@VERSION] (repeatable)")
		_ = fs.Parse(args[1:])
		if hostname == "" || image == "" {
			return errors.New("--hostname and --image are required")
		}
		body := map[string]any{"hostname": hostname, "image": image, "vcpu": vcpu,
			"mem_mib": mem, "port": port, "replicas": replicas}
		if appID != "" {
			body["app_id"] = appID
		}
		if secretList != nil && len(*secretList) > 0 {
			refs, err := refsJSON(*secretList)
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
		return do("GET", "/v1/apps/"+appID, nil, nil)

	case "deploy":
		appID, err := oneArg(args[1:], "fpctl app deploy <app_id> --image <ref>")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("deploy", flag.ExitOnError)
		var image, envSpec string
		var port int64
		fs.StringVar(&image, "image", "", "image ref (default: inherit active deployment)")
		fs.StringVar(&envSpec, "env", "", "env vars KEY=VAL, comma-separated")
		var secretSpecs secretFlags
		fs.Var(&secretSpecs, "secret", "secret binding VAR=NAME[@VERSION] (repeatable)")
		fs.Int64Var(&port, "port", 0, "ingress port (0 = inherit)")
		_ = fs.Parse(args[2:])
		body := map[string]any{}
		if len(secretSpecs) > 0 {
			refs, err := refsJSON(secretSpecs)
			if err != nil {
				return err
			}
			body["secret_refs"] = refs
		}
		if image != "" {
			body["image"] = image
		}
		if port != 0 {
			body["port"] = port
		}
		if envSpec != "" {
			env := map[string]string{}
			for _, kv := range strings.Split(envSpec, ",") {
				parts := strings.SplitN(kv, "=", 2)
				if len(parts) != 2 || parts[0] == "" {
					return fmt.Errorf("bad --env pair %q (want KEY=VAL)", kv)
				}
				env[parts[0]] = parts[1]
			}
			body["env"] = env
		}
		return do("POST", "/v1/apps/"+appID+"/deployments", body, nil)

	case "scale":
		if len(args) < 3 {
			return errors.New("usage: fpctl app scale <app_id> <replicas>")
		}
		var n int
		if _, err := fmt.Sscanf(args[2], "%d", &n); err != nil {
			return fmt.Errorf("bad replicas %q", args[2])
		}
		return do("POST", "/v1/apps/"+args[1]+"/scale", map[string]any{"replicas": n}, nil)

	case "rollback":
		appID, err := oneArg(args[1:], "fpctl app rollback <app_id>")
		if err != nil {
			return err
		}
		return do("POST", "/v1/apps/"+appID+"/rollback", map[string]any{}, nil)

	case "delete":
		appID, err := oneArg(args[1:], "fpctl app delete <app_id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/apps/"+appID, nil, nil)

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

func do(method, path string, body, out any) error {
	addr := os.Getenv("FP_API_ADDR")
	if addr == "" {
		addr = "http://127.0.0.1:8080"
	}
	token := os.Getenv("FP_API_TOKEN")
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, strings.TrimRight(addr, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s (%s)", method, path, resp.Status, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	fmt.Println(string(raw))
	return nil
}
