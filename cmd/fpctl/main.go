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
	default:
		return fmt.Errorf("unknown command %q", args[0])
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
		fs.StringVar(&hostname, "hostname", "", "hostname (required)")
		fs.StringVar(&image, "image", "", "image ref (required)")
		fs.StringVar(&appID, "app", "", "app id (default: generated)")
		fs.Int64Var(&vcpu, "vcpu", 1, "vcpus")
		fs.Int64Var(&mem, "mem", 512, "memory MiB")
		fs.Int64Var(&port, "port", 8080, "ingress port")
		fs.Int64Var(&replicas, "replicas", 1, "replicas")
		_ = fs.Parse(args[1:])
		if hostname == "" || image == "" {
			return errors.New("--hostname and --image are required")
		}
		body := map[string]any{"hostname": hostname, "image": image, "vcpu": vcpu,
			"mem_mib": mem, "port": port, "replicas": replicas}
		if appID != "" {
			body["app_id"] = appID
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
		var image string
		fs.StringVar(&image, "image", "", "image ref (default: inherit active deployment)")
		_ = fs.Parse(args[2:])
		body := map[string]any{}
		if image != "" {
			body["image"] = image
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
