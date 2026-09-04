// project 子命令（v1.5：最小可用项目面 + 治理透出）。
//
//	fpctl project create --id <id> [--name <name>]
//	fpctl project ls
//	fpctl project show <id>
//	fpctl project rm <id>
//	fpctl project quota get <id>
//	fpctl project quota set <id> [--vcpu N --mem N --disk N --machine-concurrency N
//	    --runtime-session-concurrency N] [--revision R]
//	fpctl project ratelimits get <id>
//	fpctl project ratelimits set <id> [--read-rate X --read-burst X
//	    --mutation-rate X --mutation-burst X --stream-rate X --stream-burst X]
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
)

func runProject(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl project <create|ls|show|rm|quota|ratelimits>")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("project create", flag.ExitOnError)
		id := fs.String("id", "", "project id (required, ^[a-z0-9][a-z0-9-]{0,63}$)")
		name := fs.String("name", "", "display name (default: id)")
		_ = fs.Parse(args[1:])
		if *id == "" {
			return errors.New("--id is required")
		}
		if *name == "" {
			*name = *id
		}
		return do("POST", "/v1/projects", map[string]any{"id": *id, "name": *name}, nil)
	case "ls":
		_ = flag.NewFlagSet("project ls", flag.ExitOnError).Parse(args[1:])
		return do("GET", "/v1/projects", nil, nil)
	case "show":
		id, err := oneArg(args[1:], "usage: fpctl project show <id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/projects/"+url.PathEscape(id), nil, nil)
	case "rm":
		id, err := oneArg(args[1:], "usage: fpctl project rm <id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/projects/"+url.PathEscape(id), nil, nil)
	case "quota":
		return runProjectQuota(args[1:])
	case "ratelimits":
		return runProjectRateLimits(args[1:])
	default:
		return fmt.Errorf("unknown project command %q", args[0])
	}
}

func runProjectQuota(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl project quota <get|set> <project_id>")
	}
	switch args[0] {
	case "get":
		id, err := oneArg(args[1:], "usage: fpctl project quota get <project_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/projects/"+url.PathEscape(id)+"/quota", nil, nil)
	case "set":
		if len(args) < 2 {
			return errors.New("usage: fpctl project quota set <project_id> [flags]")
		}
		fs := flag.NewFlagSet("quota set", flag.ExitOnError)
		var vcpu, mem, disk, machConc, sessConc int64
		var revision int64
		fs.Int64Var(&vcpu, "vcpu", 0, "vcpu quota")
		fs.Int64Var(&mem, "mem", 0, "memory MiB quota")
		fs.Int64Var(&disk, "disk", 0, "disk MiB quota")
		fs.Int64Var(&machConc, "machine-concurrency", 0, "machine concurrency")
		fs.Int64Var(&sessConc, "runtime-session-concurrency", 0, "runtime session concurrency")
		fs.Int64Var(&revision, "revision", 0, "quota revision (0 = auto-fetch current)")
		_ = fs.Parse(args[2:])
		id := args[1]
		if revision == 0 {
			// 未显式指定则先读当前 revision（并发冲突时服务端返回 409，调用方重试）。
			var cur struct {
				Revision int64 `json:"revision"`
			}
			if err := do("GET", "/v1/projects/"+url.PathEscape(id)+"/quota", nil, &cur); err != nil {
				return err
			}
			revision = cur.Revision
		}
		body := map[string]any{
			"vcpu_quota": vcpu, "mem_mib_quota": mem, "disk_mib_quota": disk,
			"machine_concurrency": machConc, "runtime_session_concurrency": sessConc,
			"revision": revision,
		}
		return do("PUT", "/v1/projects/"+url.PathEscape(id)+"/quota", body, nil)
	default:
		return fmt.Errorf("unknown quota command %q", args[0])
	}
}

func runProjectRateLimits(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl project ratelimits <get|set> <project_id>")
	}
	switch args[0] {
	case "get":
		id, err := oneArg(args[1:], "usage: fpctl project ratelimits get <project_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/projects/"+url.PathEscape(id)+"/rate-limits", nil, nil)
	case "set":
		if len(args) < 2 {
			return errors.New("usage: fpctl project ratelimits set <project_id> [flags]")
		}
		fs := flag.NewFlagSet("ratelimits set", flag.ExitOnError)
		var rr, rb, mr, mb, sr, sb float64
		fs.Float64Var(&rr, "read-rate", 0, "read rate (0 = unlimited)")
		fs.Float64Var(&rb, "read-burst", 0, "read burst")
		fs.Float64Var(&mr, "mutation-rate", 0, "mutation rate")
		fs.Float64Var(&mb, "mutation-burst", 0, "mutation burst")
		fs.Float64Var(&sr, "stream-rate", 0, "stream rate")
		fs.Float64Var(&sb, "stream-burst", 0, "stream burst")
		_ = fs.Parse(args[2:])
		id := args[1]
		body := map[string]any{
			"read_rate": rr, "read_burst": rb,
			"mutation_rate": mr, "mutation_burst": mb,
			"stream_rate": sr, "stream_burst": sb,
		}
		return do("PUT", "/v1/projects/"+url.PathEscape(id)+"/rate-limits", body, nil)
	default:
		return fmt.Errorf("unknown ratelimits command %q", args[0])
	}
}
