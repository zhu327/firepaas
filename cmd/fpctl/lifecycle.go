// lifecycle 子命令（v1.5）：machines/wait/ttl 此前只有 REST。
//
//	fpctl machines ls [--project <id>]
//	fpctl machines show <machine_id>
//	fpctl machines rm <machine_id>
//	fpctl machines pause <machine_id>
//	fpctl machines resume <machine_id>
//	fpctl wait machine <machine_id> --execution <exec_id> [--timeout-ms N]
//	fpctl wait operation <operation_id> [--timeout-ms N]
//	fpctl wait rollout <rollout_id> --generation G [--timeout-ms N]
//	fpctl ttl set <machine_id> <seconds>   (0 = 关闭 TTL)
//	fpctl ttl reset-restart <machine_id>   (清零 restart attempts，需 admin)
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strconv"
)

func runMachines(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl machines <ls|show|rm|pause|resume>")
	}
	switch args[0] {
	case "ls":
		fs := flag.NewFlagSet("machines ls", flag.ExitOnError)
		project := fs.String("project", "", "filter by project id")
		_ = fs.Parse(args[1:])
		path := "/v1/machines"
		if *project != "" {
			path += "?project_id=" + url.QueryEscape(*project)
		}
		return do("GET", path, nil, nil)
	case "show":
		id, err := oneArg(args[1:], "usage: fpctl machines show <machine_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/machines/"+url.PathEscape(id), nil, nil)
	case "rm":
		id, err := oneArg(args[1:], "usage: fpctl machines rm <machine_id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/machines/"+url.PathEscape(id), nil, nil)
	case "pause":
		id, err := oneArg(args[1:], "usage: fpctl machines pause <machine_id>")
		if err != nil {
			return err
		}
		return do("POST", "/v1/machines/"+url.PathEscape(id)+"/pause", map[string]any{}, nil)
	case "resume":
		id, err := oneArg(args[1:], "usage: fpctl machines resume <machine_id>")
		if err != nil {
			return err
		}
		return do("POST", "/v1/machines/"+url.PathEscape(id)+"/resume", map[string]any{}, nil)
	default:
		return fmt.Errorf("unknown machines command %q", args[0])
	}
}

func runWait(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl wait <machine|operation|rollout>")
	}
	switch args[0] {
	case "machine":
		if len(args) < 2 {
			return errors.New("usage: fpctl wait machine <machine_id> --execution <exec_id> [--timeout-ms N]")
		}
		fs := flag.NewFlagSet("wait machine", flag.ExitOnError)
		exec := fs.String("execution", "", "execution id to wait for (required)")
		timeout := fs.Int("timeout-ms", 30000, "max wait milliseconds")
		_ = fs.Parse(args[2:])
		if *exec == "" {
			return errors.New("usage: fpctl wait machine <machine_id> --execution <exec_id> [--timeout-ms N]")
		}
		q := url.Values{}
		q.Set("execution_id", *exec)
		q.Set("timeout_ms", strconv.Itoa(*timeout))
		return do("GET", "/v1/machines/"+url.PathEscape(args[1])+"/wait?"+q.Encode(), nil, nil)
	case "operation":
		if len(args) < 2 {
			return errors.New("usage: fpctl wait operation <operation_id> [--timeout-ms N]")
		}
		fs := flag.NewFlagSet("wait operation", flag.ExitOnError)
		timeout := fs.Int("timeout-ms", 30000, "max wait milliseconds")
		_ = fs.Parse(args[2:])
		q := url.Values{}
		q.Set("timeout_ms", strconv.Itoa(*timeout))
		return do("GET", "/v1/operations/"+url.PathEscape(args[1])+"/wait?"+q.Encode(), nil, nil)
	case "rollout":
		if len(args) < 2 {
			return errors.New("usage: fpctl wait rollout <rollout_id> --generation G [--timeout-ms N]")
		}
		fs := flag.NewFlagSet("wait rollout", flag.ExitOnError)
		generation := fs.Int64("generation", 0, "rollout generation to wait for (required)")
		timeout := fs.Int("timeout-ms", 30000, "max wait milliseconds")
		_ = fs.Parse(args[2:])
		if *generation <= 0 {
			return errors.New("usage: fpctl wait rollout <rollout_id> --generation G [--timeout-ms N]")
		}
		q := url.Values{}
		q.Set("generation", strconv.FormatInt(*generation, 10))
		q.Set("timeout_ms", strconv.Itoa(*timeout))
		return do("GET", "/v1/rollouts/"+url.PathEscape(args[1])+"/wait?"+q.Encode(), nil, nil)
	default:
		return fmt.Errorf("unknown wait command %q", args[0])
	}
}

func runTTL(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl ttl <set|reset-restart>")
	}
	switch args[0] {
	case "set":
		if len(args) < 3 {
			return errors.New("usage: fpctl ttl set <machine_id> <seconds> (0 = disable)")
		}
		secs, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || secs < 0 {
			return fmt.Errorf("bad seconds %q (want >= 0)", args[2])
		}
		return do("PUT", "/v1/machines/"+url.PathEscape(args[1])+"/ttl",
			map[string]any{"ttl_seconds": secs}, nil)
	case "reset-restart":
		id, err := oneArg(args[1:], "usage: fpctl ttl reset-restart <machine_id>")
		if err != nil {
			return err
		}
		return do("POST", "/v1/machines/"+url.PathEscape(id)+"/restart-reset", map[string]any{}, nil)
	default:
		return fmt.Errorf("unknown ttl command %q", args[0])
	}
}
