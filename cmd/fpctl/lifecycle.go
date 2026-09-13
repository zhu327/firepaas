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
		project := projectFlag(fs, "")
		_ = fs.Parse(args[1:])
		return do("GET", withQuery("/v1/machines", map[string]string{"project_id": *project}), nil, nil)
	case "show":
		return getByID(args[1:], "usage: fpctl machines show <machine_id>", "/v1/machines")
	case "rm":
		return deleteByID(args[1:], "usage: fpctl machines rm <machine_id>", "/v1/machines")
	case "pause":
		return postByID(args[1:], "usage: fpctl machines pause <machine_id>", "/v1/machines", "/pause")
	case "resume":
		return postByID(args[1:], "usage: fpctl machines resume <machine_id>", "/v1/machines", "/resume")
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
		machineID, err := oneArg(args[1:], "usage: fpctl wait machine <machine_id> --execution <exec_id> [--timeout-ms N]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("wait machine", flag.ExitOnError)
		exec := fs.String("execution", "", "execution id to wait for (required)")
		timeout := fs.Int("timeout-ms", 30000, "max wait milliseconds")
		_ = fs.Parse(args[2:])
		if *exec == "" {
			return errors.New("usage: fpctl wait machine <machine_id> --execution <exec_id> [--timeout-ms N]")
		}
		return doRequest(longClient, "GET", withQuery("/v1/machines/"+url.PathEscape(machineID)+"/wait", map[string]string{
			"execution_id": *exec,
			"timeout_ms":   strconv.Itoa(*timeout),
		}), nil, nil, "", true)
	case "operation":
		opID, err := oneArg(args[1:], "usage: fpctl wait operation <operation_id> [--timeout-ms N]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("wait operation", flag.ExitOnError)
		timeout := fs.Int("timeout-ms", 30000, "max wait milliseconds")
		_ = fs.Parse(args[2:])
		return doRequest(longClient, "GET", withQuery("/v1/operations/"+url.PathEscape(opID)+"/wait", map[string]string{
			"timeout_ms": strconv.Itoa(*timeout),
		}), nil, nil, "", true)
	case "rollout":
		rolloutID, err := oneArg(args[1:], "usage: fpctl wait rollout <rollout_id> --generation G [--timeout-ms N]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("wait rollout", flag.ExitOnError)
		generation := fs.Int64("generation", 0, "rollout generation to wait for (required)")
		timeout := fs.Int("timeout-ms", 30000, "max wait milliseconds")
		_ = fs.Parse(args[2:])
		if *generation <= 0 {
			return errors.New("usage: fpctl wait rollout <rollout_id> --generation G [--timeout-ms N]")
		}
		return doRequest(longClient, "GET", withQuery("/v1/rollouts/"+url.PathEscape(rolloutID)+"/wait", map[string]string{
			"generation": strconv.FormatInt(*generation, 10),
			"timeout_ms": strconv.Itoa(*timeout),
		}), nil, nil, "", true)
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
		machineID, secsStr, err := twoArgs(args[1:], "usage: fpctl ttl set <machine_id> <seconds> (0 = disable)")
		if err != nil {
			return err
		}
		secs, err := strconv.ParseInt(secsStr, 10, 64)
		if err != nil || secs < 0 {
			return fmt.Errorf("bad seconds %q (want >= 0)", secsStr)
		}
		return do("PUT", "/v1/machines/"+url.PathEscape(machineID)+"/ttl",
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
