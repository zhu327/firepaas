// ops 子命令（M5.3：fpctl ops ls/show —— operation trace）。
package main

import (
	"errors"
	"flag"
	"fmt"
)

func runOps(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: fpctl ops <ls|show> [--machine <id> --kind <k> --status <s>]")
	}
	switch args[0] {
	case "ls":
		fs := flag.NewFlagSet("ops ls", flag.ExitOnError)
		machine := fs.String("machine", "", "按 machine_id 过滤")
		kind := fs.String("kind", "", "按 kind 过滤（create/delete/pause/resume/reap）")
		status := fs.String("status", "", "按 status 过滤（PENDING/CLAIMED/SUCCEEDED/FAILED）")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		path := "/v1/operations?limit=200"
		if *machine != "" {
			path += "&machine_id=" + *machine
		}
		if *kind != "" {
			path += "&kind=" + *kind
		}
		if *status != "" {
			path += "&status=" + *status
		}
		var out struct {
			Operations []struct {
				ID          string  `json:"id"`
				MachineID   string  `json:"machine_id"`
				Kind        string  `json:"kind"`
				Status      string  `json:"status"`
				Attempts    int     `json:"attempts"`
				Error       string  `json:"error"`
				CreatedAt   string  `json:"created_at"`
				CompletedAt *string `json:"completed_at"`
			} `json:"operations"`
		}
		if err := do("GET", path, nil, &out); err != nil {
			return err
		}
		for _, op := range out.Operations {
			done := "-"
			if op.CompletedAt != nil {
				done = (*op.CompletedAt)[11:19]
			}
			fmt.Printf("%-44s %-10s %-9s attempts=%d %s..%s  %s\n",
				op.ID, op.Kind, op.Status, op.Attempts, op.CreatedAt[11:19], done, op.MachineID)
			if op.Error != "" && op.Status == "FAILED" {
				fmt.Printf("    error: %s\n", truncate(op.Error, 160))
			}
		}
	case "show":
		id, err := oneArg(args[1:], "usage: fpctl ops show <operation-id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/operations/"+id, nil, nil)
	default:
		return fmt.Errorf("unknown ops command %q", args[0])
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
