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
		path := withQuery("/v1/operations", map[string]string{
			"limit":      "200",
			"machine_id": *machine,
			"kind":       *kind,
			"status":     *status,
		})
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
		if global.json {
			return nil
		}
		for _, op := range out.Operations {
			done := "-"
			if op.CompletedAt != nil {
				done = (*op.CompletedAt)[11:19]
			}
			fmt.Printf("%-44s %-10s %-9s attempts=%d %s..%s  %s\n",
				op.ID, op.Kind, op.Status, op.Attempts, op.CreatedAt[11:19], done, op.MachineID)
			if op.Error != "" && op.Status == "FAILED" {
				fmt.Printf("    error: %s\n", boundedSnippet(op.Error, 160))
			}
		}
	case "show":
		return getByID(args[1:], "usage: fpctl ops show <operation-id>", "/v1/operations")
	default:
		return fmt.Errorf("unknown ops command %q", args[0])
	}
	return nil
}
