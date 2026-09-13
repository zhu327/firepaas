// nodes/events 子命令（v1.5：此前只有 REST）。
//
//	fpctl nodes ls
//	fpctl nodes drain <node_id> [--evacuate]
//	fpctl nodes ready <node_id>
//	fpctl nodes capabilities
//	fpctl events ls --project <id> [--app <a> --machine <m> --type <t> --limit N]
//	fpctl events scheduler [--limit N] [--project <id>]
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
)

func runNodes(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl nodes <ls|drain|ready|capabilities>")
	}
	switch args[0] {
	case "ls":
		_ = flag.NewFlagSet("nodes ls", flag.ExitOnError).Parse(args[1:])
		return do("GET", "/v1/nodes", nil, nil)
	case "drain":
		nodeID, err := oneArg(args[1:], "usage: fpctl nodes drain <node_id> [--evacuate]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("nodes drain", flag.ExitOnError)
		evacuate := fs.Bool("evacuate", false, "evacuate存量 machine（换代重建到其它节点）")
		_ = fs.Parse(args[2:])
		return do("POST", "/v1/nodes/"+url.PathEscape(nodeID)+"/drain",
			map[string]any{"evacuate": *evacuate}, nil)
	case "ready":
		id, err := oneArg(args[1:], "usage: fpctl nodes ready <node_id>")
		if err != nil {
			return err
		}
		return do("POST", "/v1/nodes/"+url.PathEscape(id)+"/ready", map[string]any{}, nil)
	case "capabilities":
		_ = flag.NewFlagSet("nodes capabilities", flag.ExitOnError).Parse(args[1:])
		return do("GET", "/v1/capabilities", nil, nil)
	default:
		return fmt.Errorf("unknown nodes command %q", args[0])
	}
}

func runEvents(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl events <ls|scheduler>")
	}
	switch args[0] {
	case "ls":
		fs := flag.NewFlagSet("events ls", flag.ExitOnError)
		project := projectFlag(fs, "")
		app := fs.String("app", "", "filter by app id")
		machine := fs.String("machine", "", "filter by machine id")
		typ := fs.String("type", "", "filter by event type")
		limit := fs.Int("limit", 200, "max events (1-1000)")
		_ = fs.Parse(args[1:])
		return do("GET", withQuery("/v1/events", map[string]string{
			"project_id": *project,
			"app_id":     *app,
			"machine_id": *machine,
			"type":       *typ,
			"limit":      fmt.Sprint(*limit),
		}), nil, nil)
	case "scheduler":
		fs := flag.NewFlagSet("events scheduler", flag.ExitOnError)
		limit := fs.Int("limit", 200, "max events")
		project := projectFlag(fs, "")
		_ = fs.Parse(args[1:])
		return do("GET", withQuery("/v1/system/scheduler-events", map[string]string{
			"limit":      fmt.Sprint(*limit),
			"project_id": *project,
		}), nil, nil)
	default:
		return fmt.Errorf("unknown events command %q", args[0])
	}
}
