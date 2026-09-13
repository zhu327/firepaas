// snapshot/volume 子命令（v1.5）：此前只有 REST。
//
//	fpctl snapshot create <machine_id> [--kind memory|filesystem] [--name N]
//	    [--compression none|zstd|lz4] [--compression-level L] [--retention-class C]
//	    [--idempotency-key K]
//	fpctl snapshot ls [--project <id>]
//	fpctl snapshot show <snapshot_id>
//	fpctl snapshot rm <snapshot_id>
//	fpctl snapshot schedule-set <machine_id> --interval <sec> [--jitter <sec>]
//	    [--max-count N] [--max-age <sec>] [--compression C] [--enable/--disable]
//	fpctl snapshot schedule-ls <machine_id>
//	fpctl snapshot schedule-rm <machine_id> <schedule_id>
//	fpctl snapshot fork <snapshot_id> --app <app_id> --ttl <sec> [--restore-mode M]
//	    [--idempotency-key K]
//	fpctl snapshot preflight <snapshot_id> [--restore-mode M]
//	fpctl snapshot rescue <machine_id> --snapshot <snap_id> [--restore-mode M]
//	    [--idempotency-key K]
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
)

func runSnapshot(args []string) error {
	if len(args) < 1 {
		return errors.New(
			"usage: fpctl snapshot <create|ls|show|rm|schedule-set|schedule-ls|schedule-rm|fork|preflight|rescue>",
		)
	}
	switch args[0] {
	case "create":
		machineID, err := oneArg(args[1:], "usage: fpctl snapshot create <machine_id> [flags]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("snapshot create", flag.ExitOnError)
		kind := fs.String("kind", "memory", "memory|filesystem")
		name := fs.String("name", "", "snapshot name")
		compression := fs.String("compression", "none", "none|zstd|lz4")
		level := fs.Int("compression-level", -1, "compression level (-1 = default)")
		retention := fs.String("retention-class", "", "retention class")
		idem := idemKeyFlag(fs)
		_ = fs.Parse(args[2:])
		body := map[string]any{"kind": *kind, "compression": *compression}
		put(body, "name", *name)
		put(body, "retention_class", *retention)
		putAny(body, "compression_level", *level, *level >= 0)
		return doRequest(apiClient, "POST", "/v1/machines/"+url.PathEscape(machineID)+"/snapshots", body, nil, resolveIdemKey(*idem), true)
	case "ls":
		fs := flag.NewFlagSet("snapshot ls", flag.ExitOnError)
		project := projectFlag(fs, "")
		_ = fs.Parse(args[1:])
		return do("GET", withQuery("/v1/snapshots", map[string]string{"project_id": *project}), nil, nil)
	case "show":
		return getByID(args[1:], "usage: fpctl snapshot show <snapshot_id>", "/v1/snapshots")
	case "rm":
		return deleteByID(args[1:], "usage: fpctl snapshot rm <snapshot_id>", "/v1/snapshots")
	case "schedule-set":
		machineID, err := oneArg(args[1:], "usage: fpctl snapshot schedule-set <machine_id> --interval <sec>")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("snapshot schedule-set", flag.ExitOnError)
		interval := fs.Int("interval", 3600, "interval seconds (>= 60)")
		jitter := fs.Int("jitter", 0, "jitter seconds")
		maxCount := fs.Int("max-count", 10, "max retained snapshots")
		maxAge := fs.Int("max-age", 0, "max age seconds (0 = unlimited)")
		compression := fs.String("compression", "none", "none|zstd|lz4")
		disable := fs.Bool("disable", false, "disable schedule (default enable)")
		_ = fs.Parse(args[2:])
		enabled := !*disable
		body := map[string]any{
			"interval_seconds": *interval, "jitter_seconds": *jitter,
			"max_count": *maxCount, "max_age_seconds": *maxAge,
			"compression": *compression, "enabled": enabled,
		}
		return do("POST", "/v1/machines/"+url.PathEscape(machineID)+"/snapshot-schedules", body, nil)
	case "schedule-ls":
		id, err := oneArg(args[1:], "usage: fpctl snapshot schedule-ls <machine_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/machines/"+url.PathEscape(id)+"/snapshot-schedules", nil, nil)
	case "schedule-rm":
		machineID, scheduleID, err := twoArgs(args[1:], "usage: fpctl snapshot schedule-rm <machine_id> <schedule_id>")
		if err != nil {
			return err
		}
		return do(
			"DELETE",
			"/v1/machines/"+url.PathEscape(machineID)+"/snapshot-schedules/"+url.PathEscape(scheduleID),
			nil,
			nil,
		)
	case "fork":
		snapID, err := oneArg(args[1:], "usage: fpctl snapshot fork <snapshot_id> --app <app_id> --ttl <sec>")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("snapshot fork", flag.ExitOnError)
		app := fs.String("app", "", "host app id (required, same project)")
		ttl := fs.Int64("ttl", 0, "debug machine TTL seconds (required, > 0)")
		mode := fs.String("restore-mode", "", "memory|filesystem|auto")
		idem := idemKeyFlag(fs)
		_ = fs.Parse(args[2:])
		if *app == "" || *ttl <= 0 {
			return errors.New("usage: fpctl snapshot fork <snapshot_id> --app <app_id> --ttl <sec>")
		}
		body := map[string]any{"app_id": *app, "ttl_seconds": *ttl, "restore_mode": *mode}
		return doRequest(apiClient, "POST", "/v1/snapshots/"+url.PathEscape(snapID)+"/fork", body, nil, resolveIdemKey(*idem), true)
	case "preflight":
		snapID, err := oneArg(args[1:], "usage: fpctl snapshot preflight <snapshot_id> [--restore-mode M]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("snapshot preflight", flag.ExitOnError)
		mode := fs.String("restore-mode", "", "memory|filesystem|auto (default auto)")
		_ = fs.Parse(args[2:])
		return do("POST", "/v1/snapshots/"+url.PathEscape(snapID)+"/preflight",
			map[string]any{"restore_mode": *mode}, nil)
	case "rescue":
		machineID, err := oneArg(args[1:], "usage: fpctl snapshot rescue <machine_id> --snapshot <snap_id>")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("snapshot rescue", flag.ExitOnError)
		snap := fs.String("snapshot", "", "snapshot id (required)")
		mode := fs.String("restore-mode", "", "memory|filesystem|auto")
		idem := idemKeyFlag(fs)
		_ = fs.Parse(args[2:])
		if *snap == "" {
			return errors.New("usage: fpctl snapshot rescue <machine_id> --snapshot <snap_id>")
		}
		body := map[string]any{"snapshot_id": *snap, "restore_mode": *mode}
		return doRequest(apiClient, "POST", "/v1/machines/"+url.PathEscape(machineID)+"/rescue", body, nil, resolveIdemKey(*idem), true)
	default:
		return fmt.Errorf("unknown snapshot command %q", args[0])
	}
}
