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
		if len(args) < 2 {
			return errors.New("usage: fpctl snapshot create <machine_id> [flags]")
		}
		fs := flag.NewFlagSet("snapshot create", flag.ExitOnError)
		kind := fs.String("kind", "memory", "memory|filesystem")
		name := fs.String("name", "", "snapshot name")
		compression := fs.String("compression", "none", "none|zstd|lz4")
		level := fs.Int("compression-level", -1, "compression level (-1 = default)")
		retention := fs.String("retention-class", "", "retention class")
		idem := idemKeyFlag(fs)
		_ = fs.Parse(args[2:])
		body := map[string]any{"kind": *kind, "name": *name, "compression": *compression, "retention_class": *retention}
		if *level >= 0 {
			body["compression_level"] = *level
		}
		return doIdem("POST", "/v1/machines/"+url.PathEscape(args[1])+"/snapshots", body, nil, resolveIdemKey(*idem))
	case "ls":
		fs := flag.NewFlagSet("snapshot ls", flag.ExitOnError)
		project := fs.String("project", "", "filter by project id")
		_ = fs.Parse(args[1:])
		path := "/v1/snapshots"
		if *project != "" {
			path += "?project_id=" + url.QueryEscape(*project)
		}
		return do("GET", path, nil, nil)
	case "show":
		id, err := oneArg(args[1:], "usage: fpctl snapshot show <snapshot_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/snapshots/"+url.PathEscape(id), nil, nil)
	case "rm":
		id, err := oneArg(args[1:], "usage: fpctl snapshot rm <snapshot_id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/snapshots/"+url.PathEscape(id), nil, nil)
	case "schedule-set":
		if len(args) < 2 {
			return errors.New("usage: fpctl snapshot schedule-set <machine_id> --interval <sec>")
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
		return do("POST", "/v1/machines/"+url.PathEscape(args[1])+"/snapshot-schedules", body, nil)
	case "schedule-ls":
		id, err := oneArg(args[1:], "usage: fpctl snapshot schedule-ls <machine_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/machines/"+url.PathEscape(id)+"/snapshot-schedules", nil, nil)
	case "schedule-rm":
		if len(args) < 3 {
			return errors.New("usage: fpctl snapshot schedule-rm <machine_id> <schedule_id>")
		}
		return do(
			"DELETE",
			"/v1/machines/"+url.PathEscape(args[1])+"/snapshot-schedules/"+url.PathEscape(args[2]),
			nil,
			nil,
		)
	case "fork":
		if len(args) < 2 {
			return errors.New("usage: fpctl snapshot fork <snapshot_id> --app <app_id> --ttl <sec>")
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
		return doIdem("POST", "/v1/snapshots/"+url.PathEscape(args[1])+"/fork", body, nil, resolveIdemKey(*idem))
	case "preflight":
		if len(args) < 2 {
			return errors.New("usage: fpctl snapshot preflight <snapshot_id> [--restore-mode M]")
		}
		fs := flag.NewFlagSet("snapshot preflight", flag.ExitOnError)
		mode := fs.String("restore-mode", "", "memory|filesystem|auto (default auto)")
		_ = fs.Parse(args[2:])
		return do("POST", "/v1/snapshots/"+url.PathEscape(args[1])+"/preflight",
			map[string]any{"restore_mode": *mode}, nil)
	case "rescue":
		if len(args) < 2 {
			return errors.New("usage: fpctl snapshot rescue <machine_id> --snapshot <snap_id>")
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
		return doIdem("POST", "/v1/machines/"+url.PathEscape(args[1])+"/rescue", body, nil, resolveIdemKey(*idem))
	default:
		return fmt.Errorf("unknown snapshot command %q", args[0])
	}
}
