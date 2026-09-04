// volume 子命令（v1.5）：此前只有 REST。
//
//	fpctl volume create --name <n> --mode LOCAL_RW|DATASET_RO --size-gib <N>
//	    [--project <id>] [--node <id>] [--source-url U --content-digest D]
//	fpctl volume ls [--project <id>]
//	fpctl volume show <volume_id>
//	fpctl volume rm <volume_id>
//	fpctl volume attach <machine_id> --volume <id> --mount-path <p>
//	    [--readonly] [--overlay-size-bytes N]
//	fpctl volume detach <machine_id> --volume <id>
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
)

func runVolume(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl volume <create|ls|show|rm|attach|detach>")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("volume create", flag.ExitOnError)
		project := fs.String("project", "dev", "project id")
		name := fs.String("name", "", "volume name (required)")
		mode := fs.String("mode", "LOCAL_RW", "LOCAL_RW|DATASET_RO")
		sizeGib := fs.Int("size-gib", 0, "size GiB (required, > 0)")
		node := fs.String("node", "", "target node id")
		sourceURL := fs.String("source-url", "", "dataset source URL (DATASET_RO)")
		digest := fs.String("content-digest", "", "dataset content digest (DATASET_RO)")
		_ = fs.Parse(args[1:])
		if *name == "" || *sizeGib <= 0 {
			return errors.New("usage: fpctl volume create --name <n> --size-gib <N> [--mode M]")
		}
		body := map[string]any{
			"project_id": *project, "name": *name, "mode": *mode,
			"size_gib": *sizeGib, "node_id": *node,
			"source_url": *sourceURL, "content_digest": *digest,
		}
		return do("POST", "/v1/volumes", body, nil)
	case "ls":
		fs := flag.NewFlagSet("volume ls", flag.ExitOnError)
		project := fs.String("project", "", "filter by project id")
		_ = fs.Parse(args[1:])
		path := "/v1/volumes"
		if *project != "" {
			path += "?project_id=" + url.QueryEscape(*project)
		}
		return do("GET", path, nil, nil)
	case "show":
		id, err := oneArg(args[1:], "usage: fpctl volume show <volume_id>")
		if err != nil {
			return err
		}
		return do("GET", "/v1/volumes/"+url.PathEscape(id), nil, nil)
	case "rm":
		id, err := oneArg(args[1:], "usage: fpctl volume rm <volume_id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/volumes/"+url.PathEscape(id), nil, nil)
	case "attach":
		if len(args) < 2 {
			return errors.New("usage: fpctl volume attach <machine_id> --volume <id> --mount-path <p>")
		}
		fs := flag.NewFlagSet("volume attach", flag.ExitOnError)
		vol := fs.String("volume", "", "volume id (required)")
		mount := fs.String("mount-path", "", "guest mount path (required)")
		readonly := fs.Bool("readonly", false, "read-only attach")
		overlay := fs.Int64("overlay-size-bytes", 0, "overlay size bytes")
		_ = fs.Parse(args[2:])
		if *vol == "" || *mount == "" {
			return errors.New("usage: fpctl volume attach <machine_id> --volume <id> --mount-path <p>")
		}
		body := map[string]any{
			"mount_path": *mount, "readonly": *readonly, "overlay_size_bytes": *overlay,
		}
		return do(
			"POST",
			"/v1/machines/"+url.PathEscape(args[1])+"/volume-attach?volume_id="+url.QueryEscape(*vol),
			body,
			nil,
		)
	case "detach":
		if len(args) < 2 {
			return errors.New("usage: fpctl volume detach <machine_id> --volume <id>")
		}
		fs := flag.NewFlagSet("volume detach", flag.ExitOnError)
		vol := fs.String("volume", "", "volume id (required)")
		_ = fs.Parse(args[2:])
		if *vol == "" {
			return errors.New("usage: fpctl volume detach <machine_id> --volume <id>")
		}
		return do("POST", "/v1/machines/"+url.PathEscape(args[1])+"/volume-detach?volume_id="+url.QueryEscape(*vol),
			map[string]any{}, nil)
	default:
		return fmt.Errorf("unknown volume command %q", args[0])
	}
}
