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
		project := projectFlag(fs, "dev")
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
		project := projectFlag(fs, "")
		_ = fs.Parse(args[1:])
		return do("GET", withQuery("/v1/volumes", map[string]string{"project_id": *project}), nil, nil)
	case "show":
		return getByID(args[1:], "usage: fpctl volume show <volume_id>", "/v1/volumes")
	case "rm":
		return deleteByID(args[1:], "usage: fpctl volume rm <volume_id>", "/v1/volumes")
	case "attach":
		machineID, err := oneArg(args[1:], "usage: fpctl volume attach <machine_id> --volume <id> --mount-path <p>")
		if err != nil {
			return err
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
			withQuery("/v1/machines/"+url.PathEscape(machineID)+"/volume-attach", map[string]string{"volume_id": *vol}),
			body,
			nil,
		)
	case "detach":
		machineID, err := oneArg(args[1:], "usage: fpctl volume detach <machine_id> --volume <id>")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("volume detach", flag.ExitOnError)
		vol := fs.String("volume", "", "volume id (required)")
		_ = fs.Parse(args[2:])
		if *vol == "" {
			return errors.New("usage: fpctl volume detach <machine_id> --volume <id>")
		}
		return do("POST", withQuery("/v1/machines/"+url.PathEscape(machineID)+"/volume-detach", map[string]string{"volume_id": *vol}),
			map[string]any{}, nil)
	default:
		return fmt.Errorf("unknown volume command %q", args[0])
	}
}
