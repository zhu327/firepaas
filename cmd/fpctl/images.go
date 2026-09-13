package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
)

// runImages 实现 v1.4-C（docs/v1.4-plan.md §7）的镜像预热/覆盖率/pin CLI。
//
//	fpctl images prewarm --image registry/app@sha256:... [--node-pool p] [--node id ...]
//	fpctl images coverage --image registry/app@sha256:... [--node-pool p]
//	fpctl images pin    --image registry/app@sha256:... [--node-pool p] --ttl 3600 --reason ...
//	fpctl images pins
//	fpctl images unpin <pin_id>
func runImages(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: fpctl images <prewarm|coverage|pin|pins|unpin>")
	}
	switch args[0] {
	case "prewarm":
		return runImagesPrewarm(args[1:])
	case "coverage":
		return runImagesCoverage(args[1:])
	case "pin":
		return runImagesPin(args[1:])
	case "pins":
		return runImagesPins(args[1:])
	case "unpin":
		return runImagesUnpin(args[1:])
	default:
		return fmt.Errorf("unknown images command %q", args[0])
	}
}

func nodeTargetFlags(fs *flag.FlagSet, nodePool *string, nodeIDs *[]string) {
	fs.StringVar(nodePool, "node-pool", "", "target node pool")
	fs.Var((*repeatable)(nodeIDs), "node", "target node id (repeatable)")
}

func runImagesPrewarm(args []string) error {
	fs := flag.NewFlagSet("images prewarm", flag.ExitOnError)
	project := projectFlag(fs, "dev")
	image := fs.String("image", "", "digest-pinned image ref (registry/app@sha256:...)")
	var nodePool string
	var nodeIDs []string
	nodeTargetFlags(fs, &nodePool, &nodeIDs)
	idem := idemKeyFlag(fs)
	_ = fs.Parse(args)
	if *image == "" || (nodePool == "" && len(nodeIDs) == 0) {
		return errors.New(
			"usage: fpctl images prewarm --image <registry/app@sha256:...> [--node-pool p | --node id ...] [--project dev]",
		)
	}
	body := map[string]any{"project_id": *project, "image_ref": *image}
	put(body, "node_pool", nodePool)
	putAny(body, "node_ids", nodeIDs, len(nodeIDs) > 0)
	return doRequest(apiClient, "POST", "/v1/images/prewarm", body, nil, resolveIdemKey(*idem), true)
}

func runImagesCoverage(args []string) error {
	fs := flag.NewFlagSet("images coverage", flag.ExitOnError)
	image := fs.String("image", "", "digest-pinned image ref")
	digest := fs.String("digest", "", "bare image digest")
	nodePool := fs.String("node-pool", "", "filter by node pool")
	_ = fs.Parse(args)
	if *image == "" && *digest == "" {
		return errors.New("usage: fpctl images coverage --image <ref> | --digest sha256:... [--node-pool p]")
	}
	return do("GET", withQuery("/v1/images/coverage", map[string]string{
		"image_ref": *image,
		"digest":    *digest,
		"node_pool": *nodePool,
	}), nil, nil)
}

func runImagesPins(args []string) error {
	fs := flag.NewFlagSet("images pins", flag.ExitOnError)
	project := projectFlag(fs, "")
	_ = fs.Parse(args)
	return do("GET", withQuery("/v1/images/pins", map[string]string{"project_id": *project}), nil, nil)
}

func runImagesUnpin(args []string) error {
	pinID, err := oneArg(args, "usage: fpctl images unpin <pin_id> [--idempotency-key K]")
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("images unpin", flag.ExitOnError)
	idem := idemKeyFlag(fs)
	_ = fs.Parse(args[1:])
	return doRequest(apiClient, "DELETE", "/v1/images/pins/"+url.PathEscape(pinID), nil, nil, resolveIdemKey(*idem), true)
}

func runImagesPin(args []string) error {
	fs := flag.NewFlagSet("images pin", flag.ExitOnError)
	project := projectFlag(fs, "dev")
	image := fs.String("image", "", "digest-pinned image ref")
	ttl := fs.Int64("ttl", 3600, "pin TTL seconds")
	reason := fs.String("reason", "", "pin reason (audited)")
	var nodePool string
	var nodeIDs []string
	nodeTargetFlags(fs, &nodePool, &nodeIDs)
	idem := idemKeyFlag(fs)
	_ = fs.Parse(args)
	if *image == "" || (nodePool == "" && len(nodeIDs) == 0) {
		return errors.New(
			"usage: fpctl images pin --image <registry/app@sha256:...> [--node-pool p | --node id ...] --ttl 3600 [--reason ...]",
		)
	}
	body := map[string]any{
		"project_id": *project, "image_ref": *image,
		"ttl_seconds": *ttl, "reason": *reason,
	}
	put(body, "node_pool", nodePool)
	putAny(body, "node_ids", nodeIDs, len(nodeIDs) > 0)
	return doRequest(apiClient, "POST", "/v1/images/pins", body, nil, resolveIdemKey(*idem), true)
}
