package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strings"
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
		return do("GET", "/v1/images/pins", nil, nil)
	case "unpin":
		if len(args) < 2 {
			return errors.New("usage: fpctl images unpin <pin_id>")
		}
		return do("DELETE", "/v1/images/pins/"+url.PathEscape(args[1]), nil, nil)
	default:
		return fmt.Errorf("unknown images command %q", args[0])
	}
}

func nodeTargetFlags(fs *flag.FlagSet, nodePool *string, nodeIDs *[]string) {
	fs.StringVar(nodePool, "node-pool", "", "target node pool")
	fs.Var(stringSliceFlag(nodeIDs), "node", "target node id (repeatable)")
}

type stringSliceValue []string

func (s *stringSliceValue) String() string { return strings.Join(*s, ",") }
func (s *stringSliceValue) Set(v string) error {
	if v == "" {
		return errors.New("empty --node value")
	}
	*s = append(*s, v)
	return nil
}

func stringSliceFlag(target *[]string) *stringSliceValue {
	return (*stringSliceValue)(target)
}

func runImagesPrewarm(args []string) error {
	fs := flag.NewFlagSet("images prewarm", flag.ExitOnError)
	project := fs.String("project", "dev", "project id")
	image := fs.String("image", "", "digest-pinned image ref (registry/app@sha256:...)")
	var nodePool string
	var nodeIDs []string
	nodeTargetFlags(fs, &nodePool, &nodeIDs)
	_ = fs.Parse(args)
	if *image == "" || (nodePool == "" && len(nodeIDs) == 0) {
		return errors.New(
			"usage: fpctl images prewarm --image <registry/app@sha256:...> [--node-pool p | --node id ...] [--project dev]",
		)
	}
	body := map[string]any{"project_id": *project, "image_ref": *image}
	if nodePool != "" {
		body["node_pool"] = nodePool
	}
	if len(nodeIDs) > 0 {
		body["node_ids"] = nodeIDs
	}
	return do("POST", "/v1/images/prewarm", body, nil)
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
	q := url.Values{}
	if *image != "" {
		q.Set("image_ref", *image)
	}
	if *digest != "" {
		q.Set("digest", *digest)
	}
	if *nodePool != "" {
		q.Set("node_pool", *nodePool)
	}
	return do("GET", "/v1/images/coverage?"+q.Encode(), nil, nil)
}

func runImagesPin(args []string) error {
	fs := flag.NewFlagSet("images pin", flag.ExitOnError)
	project := fs.String("project", "dev", "project id")
	image := fs.String("image", "", "digest-pinned image ref")
	ttl := fs.Int64("ttl", 3600, "pin TTL seconds")
	reason := fs.String("reason", "", "pin reason (audited)")
	var nodePool string
	var nodeIDs []string
	nodeTargetFlags(fs, &nodePool, &nodeIDs)
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
	if nodePool != "" {
		body["node_pool"] = nodePool
	}
	if len(nodeIDs) > 0 {
		body["node_ids"] = nodeIDs
	}
	return do("POST", "/v1/images/pins", body, nil)
}
