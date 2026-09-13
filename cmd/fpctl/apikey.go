// apikey 子命令（M5.1：fpctl apikey create/ls/rm）。
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strings"
)

// fpctl apikey create --name NAME [--scope read --scope write | --role operator] [--project <id>] [--ttl-hours N]
// fpctl apikey ls [--project <id>]
// fpctl apikey rm <id>
// fpctl apikey rotate <id> [--ttl-hours N]
func runAPIKey(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: fpctl apikey <create|ls|rm|rotate>")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
		name := fs.String("name", "", "key 说明名")
		scopes := repeatable{}
		fs.Var(&scopes, "scope", "scope（可重复：read/deploy/exec/write/debug/admin），缺省 read")
		role := fs.String("role", "", "RBAC 角色（与 --scope 二选一：viewer|operator|deployer|maintainer|owner）")
		project := projectFlag(fs, "")
		ttlHours := fs.Int("ttl-hours", 0, "过期小时数（0=不过期）")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" {
			return errors.New("--name is required")
		}
		if *role != "" && len(scopes) > 0 {
			return errors.New("--role and --scope are mutually exclusive")
		}
		if *role == "" && len(scopes) == 0 {
			scopes = repeatable{"read"}
		}
		var out struct {
			ID      string   `json:"id"`
			Key     string   `json:"key"`
			Scopes  []string `json:"scopes"`
			Project string   `json:"project_id"`
		}
		if err := do("POST", "/v1/apikeys", map[string]any{
			"name": *name, "scopes": scopes, "role": *role, "project_id": *project, "ttl_hours": *ttlHours,
		}, &out); err != nil {
			return err
		}
		if global.json {
			return nil
		}
		fmt.Printf("created %s scopes=%s project=%q\nKEY: %s\n（密钥只显示这一次，泄露即 revoke 重建）\n",
			out.ID, strings.Join(out.Scopes, ","), orDefault(out.Project, "-"), out.Key)
	case "ls":
		lsfs := flag.NewFlagSet("apikey ls", flag.ExitOnError)
		lsProject := projectFlag(lsfs, "")
		_ = lsfs.Parse(args[1:])
		path := withQuery("/v1/apikeys", map[string]string{"project_id": *lsProject})
		var out struct {
			Keys []struct {
				ID       string   `json:"id"`
				Name     string   `json:"name"`
				Scopes   []string `json:"scopes"`
				Project  string   `json:"project_id"`
				Created  string   `json:"created_at"`
				LastUsed *string  `json:"last_used_at"`
				Revoked  bool     `json:"revoked"`
			} `json:"keys"`
		}
		if err := do("GET", path, nil, &out); err != nil {
			return err
		}
		if global.json {
			return nil
		}
		for _, k := range out.Keys {
			state := "active"
			if k.Revoked {
				state = "revoked"
			}
			lastUsed := "-"
			if k.LastUsed != nil {
				lastUsed = (*k.LastUsed)[:19]
			}
			fmt.Printf("%s  %-8s %-24s scopes=%-12s project=%-12s last_used=%s created=%s\n",
				k.ID, state, k.Name, strings.Join(k.Scopes, ","), orDefault(k.Project, "-"), lastUsed, k.Created[:19])
		}
	case "rm":
		id, err := oneArg(args[1:], "usage: fpctl apikey rm <id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/apikeys/"+url.PathEscape(id), nil, nil)
	case "rotate":
		keyID, err := oneArg(args[1:], "usage: fpctl apikey rotate <id> [--ttl-hours N]")
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("apikey rotate", flag.ExitOnError)
		ttlHours := fs.Int("ttl-hours", -1, "新 key 过期小时数（缺省继承旧 key 剩余有效期，0=不过期）")
		_ = fs.Parse(args[2:])
		body := map[string]any{}
		putAny(body, "ttl_hours", *ttlHours, *ttlHours >= 0)
		var out struct {
			ID      string   `json:"id"`
			Key     string   `json:"key"`
			Scopes  []string `json:"scopes"`
			Project string   `json:"project_id"`
		}
		if err := do("POST", "/v1/apikeys/"+url.PathEscape(keyID)+"/rotate", body, &out); err != nil {
			return err
		}
		if global.json {
			return nil
		}
		fmt.Printf("rotated %s -> %s scopes=%s project=%q\nKEY: %s\n（旧 key 已撤销；密钥只显示这一次）\n",
			keyID, out.ID, strings.Join(out.Scopes, ","), orDefault(out.Project, "-"), out.Key)
	default:
		return fmt.Errorf("unknown apikey command %q", args[0])
	}
	return nil
}
