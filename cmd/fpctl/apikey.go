// apikey 子命令（M5.1：fpctl apikey create/ls/rm）。
package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
)

// fpctl apikey create --name NAME [--scope read --scope write] [--project <id>] [--ttl-hours N]
// fpctl apikey ls
// fpctl apikey rm <id>
func runAPIKey(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: fpctl apikey <create|ls|rm>")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("apikey create", flag.ExitOnError)
		name := fs.String("name", "", "key 说明名")
		scopes := secretFlags{}
		fs.Var(&scopes, "scope", "scope（可重复：read/write/admin），缺省 read")
		project := fs.String("project", "", "限制项目（空=全部项目）")
		ttlHours := fs.Int("ttl-hours", 0, "过期小时数（0=不过期）")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" {
			return errors.New("--name is required")
		}
		if len(scopes) == 0 {
			scopes = secretFlags{"read"}
		}
		var out struct {
			ID      string   `json:"id"`
			Key     string   `json:"key"`
			Scopes  []string `json:"scopes"`
			Project string   `json:"project_id"`
		}
		if err := do("POST", "/v1/apikeys", map[string]any{
			"name": *name, "scopes": scopes, "project_id": *project, "ttl_hours": *ttlHours,
		}, &out); err != nil {
			return err
		}
		fmt.Printf("created %s scopes=%s project=%q\nKEY: %s\n（密钥只显示这一次，泄露即 revoke 重建）\n",
			out.ID, strings.Join(out.Scopes, ","), orDash(out.Project), out.Key)
	case "ls":
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
		if err := do("GET", "/v1/apikeys", nil, &out); err != nil {
			return err
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
				k.ID, state, k.Name, strings.Join(k.Scopes, ","), orDash(k.Project), lastUsed, k.Created[:19])
		}
	case "rm":
		id, err := oneArg(args[1:], "usage: fpctl apikey rm <id>")
		if err != nil {
			return err
		}
		return do("DELETE", "/v1/apikeys/"+id, nil, nil)
	default:
		return fmt.Errorf("unknown apikey command %q", args[0])
	}
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
