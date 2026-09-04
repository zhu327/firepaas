package main

import (
	"bufio"
	"net/http"
	"os"
	"regexp"
	"testing"

	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// routeScope 覆盖面守卫：所有 auth 包裹端点必须显式登记，漏登记会被拒绝。
func TestAuthWrappedRoutesHaveExplicitScopes(t *testing.T) {
	file, err := os.Open("main.go")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	route := regexp.MustCompile(`mux\.HandleFunc\("([A-Z]+ /v1/[^" ]*(?:\{[^}]+\}[^" ]*)*)", api\.auth\(`)
	scanner := bufio.NewScanner(file)
	seen := make(map[string]bool)
	for scanner.Scan() {
		matches := route.FindStringSubmatch(scanner.Text())
		if len(matches) == 0 {
			continue
		}
		pattern := matches[1]
		seen[pattern] = true
		if scope, ok := routeScope[pattern]; !ok {
			t.Errorf("auth-wrapped route %q has no explicit scope", pattern)
		} else if _, ok := scopeGrants[scope]; !ok {
			t.Errorf("auth-wrapped route %q has unknown scope %q", pattern, scope)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 {
		t.Fatal("found no auth-wrapped routes in main.go")
	}
	for pattern := range routeScope {
		if !seen[pattern] {
			t.Errorf("routeScope contains stale route %q", pattern)
		}
	}
}

func TestRouteScopeCoversSensitiveRoutes(t *testing.T) {
	must := map[string]string{
		"POST /v1/machines":                   "write",
		"GET /v1/machines":                    "read",
		"GET /v1/operations":                  "read",
		"GET /v1/operations/{id}":             "read",
		"DELETE /v1/machines/{id}":            "write",
		"POST /v1/apps":                       "deploy",
		"POST /v1/apps/{id}/deployments":      "deploy",
		"POST /v1/apps/{id}/scale":            "deploy",
		"POST /v1/apps/{id}/rollback":         "deploy",
		"DELETE /v1/apps/{id}":                "deploy",
		"POST /v1/secrets":                    "write",
		"POST /v1/apikeys":                    "admin",
		"DELETE /v1/apikeys/{id}":             "admin",
		"POST /v1/apikeys/{id}/rotate":        "admin",
		"POST /v1/system/reprojections":       "admin",
		"POST /v1/nodes/{id}/drain":           "admin",
		"POST /v1/nodes/{id}/ready":           "admin",
		"POST /v1/projects":                   "admin",
		"GET /v1/projects":                    "read",
		"GET /v1/projects/{id}":               "read",
		"DELETE /v1/projects/{id}":            "admin",
		"GET /v1/machines/{id}/traffic-token": "write",
		"GET /v1/machines/{id}/logs":          "exec",
		"POST /v1/machines/{id}/exec":         "exec",
		"PUT /v1/machines/{id}/files":         "exec",
		"GET /v1/machines/{id}/files":         "exec",
		"GET /v1/apps/{id}/egress-audit":      "read",
		"POST /v1/machines/{id}/snapshots":    "write",
		"GET /v1/snapshots":                   "read",
		"GET /v1/snapshots/{id}":              "read",
		"DELETE /v1/snapshots/{id}":           "write",
		"POST /v1/snapshots/{id}/fork":        "write",
		"POST /v1/machines/{id}/rescue":       "write",
		"POST /v1/volumes":                    "write",
		"GET /v1/volumes":                     "read",
		"GET /v1/volumes/{id}":                "read",
		"DELETE /v1/volumes/{id}":             "write",
	}
	for pattern, want := range must {
		got, ok := routeScope[pattern]
		if !ok {
			t.Errorf("routeScope missing %q (want %s)", pattern, want)
			continue
		}
		if got != want {
			t.Errorf("routeScope[%q]=%s want %s", pattern, got, want)
		}
	}
}

func TestV13ByIDRoutesAreProjectGated(t *testing.T) {
	patterns := []string{
		"GET /v1/apps/{id}/egress-audit",
		"POST /v1/machines/{id}/snapshots",
		"GET /v1/snapshots/{id}",
		"DELETE /v1/snapshots/{id}",
		"POST /v1/snapshots/{id}/fork",
		"POST /v1/machines/{id}/snapshot-schedules",
		"GET /v1/machines/{id}/snapshot-schedules",
		"DELETE /v1/machines/{id}/snapshot-schedules/{schedule}",
		"POST /v1/machines/{id}/rescue",
		"GET /v1/volumes/{id}",
		"DELETE /v1/volumes/{id}",
		"POST /v1/machines/{id}/volume-attach",
		"POST /v1/machines/{id}/volume-detach",
	}
	for _, pattern := range patterns {
		if !projectGated[pattern] {
			t.Errorf("v1.3 by-ID route %q must be project gated", pattern)
		}
	}
}

func TestProjectQuotaIsProjectGated(t *testing.T) {
	const pattern = "GET /v1/projects/{id}/quota"
	if !projectGated[pattern] {
		t.Fatalf("%s must be covered by the cross-project gate", pattern)
	}
	a := &API{}
	request := func(project string) *http.Request {
		r, err := http.NewRequest(http.MethodGet, "/v1/projects/"+project+"/quota", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Pattern = pattern
		r.SetPathValue("id", project)
		return r
	}
	r := request("project-a")
	if !a.allowsProjectResource(r.Context(), r, "project-a") {
		t.Fatal("restricted key must be allowed to read its own quota")
	}
	r = request("project-b")
	if a.allowsProjectResource(r.Context(), r, "project-a") {
		t.Fatal("restricted key must not read another project's quota")
	}
}

// v1.5 租户自助预热：prewarm/pin 写 = write，coverage/pins 读 = read；
// 拓扑保护由 handler 层（node_ids 禁止 + coverage 脱敏）承担，不再用 admin 一刀切。
func TestPrewarmTenantSelfServiceScopes(t *testing.T) {
	want := map[string]string{
		"POST /v1/images/prewarm":     "write",
		"GET /v1/images/coverage":     "read",
		"POST /v1/images/pins":        "write",
		"GET /v1/images/pins":         "read",
		"DELETE /v1/images/pins/{id}": "write",
	}
	for pattern, w := range want {
		if got := routeScope[pattern]; got != w {
			t.Errorf("routeScope[%q] = %q, want %s", pattern, got, w)
		}
	}
}

func TestScopeAllowsDeployExecSplit(t *testing.T) {
	// “可 exec 不可 deploy”：operator（read+exec）能进 exec 路由，进不了 deploy。
	if !scopeAllows([]string{"read", "exec"}, "exec") {
		t.Fatal("operator must allow exec")
	}
	if scopeAllows([]string{"read", "exec"}, "deploy") {
		t.Fatal("operator must not allow deploy")
	}
	// “可 deploy 不可 exec”：deployer 反之。
	if !scopeAllows([]string{"read", "deploy"}, "deploy") {
		t.Fatal("deployer must allow deploy")
	}
	if scopeAllows([]string{"read", "deploy"}, "exec") {
		t.Fatal("deployer must not allow exec")
	}
	// 历史兼容：write 全通（除 admin），debug == exec，admin 全通。
	for _, need := range []string{"read", "exec", "deploy", "write"} {
		if !scopeAllows([]string{"write"}, need) {
			t.Fatalf("legacy write must allow %s", need)
		}
	}
	if scopeAllows([]string{"write"}, "admin") {
		t.Fatal("write must not allow admin")
	}
	if !scopeAllows([]string{"debug"}, "exec") || scopeAllows([]string{"debug"}, "deploy") {
		t.Fatal("legacy debug must allow exec only")
	}
	for _, need := range []string{"read", "exec", "deploy", "write", "admin"} {
		if !scopeAllows([]string{"admin"}, need) {
			t.Fatalf("admin must allow %s", need)
		}
	}
	if scopeAllows([]string{"read"}, "exec") || scopeAllows(nil, "read") {
		t.Fatal("read/nil must not escalate")
	}
}

func TestScopeRankOrdering(t *testing.T) {
	if scopeRank["read"] >= scopeRank["debug"] || scopeRank["debug"] > scopeRank["write"] ||
		scopeRank["write"] >= scopeRank["admin"] {
		t.Fatalf("scope ordering broken: %v", scopeRank)
	}
	if maxRank([]string{"read", "write", "bogus"}) != scopeRank["write"] {
		t.Fatalf("maxRank must ignore unknown scopes")
	}
	if maxRank(nil) != 0 {
		t.Fatalf("maxRank(nil) != 0")
	}
}

func TestCallerName(t *testing.T) {
	if callerName(identity{Kind: "root"}) != "root" {
		t.Fatal("root caller name")
	}
	if callerName(identity{Kind: "key", KeyName: "ci", KeyID: "k1"}) != "ci" {
		t.Fatal("key caller name prefers KeyName")
	}
	if callerName(identity{Kind: "key", KeyID: "k1"}) != "k1" {
		t.Fatal("key caller falls back to KeyID")
	}
	if callerName(identity{Kind: "anon"}) != "" {
		t.Fatal("anon must be empty")
	}
}

// clampBodyProject（P1-2）：受限 key 的 body.project_id 越权 → 拒绝；
// 留空 → 归一到 identity project；不受限调用方原样放行。
func TestClampBodyProject(t *testing.T) {
	mkReq := func(id identity) *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "/v1/apps", nil)
		return r.WithContext(withIdentity(r.Context(), id))
	}
	// 受限 key 指定他 project。
	if _, ok := clampBodyProject(mkReq(identity{Kind: "key", ProjectID: "other"}), "dev"); ok {
		t.Fatal("restricted key must not set foreign project")
	}
	// 受限 key 留空 → 归一。
	if p, ok := clampBodyProject(mkReq(identity{Kind: "key", ProjectID: "other"}), ""); !ok || p != "other" {
		t.Fatalf("empty project must clamp to identity project, got %q ok=%v", p, ok)
	}
	// 受限 key 指定自己 → 放行。
	if p, ok := clampBodyProject(mkReq(identity{Kind: "key", ProjectID: "dev"}), "dev"); !ok || p != "dev" {
		t.Fatalf("own project must pass, got %q ok=%v", p, ok)
	}
	// root 显式指定 → 放行。
	if p, ok := clampBodyProject(mkReq(identity{Kind: "root"}), "any"); !ok || p != "any" {
		t.Fatalf("unrestricted caller must keep explicit project, got %q ok=%v", p, ok)
	}
}

// TestMachineReadyPredicate（v1.2-D review 修复）：wait 的目标语义是
// "execution X ready"，不是 "RUNNING"。RUNNING 但 NOT_READY/UNKNOWN 时
// 不得返回 reached；UNCONFIGURED 等价 READY（ADR-0008）。
func TestMachineReadyPredicate(t *testing.T) {
	cases := []struct {
		name      string
		state     string
		readiness string
		want      bool
	}{
		{"running ready", "RUNNING", "READY", true},
		{"running unconfigured", "RUNNING", "UNCONFIGURED", true},
		{"paused ready", "PAUSED", "READY", true},
		{"running not_ready", "RUNNING", "NOT_READY", false},
		{"running unknown readiness", "RUNNING", "UNKNOWN", false},
		{"running empty readiness", "RUNNING", "", false},
		{"initializing ready", "INITIALIZING", "READY", false},
		{"stopped ready", "STOPPED", "READY", false},
		{"empty state", "", "READY", false},
	}
	for _, c := range cases {
		m := &store.Machine{ObservedState: c.state, ObservedReadiness: c.readiness}
		if got := machineReady(m); got != c.want {
			t.Errorf("%s: machineReady(state=%q readiness=%q) = %v, want %v",
				c.name, c.state, c.readiness, got, c.want)
		}
	}
}
