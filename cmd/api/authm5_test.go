package main

import (
	"net/http"
	"testing"
)

// routeScope 覆盖面守卫（P1-4）：敏感端点必须在 scope 表内登记。
// 新端点若漏登记，默认只需认证即可写——此测试把“必须显式授权”的门脸守住。
func TestRouteScopeCoversSensitiveRoutes(t *testing.T) {
	must := map[string]string{
		"POST /v1/machines":                   "write",
		"GET /v1/machines":                    "read",
		"GET /v1/operations":                  "read",
		"GET /v1/operations/{id}":             "read",
		"DELETE /v1/machines/{id}":            "write",
		"POST /v1/apps":                       "write",
		"POST /v1/secrets":                    "write",
		"POST /v1/apikeys":                    "admin",
		"DELETE /v1/apikeys/{id}":             "admin",
		"POST /v1/system/reprojections":       "admin",
		"POST /v1/nodes/{id}/drain":           "admin",
		"POST /v1/nodes/{id}/ready":           "admin",
		"GET /v1/machines/{id}/traffic-token": "write",
	}
	for pattern, want := range must {
		got, ok := routeScope[pattern]
		if !ok {
			t.Errorf("routeScope missing %q (want %s)", pattern, want)
			continue
		}
		if scopeRank[got] < scopeRank[want] {
			t.Errorf("routeScope[%q]=%s weaker than %s", pattern, got, want)
		}
	}
}

func TestScopeRankOrdering(t *testing.T) {
	if !(scopeRank["read"] < scopeRank["write"] && scopeRank["write"] < scopeRank["admin"]) {
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
