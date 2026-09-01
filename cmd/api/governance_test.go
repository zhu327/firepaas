package main

import "testing"

// v1.2-E（ADR-0035）：路由分类——流式端点 → runtime-stream；
// 其余 GET → read；写方法 → mutation。
func TestRouteClassOf(t *testing.T) {
	cases := []struct {
		pattern, method string
		want            string
	}{
		{"/v1/machines/{id}/logs", "GET", "runtime-stream"},
		{"/v1/machines/{id}/exec", "POST", "runtime-stream"},
		{"/v1/machines/{id}/files", "PUT", "runtime-stream"},
		{"/v1/machines/{id}/files", "GET", "runtime-stream"},
		{"/v1/machines/{id}/wait", "GET", "runtime-stream"},
		{"/v1/operations/{id}/wait", "GET", "runtime-stream"},
		{"/v1/machines", "GET", "read"},
		{"/v1/machines/{id}", "GET", "read"},
		{"/v1/apps", "GET", "read"},
		// r.Pattern 实际带方法前缀（net/http 语义）——归一后分类。
		{"GET /v1/machines/{id}/wait", "GET", "runtime-stream"},
		{"POST /v1/machines/{id}/exec", "POST", "runtime-stream"},
		{"GET /v1/rollouts/{id}/wait", "GET", "runtime-stream"},
		{"/v1/machines", "POST", "mutation"},
		{"/v1/machines/{id}", "DELETE", "mutation"},
		{"/v1/apps/{id}/deployments", "POST", "mutation"},
		{"/v1/secrets", "POST", "mutation"},
	}
	for _, c := range cases {
		if got := string(routeClassOf(c.pattern, c.method)); got != c.want {
			t.Errorf("routeClassOf(%s %s) = %s, want %s", c.method, c.pattern, got, c.want)
		}
	}
}
