// apikeys_test.go：P0（R2 评审）apikey 管理端点跨租户锁定的行为测试。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// v1.5 自助 key：受限 project admin 可管理本项目 key（目标 project == 自身 +
// 申请 scopes ⊆ 自身），仍不能碰全局 key 或他项目。全局身份不受限。
// 中间件只做 admin scope 门槛，越权边界由 handler 强制——本测试走全链路断言。
func TestAPIKeyEndpointsRequireGlobalIdentityMiddleware(t *testing.T) {
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run apikey middleware tests")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}

	// api_keys.project_id 外键到 projects：先建两个项目。
	st := store.New(pool)
	for _, p := range []string{"t-proj-x", "t-proj-y"} {
		if err := st.EnsureProject(t.Context(), p, p); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM api_keys WHERE name IN ('t-scoped-admin','t-global-admin','child')`)
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM projects WHERE id IN ('t-proj-x','t-proj-y')`)
	})
	mgr := apikeys.New(pool)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM api_keys WHERE name IN ('t-scoped-admin','t-global-admin','child')`)
	})
	_, scopedPlain, err := mgr.Create(t.Context(), "t-scoped-admin", []string{"admin"}, "t-proj-x", 0)
	if err != nil {
		t.Fatal(err)
	}
	_, globalPlain, err := mgr.Create(t.Context(), "t-global-admin", []string{"admin"}, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	a := &API{store: store.New(pool), apiKeys: mgr}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/apikeys", a.auth(a.createAPIKey))
	mux.HandleFunc("GET /v1/apikeys", a.auth(a.listAPIKeys))
	mux.HandleFunc("DELETE /v1/apikeys/{id}", a.auth(a.revokeAPIKey))

	// 受限 admin 越权操作仍 403：造他 project key。
	// 注：project_id 留空不是“造全局 key”——handler 按 clamp 语义归一到自身
	// 项目（全局 key 只能由全局身份显式创建，见下），此处单独断言归一行为。
	req0 := httptest.NewRequest(http.MethodPost, "/v1/apikeys",
		strings.NewReader(`{"name":"x","scopes":["admin"],"project_id":"t-proj-y"}`))
	req0.Header.Set("Authorization", "Bearer "+scopedPlain)
	rec0 := httptest.NewRecorder()
	mux.ServeHTTP(rec0, req0)
	if rec0.Code != 403 {
		t.Fatalf("scoped admin cross-project create status = %d, want 403; body=%q",
			rec0.Code, rec0.Body.String())
	}
	// project_id 留空 → 归一到自身项目（201，且返回的 project_id 为自身）。
	reqClamp := httptest.NewRequest(http.MethodPost, "/v1/apikeys",
		strings.NewReader(`{"name":"clamped","scopes":["read"],"project_id":""}`))
	reqClamp.Header.Set("Authorization", "Bearer "+scopedPlain)
	recClamp := httptest.NewRecorder()
	mux.ServeHTTP(recClamp, reqClamp)
	if recClamp.Code != 201 {
		t.Fatalf("scoped empty-project create status = %d, want 201; body=%q",
			recClamp.Code, recClamp.Body.String())
	}
	var clamped struct {
		Project string `json:"project_id"`
	}
	if err := json.Unmarshal(recClamp.Body.Bytes(), &clamped); err != nil || clamped.Project != "t-proj-x" {
		t.Fatalf("empty project_id must clamp to own project, got %q err=%v", recClamp.Body.String(), err)
	}

	// 受限 admin 自助：给自己 project 造 read key（scope 子集）应 201。
	req := httptest.NewRequest(http.MethodPost, "/v1/apikeys",
		strings.NewReader(`{"name":"self-read","scopes":["read"],"project_id":"t-proj-x"}`))
	req.Header.Set("Authorization", "Bearer "+scopedPlain)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("scoped self-issue status = %d, want 201; body=%q", rec.Code, rec.Body.String())
	}
	// 提权尝试：受限 admin 不能签发自己没有的 scope（此处 scoped admin 有 admin，
	// 用另一枚 read-only 受限 key 验证超集拒绝）。
	_, scopedReadPlain, err := mgr.Create(t.Context(), "t-scoped-read", []string{"read"}, "t-proj-x", 0)
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/apikeys",
		strings.NewReader(`{"name":"escalate","scopes":["write"],"project_id":"t-proj-x"}`))
	req.Header.Set("Authorization", "Bearer "+scopedReadPlain)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	// read key 连中间件 admin 门槛都过不了（403 insufficient scope）。
	if rec.Code != 403 {
		t.Fatalf("scoped read self-issue status = %d, want 403; body=%q", rec.Code, rec.Body.String())
	}

	// 受限 admin list：200 且仅见本项目。
	req = httptest.NewRequest(http.MethodGet, "/v1/apikeys", nil)
	req.Header.Set("Authorization", "Bearer "+scopedPlain)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("scoped list status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var scopedList struct {
		Keys []struct {
			Project string `json:"project_id"`
		} `json:"keys"`
	}
	if uerr := json.Unmarshal(rec.Body.Bytes(), &scopedList); uerr != nil {
		t.Fatal(uerr)
	}
	for _, k := range scopedList.Keys {
		if k.Project != "t-proj-x" {
			t.Fatalf("scoped list leaked project %q", k.Project)
		}
	}

	// 全局 admin：handler 正常工作，list 200。
	req = httptest.NewRequest(http.MethodGet, "/v1/apikeys", nil)
	req.Header.Set("Authorization", "Bearer "+globalPlain)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("global admin list status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var out struct {
		Keys []map[string]any `json:"keys"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Keys) == 0 {
		t.Fatalf("global admin list must return key metadata, got %q err=%v", rec.Body.String(), err)
	}

	// 全局 admin create 带 project_id>"" 仍放行（身份全局即可，创建目标自由）。
	req = httptest.NewRequest(http.MethodPost, "/v1/apikeys",
		strings.NewReader(`{"name":"child","scopes":["read"],"project_id":"t-proj-y"}`))
	req.Header.Set("Authorization", "Bearer "+globalPlain)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("global admin create-project-key status = %d, want 201; body=%q", rec.Code, rec.Body.String())
	}
}

// 第二层：handler 自身再次复核 admin 能力（中间件单点失效/漏注册时，
// 非 admin 与匿名返回的也必须是 403 而不是越过 guard 落库）。
// 受限 admin 走自助路径，需真实 store，本测试只断言非 admin/匿名被拦。
func TestAPIKeyHandlersReverifyGlobalIdentity(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), "postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	a := &API{apiKeys: apikeys.New(pool)}

	identities := []identity{
		{Kind: "key", KeyID: "k2", ProjectID: "p-x", Scopes: nil},              // 受限 read
		{Kind: "key", KeyID: "k3", ProjectID: "p-x", Scopes: []string{"read"}}, // 受限 read
		{Kind: "anon"}, // 无身份（中间件被绕过）
	}
	for _, id := range identities {
		for _, call := range []func(*API, http.ResponseWriter, *http.Request){
			func(a *API, w http.ResponseWriter, r *http.Request) { a.createAPIKey(w, r) },
			func(a *API, w http.ResponseWriter, r *http.Request) { a.listAPIKeys(w, r) },
			func(a *API, w http.ResponseWriter, r *http.Request) { a.revokeAPIKey(w, r) },
			func(a *API, w http.ResponseWriter, r *http.Request) { a.rotateAPIKey(w, r) },
		} {
			req := httptest.NewRequest(http.MethodGet, "/v1/apikeys",
				strings.NewReader(`{"name":"x","project_id":"etc"}`))
			req.SetPathValue("id", "apik_any")
			req = req.WithContext(withIdentity(req.Context(), id))
			rec := httptest.NewRecorder()
			call(a, rec, req)
			if rec.Code != 403 {
				t.Fatalf("identity %+v must be blocked at handler layer: status=%d body=%q",
					id, rec.Code, rec.Body.String())
			}
		}
	}
}

// v1.5 自助 key 委托逻辑（纯函数，无 DB）：全局自由、受限归一本项目 +
// 能力子集，无越权。
func TestResolveCreateTargetDelegation(t *testing.T) {
	global := identity{Kind: "key", Scopes: []string{"admin"}}
	if p, s, ok := resolveCreateTarget(global, "any", []string{"deploy"}); !ok || p != "any" || len(s) != 1 {
		t.Fatalf("global must pass through: %q %v %v", p, s, ok)
	}
	scopedAdmin := identity{Kind: "key", ProjectID: "p-a", Scopes: []string{"admin"}}
	// 留空归一到自身。
	if p, _, ok := resolveCreateTarget(scopedAdmin, "", []string{"read"}); !ok || p != "p-a" {
		t.Fatalf("empty project must clamp to self: %q %v", p, ok)
	}
	// 本项目 + 能力子集放行（含 admin 签发 deploy/exec：能力语义）。
	for _, scopes := range [][]string{{"read"}, {"deploy"}, {"read", "exec"}, {"admin"}} {
		if _, _, ok := resolveCreateTarget(scopedAdmin, "p-a", scopes); !ok {
			t.Fatalf("scoped admin must issue %v in own project", scopes)
		}
	}
	// 他项目拒绝。
	if _, _, ok := resolveCreateTarget(scopedAdmin, "p-b", []string{"read"}); ok {
		t.Fatal("scoped admin must not issue for another project")
	}
	// 非法 scope 永不放行（scopeAllows 对未知 scope 为 false）。
	if _, _, ok := resolveCreateTarget(scopedAdmin, "p-a", []string{"root"}); ok {
		t.Fatal("unknown scope must be rejected")
	}
	if scopesSubset([]string{"exec"}, []string{"read", "deploy"}) {
		t.Fatal("deployer must not cover exec")
	}
	if !scopesSubset([]string{"read", "deploy"}, []string{"write"}) {
		t.Fatal("legacy write must cover read+deploy")
	}
}

// authDisabled 模式注入 root 身份，仍视为全局（与 auth() 分支行为一致）。
func TestIdentityIsGlobal(t *testing.T) {
	if !identityIsGlobal(identity{Kind: "root"}) {
		t.Fatal("root must be global")
	}
	if !identityIsGlobal(identity{Kind: "key", ProjectID: ""}) {
		t.Fatal("unscoped key must be global")
	}
	if identityIsGlobal(identity{Kind: "key", ProjectID: "p", Scopes: []string{"admin"}}) {
		t.Fatal("scoped admin key must not be global")
	}
	if identityIsGlobal(identity{Kind: "anon"}) {
		t.Fatal("anon must not be global")
	}
}
