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

// 受限 admin key（project 绑定）绝不能：
//   - 创建全局 key（project_id=""）或他 project 的 key；
//   - 列出全部 key；
//   - 撤销任何 key。
// 只有全局身份（root token 或 project_id="" 的 admin key）放行。

// 第一层：middleware 必须拦截受限身份（不依赖 handler 复核）。
// 用真实 PG 造一枚 project 受限 admin key 与一枚全局 admin key。
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

	// 受限 admin：四条定向操作（含“给自己 project 造 key”）全部 403。
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/v1/apikeys", `{"name":"x","scopes":["admin"],"project_id":""}`},         // 造全局 key
		{http.MethodPost, "/v1/apikeys", `{"name":"x","scopes":["admin"],"project_id":"t-proj-y"}`}, // 造他 project key
		{http.MethodGet, "/v1/apikeys", ""},                                                         // list 全部
		{http.MethodDelete, "/v1/apikeys/apik_any", ""},                                             // revoke 任意
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+scopedPlain)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != 403 {
			t.Fatalf("%s %s: scoped admin status = %d, want 403; body=%q",
				tc.method, tc.path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "global identity") {
			t.Fatalf("%s %s: 403 must explain global identity requirement, got %q",
				tc.method, tc.path, rec.Body.String())
		}
	}

	// 全局 admin：middleware 不拦截（handler 正常工作，list 200）。
	req := httptest.NewRequest(http.MethodGet, "/v1/apikeys", nil)
	req.Header.Set("Authorization", "Bearer "+globalPlain)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("global admin list status = %d, want 200; body=%q", rec.Code, rec.Body.String())
	}
	var out struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Keys) == 0 {
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

// 第二层：handler 自身再次复核（中间件单点失效/漏注册时，返回的也必须是
// 403 而不是越过 guard 落库）。apiKeys Manager 指向已关闭的池——若 handler
// 误触 PG 会直接报错而非 403，测试即失败。
func TestAPIKeyHandlersReverifyGlobalIdentity(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), "postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	a := &API{apiKeys: apikeys.New(pool)}

	identities := []identity{
		{Kind: "key", KeyID: "k1", ProjectID: "p-x", Scopes: []string{"admin"}}, // 受限 admin
		{Kind: "key", KeyID: "k2", ProjectID: "p-x", Scopes: nil},               // 受限 read
		{Kind: "anon"}, // 无身份（中间件被绕过）
	}
	for _, id := range identities {
		for _, call := range []func(*API, http.ResponseWriter, *http.Request){
			func(a *API, w http.ResponseWriter, r *http.Request) { a.createAPIKey(w, r) },
			func(a *API, w http.ResponseWriter, r *http.Request) { a.listAPIKeys(w, r) },
			func(a *API, w http.ResponseWriter, r *http.Request) { a.revokeAPIKey(w, r) },
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
