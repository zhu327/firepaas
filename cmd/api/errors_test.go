// errors_test.go：5xx 收口 helper、recover 中间件、fence 字段拒绝、
// apikey 认证后端 503 语义的可观察行为测试。
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/apikeys"
	"github.com/zhu327/firepaas/internal/controlplane/secrets"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

// 5xx body 固定文案，不回吐内部错误原文。
func TestWriteInternalErrHidesDetail(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/machines", nil)
	req.Pattern = "GET /v1/machines"

	rec := httptest.NewRecorder()
	writeInternalErr(rec, req, fmt.Errorf("pq: relation \"machines\" does not exist"))
	if rec.Code != 500 {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "pq:") || !strings.Contains(rec.Body.String(), "internal error") {
		t.Fatalf("500 body must be generic, got %q", rec.Body.String())
	}

	// DB 连接不可用类 → 503（超时覆盖网络与 context deadline）。
	rec = httptest.NewRecorder()
	connErr := fmt.Errorf("dial: %w", context.DeadlineExceeded)
	if !isDBUnavailable(connErr) {
		t.Fatal("timeout error must classify as db-unavailable")
	}
	writeInternalErr(rec, req, connErr)
	if rec.Code != 503 {
		t.Fatalf("db-unavailable status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "service temporarily unavailable") {
		t.Fatalf("503 body = %q", rec.Body.String())
	}

	// 普通内部错误 → 500。
	if isDBUnavailable(fmt.Errorf("scan type mismatch")) {
		t.Fatal("plain internal error must not classify as db-unavailable")
	}
}

// panic 被兜住：log + 固定 500，不再让 net/http 默认处理切面泄漏。
func TestRecoverMiddleware(t *testing.T) {
	h := recoverMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "internal error") {
		t.Fatalf("panic response = %d %q", rec.Code, rec.Body.String())
	}

	// 正常路径不受影响。
	ok := false
	h = recoverMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ok = true
		w.WriteHeader(204)
	}))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if !ok || rec.Code != 204 {
		t.Fatalf("normal path broken: ok=%v code=%d", ok, rec.Code)
	}
}

// P0：fence 字段（machine_id/execution_id/generation）客户端不可提交，
// 400 且文案明示这些字段由服务端生成（否则无法走到 store）。
func TestCreateMachineRejectsFenceFields(t *testing.T) {
	a := &API{}
	cases := []string{
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","machine_id":"m-evil"}`,
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","execution_id":"exec-9"}`,
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","generation":999999}`,
	}
	for _, body := range cases {
		req := httptest.NewRequest(http.MethodPost, "/v1/machines", strings.NewReader(body))
		rec := httptest.NewRecorder()
		a.createMachine(rec, req)
		if rec.Code != 400 {
			t.Fatalf("body %s: status = %d, want 400", body, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "server-generated") {
			t.Fatalf("body %s: 400 message must say server-generated, got %q", body, rec.Body.String())
		}
	}
}

// P1：apikey 的 PG 查询失败 → 503（可重试），绝不降级成 401。
func TestAuthAPIKeyLookupFailure503(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close() // 关闭池：任何查询立即失败

	a := &API{apiToken: "root-token", apiKeys: apikeys.New(pool)}
	next := a.auth(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })

	req := httptest.NewRequest(http.MethodGet, "/v1/machines", nil)
	req.Pattern = "GET /v1/machines"
	req.Header.Set("Authorization", "Bearer fp_nottheroottoken")
	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Fatalf("pg failure status = %d, want 503", rec.Code)
	}
	if rec.Code == 401 {
		t.Fatal("pg failure must never degrade to 401")
	}
}

// root token 路径不经 apikey 后端，PG 挂了也不受影响。
func TestAuthRootTokenBypassesAPIKeyBackend(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	a := &API{apiToken: "root-token", apiKeys: apikeys.New(pool), authDisabled: false}
	next := a.auth(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })

	req := httptest.NewRequest(http.MethodGet, "/v1/machines", nil)
	req.Pattern = "GET /v1/machines"
	req.Header.Set("Authorization", "Bearer root-token")
	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, req)
	if rec.Code != 204 {
		t.Fatalf("root token status = %d, want 204", rec.Code)
	}
}

// P0#4 评审回流：putProjectQuota 的 PG 故障必须 5xx，不能误报 409。
func TestPutProjectQuotaPGFailureIs5xx(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	a := &API{store: store.New(pool)}
	body := `{"vcpu_quota":1,"mem_mib_quota":1,"disk_mib_quota":1,"machine_concurrency":1,"runtime_session_concurrency":1,"revision":1}`
	req := httptest.NewRequest(http.MethodPut, "/v1/projects/p1/quota", strings.NewReader(body))
	req.SetPathValue("id", "p1")
	rec := httptest.NewRecorder()
	a.putProjectQuota(rec, req)
	if rec.Code < 500 {
		t.Fatalf(
			"pg failure status = %d, want 5xx (must not masquerade as revision conflict); body=%q",
			rec.Code,
			rec.Body.String(),
		)
	}
	if strings.Contains(rec.Body.String(), "dial") || strings.Contains(rec.Body.String(), "pq:") {
		t.Fatalf("5xx body must not leak driver text, got %q", rec.Body.String())
	}
}

// R2 长尾：错误映射回归（关闭池 → 依赖不可用，必须 5xx，绝不伪装 404）。
// 各选一个能触发 store 调用的最小路径，覆盖 job-specific handler 家族：
//   - getVolume（snapshots.go read 路径）
//   - upsertSnapshotSchedule（snapshots.go write 路径）
//   - snapshotPreflight（read + 404 混淆上限）
//   - trafficToken（secrets.go 路径）
func TestHandlersPGFailureIs5xxLongTail(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	signer, err := traffic.NewSigner(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	a := &API{store: store.New(pool), traffic: signer}

	cases := []struct {
		name string
		call func(http.ResponseWriter, *http.Request)
	}{
		{"getVolume", func(w http.ResponseWriter, r *http.Request) {
			r.SetPathValue("id", "vol-x")
			a.getVolume(w, r)
		}},
		{"upsertSnapshotSchedule", func(w http.ResponseWriter, r *http.Request) {
			r.SetPathValue("id", "m-x")
			a.upsertSnapshotSchedule(w, r)
		}},
		{"snapshotPreflight", func(w http.ResponseWriter, r *http.Request) {
			r.SetPathValue("id", "snap-x")
			a.snapshotPreflight(w, r)
		}},
		{"trafficToken", func(w http.ResponseWriter, r *http.Request) {
			r.SetPathValue("id", "m-x")
			a.trafficToken(w, r)
		}},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "/v1/x", strings.NewReader("{}"))
		req.Pattern = "GET /v1/x"
		rec := httptest.NewRecorder()
		tc.call(rec, req)
		if rec.Code < 500 {
			t.Fatalf("%s: pg failure status = %d, want 5xx (no fake 404); body=%q",
				tc.name, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "pool is closed") {
			t.Fatalf("%s: 5xx body must not leak driver text, got %q", tc.name, rec.Body.String())
		}
	}
}

// R2 请求体防护：createMachine 负 vcpu 显式 400（不再靠 uint64 转换绕成
// 大度资源）。
func TestCreateMachineRejectsNegativeResources(t *testing.T) {
	a := &API{}
	for _, body := range []string{
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","vcpu":-1}`,
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","mem_mib":-256}`,
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","port":-80}`,
		`{"hostname":"h.local","image":"img:1","operation_id":"op-1","port":65536}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/machines", strings.NewReader(body))
		rec := httptest.NewRecorder()
		a.createMachine(rec, req)
		if rec.Code != 400 {
			t.Fatalf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
}

// createApp 的负 vcpu/mem 与越界 port 同样 400。
func TestCreateAppRejectsNegativeResources(t *testing.T) {
	a := &API{}
	for _, body := range []string{
		`{"hostname":"h.local","image":"img:1","vcpu":-1}`,
		`{"hostname":"h.local","image":"img:1","mem_mib":-1}`,
		`{"hostname":"h.local","image":"img:1","port":65536}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/apps", strings.NewReader(body))
		req = req.WithContext(withIdentity(req.Context(), identity{Kind: "root", Scopes: []string{"admin"}}))
		rec := httptest.NewRecorder()
		a.createApp(rec, req)
		if rec.Code != 400 {
			t.Fatalf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
}

// deleteMachine 显式 execution_id 与 machine 当前值不符 → 409 冲突。
// 用关闭池只能测到 store 失败路径； conflict 路径需要真实行，见
// apps 集成测试。此处验证超限 body 保护：createApp 超过 1MiB body 400。
func TestCreateAppBodyLimit(t *testing.T) {
	a := &API{}
	big := strings.Repeat(" ", 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/v1/apps",
		strings.NewReader(`{"hostname":"`+big+`","image":"img:1"}`))
	req = req.WithContext(withIdentity(req.Context(), identity{Kind: "root", Scopes: []string{"admin"}}))
	rec := httptest.NewRecorder()
	a.createApp(rec, req)
	if rec.Code != 400 {
		t.Fatalf("oversized body status = %d, want 400", rec.Code)
	}
}

// P0#4 评审回流：getSecretMeta 的 PG 故障必须 5xx，not-found 才是 404。
func TestGetSecretMetaPGFailureIs5xx(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	mgr, err := secrets.NewManager("MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=")
	if err != nil {
		t.Fatal(err)
	}
	a := &API{store: store.New(pool), secrets: mgr}
	req := httptest.NewRequest(http.MethodGet, "/v1/secrets/db", nil)
	req.SetPathValue("name", "db")
	rec := httptest.NewRecorder()
	a.getSecretMeta(rec, req)
	if rec.Code < 500 {
		t.Fatalf(
			"pg failure status = %d, want 5xx (must not masquerade as not-found); body=%q",
			rec.Code,
			rec.Body.String(),
		)
	}
	if strings.Contains(rec.Body.String(), "dial") || strings.Contains(rec.Body.String(), "pq:") {
		t.Fatalf("5xx body must not leak driver text, got %q", rec.Body.String())
	}
}
