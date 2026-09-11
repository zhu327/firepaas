// rbac_platform_test.go：review 2026-09-10 的 RBAC 回归。
//
// 项目 owner 角色展开为 scopes=["admin"]，中间件按 capability 放行；节点运维、
// 投影重建、配额/限流写是集群级能力，必须在 handler 层复核全局身份。
// 本测试用 nil store 断言「拒绝发生在任何副作用/存储访问之前」。
package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// TestPlatformRoutesRejectProjectScopedAdmin：受限 admin key 调平台专属端点
// 必须 403；handler 在 guard 之前不得触碰 store（此处 store 为 nil）。
func TestPlatformRoutesRejectProjectScopedAdmin(t *testing.T) {
	scoped := identity{Kind: "key", KeyID: "k-scoped", ProjectID: "p-a", Scopes: []string{"admin"}}
	cases := []struct {
		name   string
		method string
		path   string
		id     string
		body   string
		call   func(a *API, w http.ResponseWriter, r *http.Request)
	}{
		{
			name: "node drain", method: http.MethodPost, path: "/v1/nodes/n1/drain", id: "n1",
			body: `{"evacuate":true}`,
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.drainNode(w, r) },
		},
		{
			name: "node ready", method: http.MethodPost, path: "/v1/nodes/n1/ready", id: "n1",
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.readyNode(w, r) },
		},
		{
			name: "list nodes", method: http.MethodGet, path: "/v1/nodes",
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.listNodes(w, r) },
		},
		{
			name: "scheduler events", method: http.MethodGet, path: "/v1/system/scheduler-events",
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.listSchedulerEvents(w, r) },
		},
		{
			name: "reproject", method: http.MethodPost, path: "/v1/system/reprojections",
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.reproject(w, r) },
		},
		{
			name: "put quota", method: http.MethodPut, path: "/v1/projects/p-a/quota", id: "p-a",
			body: `{"vcpu":8,"mem_mib":8192,"disk_mib":10240,"machine_concurrency":4,"runtime_session_concurrency":2,"revision":1}`,
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.putProjectQuota(w, r) },
		},
		{
			// GET rate-limits 对项目内 admin 保留读取（review L5）；只验证写被拒。
			name: "put rate limits", method: http.MethodPut, path: "/v1/projects/p-a/rate-limits", id: "p-a",
			body: `{"read_rate":0,"read_burst":0,"mutation_rate":0,"mutation_burst":0,"stream_rate":0,"stream_burst":0}`,
			call: func(a *API, w http.ResponseWriter, r *http.Request) { a.putRateLimits(w, r) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reader *strings.Reader
			if tc.body != "" {
				reader = strings.NewReader(tc.body)
			}
			var req *http.Request
			if reader == nil {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			} else {
				req = httptest.NewRequest(tc.method, tc.path, reader)
			}
			if tc.id != "" {
				req.SetPathValue("id", tc.id)
			}
			req = req.WithContext(withIdentity(req.Context(), scoped))
			rec := httptest.NewRecorder()
			tc.call(&API{}, rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403; body=%q", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestGlobalIdentityPassesPlatformGuard：root 与无 project 绑定的 key 不被
// handler 层 guard 拒绝（否则合法运维路径会回归）。
func TestGlobalIdentityPassesPlatformGuard(t *testing.T) {
	for name, id := range map[string]identity{
		"root":       {Kind: "root", Scopes: []string{"admin"}},
		"global key": {Kind: "key", Scopes: []string{"admin"}},
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
			req = req.WithContext(withIdentity(req.Context(), id))
			if !(&API{}).requireGlobalIdentity(rec, req) {
				t.Fatalf("global identity rejected: %q", rec.Body.String())
			}
			if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
				t.Fatalf("guard wrote response for global identity: code=%d body=%q", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestCapabilitiesRedactsNodeIDsForScopedIdentity：受限身份只看到聚合计数，
// 全局身份保留 node_ids（与 prewarm coverage 的脱敏口径一致）。
func TestCapabilitiesRedactsNodeIDsForScopedIdentity(t *testing.T) {
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run capabilities redaction test")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := db.Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool)
	nodeID := "t-cap-node-" + uniqueSuffix(t)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM nodes WHERE id=$1`, nodeID)
	})
	if err := st.UpsertNode(t.Context(), store.Node{
		ID: nodeID, Status: "HEALTHY", FeatureIDs: []string{"feature.x"},
	}); err != nil {
		t.Fatal(err)
	}
	a := &API{store: st}

	call := func(id identity) map[string]any {
		req := httptest.NewRequest(http.MethodGet, "/v1/capabilities", nil)
		req = req.WithContext(withIdentity(req.Context(), id))
		rec := httptest.NewRecorder()
		a.listCapabilities(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d body=%q", rec.Code, rec.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	entryNodeIDs := func(out map[string]any) []any {
		caps, _ := out["capabilities"].([]any)
		for _, c := range caps {
			m, _ := c.(map[string]any)
			if m["feature_id"] == "feature.x" {
				ids, _ := m["node_ids"].([]any)
				return ids
			}
		}
		return nil
	}

	scopedOut := call(identity{Kind: "key", KeyID: "k", ProjectID: "dev", Scopes: []string{"admin"}})
	if ids := entryNodeIDs(scopedOut); len(ids) != 0 {
		t.Fatalf("scoped identity must not see node_ids, got %v", ids)
	}
	// 共享 lab 库里可能有其它节点，断言必须针对本测试的 node，而不是全局计数。
	globalIDs := entryNodeIDs(call(identity{Kind: "root", Scopes: []string{"admin"}}))
	found := false
	for _, v := range globalIDs {
		if s, _ := v.(string); s == nodeID {
			found = true
		}
	}
	if !found {
		t.Fatalf("global identity must see node %s in node_ids, got %v", nodeID, globalIDs)
	}
}

// uniqueSuffix 提供进程+纳秒级唯一后缀，避免与并行测试/共享 lab 残留冲突。
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(os.Getpid())
}
