// autoscale_test.go：ADR-0041 策略端点与手动接管（PG-gated 部分跳过）。
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

// 无 store 即可验证的 400 矩阵（校验先于任何 store 访问）。
func TestPutAutoscaleValidation400(t *testing.T) {
	a := &API{}
	bodies := []string{
		`{"enabled":true,"min_replicas":5,"max_replicas":3,"target_concurrency":20,"scale_down_delay_sec":120,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":0,"max_replicas":0,"target_concurrency":20,"scale_down_delay_sec":120,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":101,"target_concurrency":20,"scale_down_delay_sec":120,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":10,"target_concurrency":0,"scale_down_delay_sec":120,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":10,"target_concurrency":257,"scale_down_delay_sec":120,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":10,"target_concurrency":20,"scale_down_delay_sec":29,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":10,"target_concurrency":20,"scale_down_delay_sec":601,"panic_threshold":2.0}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":10,"target_concurrency":20,"scale_down_delay_sec":120,"panic_threshold":1.4}`,
		`{"enabled":true,"min_replicas":1,"max_replicas":10,"target_concurrency":20,"scale_down_delay_sec":120,"panic_threshold":5.1}`,
		`not-json`,
	}
	for i, b := range bodies {
		req := httptest.NewRequest("PUT", "/v1/apps/x/autoscale", strings.NewReader(b))
		req.SetPathValue("id", "x")
		rec := httptest.NewRecorder()
		a.putAutoscale(rec, req)
		if rec.Code != 400 {
			t.Errorf("body %d: status=%d, want 400 (%s)", i, rec.Code, rec.Body.String())
		}
	}
}

func newAutoscaleAPI(t *testing.T) *API {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run autoscale API tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	st := store.New(pool)
	project := "test-autoscale-api"
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id, name) VALUES($1,$2) ON CONFLICT (id) DO NOTHING`,
		project, project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM machines WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`,
			project,
		)
		_, _ = pool.Exec(context.Background(), `DELETE FROM user_events WHERE project_id=$1`, project)
		_, _ = pool.Exec(context.Background(), `DELETE FROM apps WHERE project_id=$1`, project)
		_, _ = pool.Exec(context.Background(), `DELETE FROM projects WHERE id=$1`, project)
	})
	return &API{store: st}
}

// TestAutoscalePutGetRoundTrip：PUT 全量替换 → GET 回读；不改 desired。
func TestAutoscalePutGetRoundTrip(t *testing.T) {
	a := newAutoscaleAPI(t)
	ctx := context.Background()
	st := a.store
	project, appID := "test-autoscale-api", "app-as-api-1"
	if err := st.EnsureApp(ctx, project, appID, "as-api.local", "img:v1", 1, 512, 80, 2); err != nil {
		t.Fatal(err)
	}
	put := `{"enabled":true,"min_replicas":1,"max_replicas":5,"target_concurrency":20,"scale_down_delay_sec":60,"panic_threshold":2.5}`
	req := httptest.NewRequest("PUT", "/v1/apps/"+appID+"/autoscale", strings.NewReader(put))
	req.SetPathValue("id", appID)
	rec := httptest.NewRecorder()
	a.putAutoscale(rec, req)
	if rec.Code != 200 {
		t.Fatalf("PUT status=%d (%s)", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest("GET", "/v1/apps/"+appID+"/autoscale", nil)
	req.SetPathValue("id", appID)
	rec = httptest.NewRecorder()
	a.getAutoscale(rec, req)
	if rec.Code != 200 {
		t.Fatalf("GET status=%d (%s)", rec.Code, rec.Body.String())
	}
	var got struct {
		AppID           string        `json:"app_id"`
		Autoscale       autoscaleBody `json:"autoscale"`
		DesiredReplicas int           `json:"desired_replicas"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Autoscale.Enabled || got.Autoscale.MaxReplicas != 5 || got.DesiredReplicas != 2 {
		t.Fatalf("GET = %+v, want enabled/max=5/desired=2", got)
	}
	// 未知 app → 404。
	req = httptest.NewRequest("GET", "/v1/apps/nope/autoscale", nil)
	req.SetPathValue("id", "nope")
	rec = httptest.NewRecorder()
	a.getAutoscale(rec, req)
	if rec.Code != 404 {
		t.Fatalf("GET missing app status=%d, want 404", rec.Code)
	}
}

// TestScaleTakeoverDisablesAutoscale：启用中 POST /scale 接管（desired+关
// enabled 一条 UPDATE），之后再次 scale 走普通路径；autoscaler CAS 撞不上接管值。
func TestScaleTakeoverDisablesAutoscale(t *testing.T) {
	a := newAutoscaleAPI(t)
	ctx := context.Background()
	st := a.store
	project, appID := "test-autoscale-api", "app-as-api-2"
	if err := st.EnsureApp(ctx, project, appID, "as-takeover.local", "img:v1", 1, 512, 80, 2); err != nil {
		t.Fatal(err)
	}
	if err := st.SetAutoscalePolicy(ctx, appID, store.AutoscalePolicy{
		Enabled: true, MinReplicas: 1, MaxReplicas: 5,
		TargetConcurrency: 20, ScaleDownDelaySec: 60, PanicThreshold: 2.0,
	}); err != nil {
		t.Fatal(err)
	}
	scale := func(n int) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest("POST", "/v1/apps/"+appID+"/scale",
			strings.NewReader(fmt.Sprintf(`{"replicas":%d}`, n)))
		req.SetPathValue("id", appID)
		rec := httptest.NewRecorder()
		a.scaleApp(rec, req)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}
	if code, body := scale(4); code != 202 || body["autoscale_enabled"] != false {
		t.Fatalf("takeover scale: code=%d body=%v", code, body)
	}
	app, _ := st.GetApp(ctx, appID)
	if app.DesiredReplicas != 4 || app.AutoscaleEnabled {
		t.Fatalf("after takeover: desired=%d enabled=%v", app.DesiredReplicas, app.AutoscaleEnabled)
	}
	// 接管后 autoscaler 的 CAS（expect=旧值）必须落空。
	if ok, _ := st.SetAppReplicasCAS(ctx, appID, 9, 2); ok {
		t.Fatal("CAS must miss after manual takeover")
	}
	// 再次手动 scale 同样走接管（幂等：仍关 enabled）。
	if code, body := scale(3); code != 202 || body["autoscale_enabled"] != false {
		t.Fatalf("second scale code=%d body=%v", code, body)
	}
	if app, _ := st.GetApp(ctx, appID); app.DesiredReplicas != 3 {
		t.Fatalf("desired=%d, want 3", app.DesiredReplicas)
	}
	if pol, _ := st.GetAutoscalePolicy(ctx, appID); pol.Enabled {
		t.Fatal("repeat manual scale must keep autoscale disabled (P1-2)")
	}
}
