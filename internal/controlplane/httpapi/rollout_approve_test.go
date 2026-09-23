package httpapi

// rollout_approve_test.go：Wave3 Stage B 放行端点（PG-gated；无 PG 时跳过）。
import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

func newApproveAPI(t *testing.T) (*API, context.Context) {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run approve API tests")
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
	project := "test-approve-api"
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id, name) VALUES($1,$2) ON CONFLICT (id) DO NOTHING`,
		project, project); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM machines WHERE app_id IN (SELECT id FROM apps WHERE project_id=$1)`, project)
		_, _ = pool.Exec(context.Background(), `DELETE FROM user_events WHERE project_id=$1`, project)
		_, _ = pool.Exec(context.Background(), `DELETE FROM apps WHERE project_id=$1`, project)
		_, _ = pool.Exec(context.Background(), `DELETE FROM projects WHERE id=$1`, project)
	})
	return &API{store: store.New(pool)}, ctx
}

func TestApproveRolloutEndpoint(t *testing.T) {
	a, ctx := newApproveAPI(t)
	st := a.store
	project, appID := "test-approve-api", "app-approve-api"
	if err := st.EnsureApp(ctx, project, appID, "approve-api.local", "img:v1", 1, 512, 80, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRollout(ctx, store.Rollout{
		ID: "rl-approve-api", AppID: appID, FromGeneration: 1, ToGeneration: 2, ApprovalRequired: true,
	}); err != nil {
		t.Fatal(err)
	}
	approve := func(id string) int {
		req := httptest.NewRequest("POST", "/v1/rollouts/"+id+"/approve", strings.NewReader(""))
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		a.approveRollout(rec, req)
		return rec.Code
	}
	// PAUSED 前放行 → 409。
	if code := approve("rl-approve-api"); code != 409 {
		t.Fatalf("approve before pause = %d, want 409", code)
	}
	if err := st.RolloutToPaused(ctx, appID); err != nil {
		t.Fatal(err)
	}
	// 放行 → 202。
	if code := approve("rl-approve-api"); code != 202 {
		t.Fatalf("approve = %d, want 202", code)
	}
	// 重复放行 → 409。
	if code := approve("rl-approve-api"); code != 409 {
		t.Fatalf("re-approve = %d, want 409", code)
	}
	// 不存在的 rollout → 404。
	if code := approve("nope"); code != 404 {
		t.Fatalf("missing rollout = %d, want 404", code)
	}
}
