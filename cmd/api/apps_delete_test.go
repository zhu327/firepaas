// apps_delete_test.go：R2 评审——DELETE /v1/apps/{id} 的原子性与重复删除
// 收敛（需要真实 PG；FIREPAAS_TEST_POSTGRES 未设时跳过）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhu327/firepaas/internal/controlplane/db"
	"github.com/zhu327/firepaas/internal/controlplane/store"
)

type deleteAppResp struct {
	AppID            string `json:"app_id"`
	MachinesToDelete int    `json:"machines_to_delete"`
	AlreadyDeleted   bool   `json:"already_deleted"`
}

func newDeleteAppAPI(t *testing.T) (*API, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run deleteApp integration test")
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
	return &API{store: store.New(pool)}, pool
}

func seedDeleteable(t *testing.T, pool *pgxpool.Pool, project, appID string, pairs [][2]string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id, name) VALUES($1,$2) ON CONFLICT (id) DO NOTHING`,
		project, project); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
		VALUES($1,$2,$3,'img',1,512,1,1)`, appID, project, appID+".test"); err != nil {
		t.Fatal(err)
	}
	for i, p := range pairs {
		if _, err := pool.Exec(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname,
				desired_state, generation, current_execution_id, requested_vcpu,
				requested_mem_mib, image_ref, node_id, ingress_port)
			VALUES($1,$2,'dep', $3,$4,'CREATED',1,$5,1,512,'img','n-x',8080)`,
			p[0], appID, i, p[0]+".test", p[1]); err != nil {
			t.Fatal(err)
		}
	}
}

func callDeleteApp(t *testing.T, a *API, appID string) (int, deleteAppResp) {
	t.Helper()
	req := httptest.NewRequest("DELETE", "/v1/apps/"+appID, nil)
	req.SetPathValue("id", appID)
	rec := httptest.NewRecorder()
	a.deleteApp(rec, req)
	var body deleteAppResp
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

// 完整收敛闭环：首次 DELETE 原子提交 → 人为恢复 MBEDTLS“入队一半”残留 →
// 重试 DELETE 重新补发，最终 2 台都有 delete op。
func TestDeleteAppAtomicAndRetryConverges(t *testing.T) {
	a, pool := newDeleteAppAPI(t)
	ctx := context.Background()
	sfx := fmt.Sprint(os.Getpid())
	project := "p-delapi-" + sfx
	appID := "app-delapi-" + sfx
	m1, m2 := "m1-delapi-"+sfx, "m2-delapi-"+sfx
	seedDeleteable(t, pool, project, appID,
		[][2]string{{m1, "e1-" + sfx}, {m2, "e2-" + sfx}})

	code, first := callDeleteApp(t, a, appID)
	if code != 202 {
		t.Fatalf("first delete status = %d, want 202", code)
	}
	if first.AlreadyDeleted || first.MachinesToDelete != 2 {
		t.Fatalf("first delete body = %+v, want {deletes:2 already:false}", first)
	}
	var appDeleted bool
	if err := pool.QueryRow(ctx,
		`SELECT deleted_at IS NOT NULL FROM apps WHERE id=$1`, appID).Scan(&appDeleted); err != nil {
		t.Fatal(err)
	}
	if !appDeleted {
		t.Fatal("app must be tombstoned after first DELETE")
	}
	opsFor := func(machineID string) int {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM operations WHERE project_id=$1 AND machine_id=$2 AND kind='delete'`,
			project, machineID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if opsFor(m1) != 1 || opsFor(m2) != 1 {
		t.Fatalf("both machines must have a delete op after one tx")
	}

	// 模拟中途崩溃留下的“墓碑已提交、只入了 m1 的 delete”残余，
	// 验证幂等重试重新补发 m2。
	if _, err := pool.Exec(ctx,
		`DELETE FROM operations WHERE project_id=$1 AND machine_id=$2 AND kind='delete'`,
		project, m2); err != nil {
		t.Fatal(err)
	}
	code, retry := callDeleteApp(t, a, appID)
	if code != 202 || !retry.AlreadyDeleted {
		t.Fatalf("retry delete = %d %+v, want 202 already_deleted", code, retry)
	}
	if retry.MachinesToDelete != 2 {
		t.Fatalf("retry must report 2 still-unconverged machines, got %d", retry.MachinesToDelete)
	}
	if opsFor(m2) != 1 {
		t.Fatal("retry must re-enqueue m2 delete with the same idempotency key")
	}
	if opsFor(m1) != 1 {
		t.Fatal("retry must not duplicate m1 delete")
	}

	// 重复幂等：第三次调用不再产生新行。
	code, third := callDeleteApp(t, a, appID)
	if code != 202 || !third.AlreadyDeleted {
		t.Fatalf("third delete = %d %+v", code, third)
	}
	if opsFor(m1) != 1 || opsFor(m2) != 1 {
		t.Fatal("repeated DELETE must remain idempotent")
	}

	// 404：不存在的 app。
	code, _ = callDeleteApp(t, a, "app-missing"+sfx)
	if code != 404 {
		t.Fatalf("missing app status = %d, want 404", code)
	}
}

// deleteMachine：显式 execution_id 与 machine 当前值不符 → 409（R2 评审）。
func TestDeleteMachineExecutionMismatch409(t *testing.T) {
	a, pool := newDeleteAppAPI(t)
	sfx := fmt.Sprint(os.Getpid()) + "-dm"
	project := "p-delvm-" + sfx
	appID := "app-delvm-" + sfx
	m1 := "m1-delvm-" + sfx
	seedDeleteable(t, pool, project, appID, [][2]string{{m1, "e-current-" + sfx}})

	call := func(query string) int {
		req := httptest.NewRequest("DELETE", "/v1/machines/"+m1+query, nil)
		req.SetPathValue("id", m1)
		rec := httptest.NewRecorder()
		a.deleteMachine(rec, req)
		return rec.Code
	}
	if code := call("?operation_id=op-dm-" + sfx + "&execution_id=e-stale"); code != 409 {
		t.Fatalf("stale execution_id status = %d, want 409", code)
	}
	// 不带 execution_id 默认作用于 machine 的当前执行，202。
	if code := call("?operation_id=op-dm-" + sfx); code != 202 {
		t.Fatalf("delete with omitted execution_id status = %d, want 202", code)
	}
	// 显式带当前值同样 202。
	if code := call("?operation_id=op-dm2-" + sfx + "&execution_id=e-current-" + sfx); code != 202 {
		t.Fatalf("delete with matching execution_id status = %d, want 202", code)
	}
}
