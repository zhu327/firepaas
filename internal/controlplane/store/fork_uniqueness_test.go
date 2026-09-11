// fork_uniqueness_test.go：review 2026-09-10 的 schema/投影回归。
//
//   - migration 0037 把 machine 唯一键放宽为“正式副本槽位”（replica_ordinal >= 0），
//     debug fork（ordinal 为 -1、deployment 为空）不再互相冲突，正式副本唯一性保持不变；
//   - ActiveRouteMachines 不返回 hostname 为空的 fork 机器（无公开 route 语义）。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestForkMachineSlotsDoNotCollide(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-fork-uniq-" + suffix
	appID := "app-fork-uniq-" + suffix
	if err := s.EnsureProject(ctx, project, "fork-uniq"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref)
		VALUES($1,$2,$3,'img:1') ON CONFLICT (id) DO NOTHING`,
		appID, project, "fork-uniq-"+suffix+".local"); err != nil {
		t.Fatal(err)
	}

	insertMachine := func(id string, ordinal int, deployment string) error {
		_, err := s.Pool().Exec(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref)
			VALUES($1,$2,$3,$4,'','img:1')`, id, appID, deployment, ordinal)
		return err
	}

	// 两次 fork（ordinal=-1, deployment 为空）必须共存：这是修复前的 23505 场景。
	if err := insertMachine("m-fork-a-"+suffix, -1, ""); err != nil {
		t.Fatalf("first fork machine must insert: %v", err)
	}
	if err := insertMachine("m-fork-b-"+suffix, -1, ""); err != nil {
		t.Fatalf("second fork machine must insert (partial index): %v", err)
	}

	// 正式副本唯一性不变：同 (app, ordinal>=0, deployment) 第二行必须冲突。
	if err := insertMachine("m-replica-a-"+suffix, 0, "dep-"+suffix); err != nil {
		t.Fatal(err)
	}
	err := insertMachine("m-replica-b-"+suffix, 0, "dep-"+suffix)
	if err == nil {
		t.Fatal("duplicate formal replica slot must violate the partial unique index")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("want 23505 unique violation, got %v", err)
	}
}

func TestActiveRouteMachinesExcludesEmptyHostname(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-fork-route-" + suffix
	appID := "app-fork-route-" + suffix
	if err := s.EnsureProject(ctx, project, "fork-route"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO apps(id, project_id, hostname, image_ref)
		VALUES($1,$2,$3,'img:1') ON CONFLICT (id) DO NOTHING`,
		appID, project, "fork-route-"+suffix+".local"); err != nil {
		t.Fatal(err)
	}
	insert := func(id, hostname string) {
		t.Helper()
		if _, err := s.Pool().Exec(ctx, `
			INSERT INTO machines(id, app_id, deployment_id, replica_ordinal, hostname, image_ref,
				desired_state, current_execution_id, observed_state)
			VALUES($1,$2,'',-1,$3,'img:1','RUNNING',$4,'RUNNING')`,
			id, appID, hostname, "exec-"+id); err != nil {
			t.Fatal(err)
		}
	}
	forkID := "m-fork-route-" + suffix
	routedID := "m-routed-route-" + suffix
	insert(forkID, "") // debug fork：无公开 route
	insert(routedID, "routed-"+suffix+".local")

	rows, err := s.ActiveRouteMachines(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, m := range rows {
		seen[m.ID] = true
	}
	if seen[forkID] {
		t.Fatal("hostname='' fork machine must not be published as a route backend")
	}
	if !seen[routedID] {
		t.Fatalf("routed machine missing from ActiveRouteMachines: %v", rows)
	}
	// 防御：返回行里不应再有空 hostname。
	for _, m := range rows {
		if strings.TrimSpace(m.Hostname) == "" {
			t.Fatalf("empty hostname leaked into route projection: %+v", m)
		}
	}
}
