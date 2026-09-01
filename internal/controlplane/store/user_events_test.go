package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// v1.2-F（v1.2-plan §9）：user_events 记录、过滤、keyset 游标与保留期。
func TestUserEventsLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-ue-%d", os.Getpid())
	defer cleanupProject(t, s, project)

	for i := 0; i < 5; i++ {
		if err := s.RecordUserEvent(ctx, UserEvent{
			ProjectID: project, AppID: "app-a", MachineID: fmt.Sprintf("m-%d", i),
			Type:    UserEventMachineCreated,
			Details: []byte(fmt.Sprintf(`{"generation":%d}`, i+1)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 过滤：type。
	evs, err := s.ListUserEvents(ctx, UserEventFilter{ProjectID: project, Type: UserEventMachineCreated, Limit: 10})
	if err != nil || len(evs) != 5 {
		t.Fatalf("type filter: %d events, err=%v", len(evs), err)
	}
	// machine 过滤。
	evs, err = s.ListUserEvents(ctx, UserEventFilter{ProjectID: project, MachineID: "m-3"})
	if err != nil || len(evs) != 1 {
		t.Fatalf("machine filter: %d events, err=%v", len(evs), err)
	}
	// 游标：取前 2 条的 next_before，翻页后拿余下 3 条。
	evs, err = s.ListUserEvents(ctx, UserEventFilter{ProjectID: project, Limit: 2})
	if err != nil || len(evs) != 2 {
		t.Fatalf("page1: %d, err=%v", len(evs), err)
	}
	cursor := evs[len(evs)-1].ID
	evs2, err := s.ListUserEvents(ctx, UserEventFilter{ProjectID: project, Before: cursor, Limit: 10})
	if err != nil || len(evs2) != 3 {
		t.Fatalf("page2: %d, err=%v", len(evs2), err)
	}
	// 跨项目隔离。
	other, _ := s.ListUserEvents(ctx, UserEventFilter{ProjectID: project + "-other"})
	if len(other) != 0 {
		t.Fatalf("cross-project leak: %d", len(other))
	}
	// 保留期：清理“1 小时前”（未来时间戳 → 全删）。
	n, err := s.DeleteUserEventsOlderThan(ctx, time.Now().Add(time.Hour))
	if err != nil || n == 0 {
		t.Fatalf("retention delete: n=%d err=%v", n, err)
	}
	// 空属性拒绝。
	if err := s.RecordUserEvent(ctx, UserEvent{ProjectID: project}); err == nil {
		t.Fatal("event without type must be rejected")
	}
}

// v1.2-F：GC roots——active deployment + 在途 rollout + 在途 create。
func TestGCRootImages(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-gc-%d", os.Getpid())
	defer cleanupProject(t, s, project)
	if err := s.EnsureProject(ctx, project, "gc-test"); err != nil {
		t.Fatal(err)
	}
	seedMachine(t, s, project, "app-gc", "m-gc-1", "dep-gc-1", "exec-gc-1")
	// active deployment 引用 img-gc-active。
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO deployments(id, app_id, generation, image_ref, vcpu, mem_mib, port, status)
		VALUES('dep-gc-1', 'app-gc', 1, 'img-gc-active', 1, 512, 8080, 'ACTIVE')
		ON CONFLICT (id) DO UPDATE SET image_ref='img-gc-active', status='ACTIVE'`); err != nil {
		t.Fatal(err)
	}
	roots, err := s.GCRootImages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range roots {
		if r == "img-gc-active" {
			found = true
		}
	}
	if !found {
		t.Fatalf("active deployment image must be a root, got %v", roots)
	}
}
