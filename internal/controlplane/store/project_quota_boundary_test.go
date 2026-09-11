// project_quota_boundary_test.go：review 2026-09-10 对配额语义的回归锁定。
//
// 结论复核：ProjectUsage / ProjectMachineUsage 在 Place 被调用前就已包含本次
// 在途 create（EnsureAppAndEnqueueCreate 先落 machine+operation），因此
// placement 的边界判定是 `used > quota`，不能再加 requested。本测试锁定该
// 形状：pending 请求计入 usage，超限表现为 usage > quota。
package store

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestProjectUsageCountsPendingRequestAtQuotaBoundary(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-quota-boundary-" + suffix
	if err := s.EnsureProject(ctx, project, "quota-boundary"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	detail, err := s.GetProjectQuotaDetail(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	detail.VCPU = 3
	if _, err := s.UpdateProjectQuota(ctx, project, detail.Revision, *detail); err != nil {
		t.Fatal(err)
	}

	enqueue := func(n int, vcpu int64) {
		t.Helper()
		_, err := s.EnsureAppAndEnqueueCreate(ctx, project,
			fmt.Sprintf("app-qb%d", n), fmt.Sprintf("qb%d.local", n), "img:1",
			vcpu, 512, 1024, 80,
			fmt.Sprintf("m-qb%d-%s", n, suffix), fmt.Sprintf("dep-qb%d", n),
			fmt.Sprintf("exec-qb%d-%s", n, suffix), fmt.Sprintf("op-qb%d-%s", n, suffix),
			1, 0, []byte(`{"generation":"1"}`), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 两台共 3 vcpu = 恰好用满。第一次入队后 usage=2，第二次入队后 usage=3。
	enqueue(1, 2)
	if vcpu, _, _, err := s.ProjectUsage(ctx, project); err != nil || vcpu != 2 {
		t.Fatalf("usage after first enqueue = (%d, %v), want (2, nil)", vcpu, err)
	}
	enqueue(2, 1)
	vcpu, _, _, err := s.ProjectUsage(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if vcpu != 3 {
		t.Fatalf("usage after second enqueue = %d, want 3 (pending create must be counted)", vcpu)
	}
	quota, _, _, err := s.ProjectQuota(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if vcpu > quota {
		t.Fatalf("usage %d must not exceed quota %d at exact boundary", vcpu, quota)
	}

	// 第三台 1 vcpu：usage=4 > quota=3，Place 的 used > quota 必须拒绝。
	enqueue(3, 1)
	vcpu, _, _, err = s.ProjectUsage(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if vcpu != 4 || vcpu <= quota {
		t.Fatalf("usage = %d, quota = %d; want usage > quota after over-commit enqueue", vcpu, quota)
	}
}
