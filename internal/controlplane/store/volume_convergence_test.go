// volume_convergence_test.go：review 2026-09-10 的状态机收敛回归。
//
// 崩溃窗口：agent RPC 已成功、PG 终态已推进，但 CompleteOperation 失败/进程
// 崩溃 → operation 重试。旧实现下 MarkReady/SealDataset 的第二次调用必然
// 返回状态冲突，operation 无限 requeue。新实现必须幂等收敛，同时不得
// 放过真正的值冲突（不同 digest）。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestMarkVolumeReadyIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-vol-ready-" + suffix
	if err := s.EnsureProject(ctx, project, "vol-ready"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	vol, err := s.CreateVolume(ctx, Volume{
		ID: "v-ready-" + suffix, ProjectID: project, Name: "data",
		NodeID: "node-1", SizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if vol.State != "CREATING" {
		t.Fatalf("initial state = %s, want CREATING", vol.State)
	}
	// 首次推进 + 崩溃后重试都必须成功（幂等）。
	for i := 0; i < 2; i++ {
		if err := s.MarkVolumeReady(ctx, vol.ID); err != nil {
			t.Fatalf("mark ready attempt %d: %v", i+1, err)
		}
	}
	got, err := s.GetVolume(ctx, vol.ID)
	if err != nil || got == nil || got.State != "READY" {
		t.Fatalf("volume = %+v, err = %v; want READY", got, err)
	}
	// UNAVAILABLE（节点失联后恢复）同样收敛。
	if _, err := s.Pool().Exec(ctx, `UPDATE volumes SET state='UNAVAILABLE' WHERE id=$1`, vol.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkVolumeReady(ctx, vol.ID); err != nil {
		t.Fatalf("UNAVAILABLE -> READY: %v", err)
	}
	// 不存在/已删除：仍然冲突，不得静默成功。
	if err := s.MarkVolumeReady(ctx, "v-missing-"+suffix); !errors.Is(err, ErrVolumeStateConflict) {
		t.Fatalf("missing volume err = %v, want ErrVolumeStateConflict", err)
	}
}

func TestSealDatasetIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(os.Getpid())
	project := "p-dataset-seal-" + suffix
	if err := s.EnsureProject(ctx, project, "dataset-seal"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupProject(t, s, project) })

	volID := "v-dataset-" + suffix
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO volumes(id, project_id, name, mode, node_id, state, content_digest, import_status)
		VALUES($1,$2,'ds','DATASET_RO','node-1','CREATING',$3,'importing')`,
		volID, project, "sha256:aaa"); err != nil {
		t.Fatal(err)
	}

	// 首次导入 + 崩溃后重试（同 digest/size）都必须成功。
	for i := 0; i < 2; i++ {
		if err := s.SealDataset(ctx, volID, "sha256:aaa", 4096); err != nil {
			t.Fatalf("seal attempt %d: %v", i+1, err)
		}
	}
	got, err := s.GetVolume(ctx, volID)
	if err != nil || got == nil {
		t.Fatal(err)
	}
	if got.State != "READY" || got.ImportStatus != "sealed" || got.SizeBytes != 4096 {
		t.Fatalf("sealed volume = %+v", got)
	}
	// 不同 digest 重试：值冲突不得被幂等吞掉。
	if err := s.SealDataset(ctx, volID, "sha256:bbb", 4096); !errors.Is(err, ErrDatasetDigest) {
		t.Fatalf("digest mismatch err = %v, want ErrDatasetDigest", err)
	}
	// 不同 size 同样冲突。
	if err := s.SealDataset(ctx, volID, "sha256:aaa", 8192); !errors.Is(err, ErrDatasetDigest) {
		t.Fatalf("size mismatch err = %v, want ErrDatasetDigest", err)
	}
	// 不存在：冲突。
	if err := s.SealDataset(ctx, "v-missing-"+suffix, "sha256:aaa", 1); !errors.Is(err, ErrDatasetDigest) {
		t.Fatalf("missing dataset err = %v, want ErrDatasetDigest", err)
	}
}
