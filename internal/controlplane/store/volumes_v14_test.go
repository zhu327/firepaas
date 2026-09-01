// volumes_v14_test.go：v1.4-D dataset/LOCAL_RW 回归——同项目 DATASET_RO 多
// 只读挂载、删除引用保护、半成品（未 seal）不可挂载。
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func seedVolume(t *testing.T, s *Store, project, volID, mode, state string) {
	t.Helper()
	digest := ""
	if mode == "DATASET_RO" {
		digest = "sha256:" + repeatHex(5, 64)
	}
	if _, err := s.pool.Exec(context.Background(), `
		INSERT INTO volumes(id,project_id,name,mode,node_id,size_bytes,state,content_digest,import_status)
		VALUES($1,$2,$3,$4,'node-v14d',1048576,$5,$6,CASE WHEN $4='DATASET_RO' AND $5='READY' THEN 'sealed' ELSE '' END)`,
		volID, project, volID, mode, state, digest); err != nil {
		t.Fatal(err)
	}
}

func seedV14Machine(t *testing.T, s *Store, project, machineID, executionID string, ordinal int) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `
		INSERT INTO apps(id, project_id, hostname, image_ref, vcpu, mem_mib, desired_replicas, generation)
		VALUES('app-v14d',$1,'v14d.local','img',1,512,1,1) ON CONFLICT (id) DO NOTHING`, project); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(context.Background(), `
		INSERT INTO machines(id,app_id,deployment_id,replica_ordinal,hostname,desired_state,generation,
			current_execution_id,requested_vcpu,requested_mem_mib,requested_disk_mib,image_ref,node_id)
		VALUES($1,'app-v14d','',$2,'','CREATED',1,$3,1,512,10240,'','node-v14d')`,
		machineID, ordinal, executionID); err != nil {
		t.Fatal(err)
	}
}

// v1.4-B：MISSING/UNAVAILABLE 墓碑可回收——完整 inventory 证明产物不存在的
// volume 必须允许删除（否则永久 wedge）；agent 删除对 NotFound 幂等。
func TestDeleteUnavailableVolumeConverges(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-vol-del-unavail-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "vol-del-unavail-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	volID := "vol-del-unavail"
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO volumes(id,project_id,name,mode,node_id,size_bytes,state,integrity)
		VALUES($1,$2,$3,'LOCAL_RW','node-v14d',1048576,'UNAVAILABLE','MISSING')`, volID, project, volID); err != nil {
		t.Fatal(err)
	}
	opID := "op-vol-del-unavail"
	if _, err := s.BeginVolumeDeleteAndEnqueue(ctx, volID, EnqueueOperationParams{
		OperationID: opID, ProjectID: project, Kind: "volume_delete",
		Request:        []byte(fmt.Sprintf(`{"volume_id":%q,"operation_id":%q}`, volID, opID)),
		DispatchNodeID: "node-v14d",
	}); err != nil {
		t.Fatalf("delete UNAVAILABLE/MISSING volume: %v", err)
	}
	if err := s.TransitionVolume(ctx, volID, "DELETING", "DELETED"); err != nil {
		t.Fatalf("transition to DELETED: %v", err)
	}
	v, err := s.GetVolume(ctx, volID)
	if err != nil || v.State != "DELETED" {
		t.Fatalf("volume state = %q err=%v, want DELETED", v.State, err)
	}
}

func TestBeginVolumeDeleteReEnqueuesAfterTerminalFailure(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-vol-del-retry-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "vol-delete-retry"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedVolume(t, s, project, "vol-del-retry", "LOCAL_RW", "READY")
	p := EnqueueOperationParams{
		OperationID: "op-vol-del-retry", ProjectID: project, Kind: "volume_delete",
		Request: []byte(`{"volume_id":"vol-del-retry"}`), DispatchNodeID: "node-v14d",
	}
	first, err := s.BeginVolumeDeleteAndEnqueue(ctx, "vol-del-retry", p)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOperation(ctx, first.ID, "FAILED", nil, "failed"); err != nil {
		t.Fatal(err)
	}
	retry, err := s.BeginVolumeDeleteAndEnqueue(ctx, "vol-del-retry", p)
	if err != nil || retry.ID == first.ID || retry.Status != "PENDING" {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
	again, err := s.BeginVolumeDeleteAndEnqueue(ctx, "vol-del-retry", p)
	if err != nil || again.ID != retry.ID {
		t.Fatalf("in-flight retry=%+v err=%v", again, err)
	}
}

func TestFailedDatasetImportIsDeletable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-ds-failed-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "dataset-failed"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM operations WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedVolume(t, s, project, "vol-ds-failed", "DATASET_RO", "CREATING")
	if err := s.FailDatasetImport(ctx, "vol-ds-failed"); err != nil {
		t.Fatal(err)
	}
	v, err := s.GetVolume(ctx, "vol-ds-failed")
	if err != nil || v.State != "CREATING" || v.ImportStatus != "failed" {
		t.Fatalf("failed dataset=%+v err=%v", v, err)
	}
	if _, err := s.BeginVolumeDeleteAndEnqueue(ctx, v.ID, EnqueueOperationParams{
		OperationID: "op-del-ds-failed", ProjectID: project, Kind: "volume_delete",
		Request: []byte(`{}`), DispatchNodeID: "node-v14d",
	}); err != nil {
		t.Fatalf("delete failed dataset: %v", err)
	}
}

func TestReleaseTerminalExecutionAttachments(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-att-release-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "attachment-release"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM volume_attachments WHERE volume_id='vol-att-release'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedVolume(t, s, project, "vol-att-release", "LOCAL_RW", "READY")
	if err := s.UpsertVolumeAttachment(ctx, VolumeAttachment{VolumeID: "vol-att-release", MachineID: "m-old", ExecutionID: "e-old", Status: "ATTACHED"}); err != nil {
		t.Fatal(err)
	}
	n, err := s.ReleaseTerminalExecutionAttachments(ctx, "m-old", "e-old")
	if err != nil || n != 1 {
		t.Fatalf("released=%d err=%v", n, err)
	}
	active, err := s.ActiveAttachments(ctx, "vol-att-release")
	if err != nil || len(active) != 0 {
		t.Fatalf("active=%v err=%v", active, err)
	}
}

// v1.4-D：同一 DATASET_RO base 可被同 project 多个 execution 只读挂载。
func TestDatasetMultiAttachAllowed(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-ds-multi-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "ds-multi-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM volume_attachments WHERE volume_id LIKE 'vol-dsm%'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM machines WHERE app_id='app-v14d'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM apps WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedV14Machine(t, s, project, "m-ds-1", "exec-ds-1", 1)
	seedV14Machine(t, s, project, "m-ds-2", "exec-ds-2", 2)
	seedVolume(t, s, project, "vol-dsm", "DATASET_RO", "READY")

	for i, m := range []string{"m-ds-1", "m-ds-2"} {
		att := VolumeAttachment{
			VolumeID: "vol-dsm", MachineID: m,
			ExecutionID: "exec-ds-" + fmt.Sprint(i+1), MountPath: "/data", Readonly: true,
		}
		p := EnqueueOperationParams{
			OperationID: fmt.Sprintf("op-dsm-%d", i+1), ProjectID: project,
			MachineID: m, ExecutionID: att.ExecutionID, Generation: 1,
			Kind: "volume_attach", Request: []byte(`{}`), DispatchNodeID: "node-v14d",
		}
		if _, err := s.ClaimDatasetAttachmentAndEnqueue(ctx, att, p); err != nil {
			t.Fatalf("readonly attach %d: %v", i+1, err)
		}
	}
	atts, err := s.ActiveAttachments(ctx, "vol-dsm")
	if err != nil || len(atts) != 2 {
		t.Fatalf("active attachments = %d err=%v, want 2", len(atts), err)
	}
	// 删除被 active attachment 阻塞。
	if _, err := s.BeginVolumeDeleteAndEnqueue(ctx, "vol-dsm", EnqueueOperationParams{
		OperationID: "op-dsm-del", ProjectID: project, Kind: "volume_delete",
		Request: []byte(`{}`), DispatchNodeID: "node-v14d",
	}); !errors.Is(err, ErrVolumeStateConflict) {
		t.Fatalf("delete with active attachments = %v, want conflict", err)
	}
}

// v1.4-D：未 seal（CREATING/importing）的 DATASET_RO 不可挂载；digest 验证
// 失败后 volume 停在 CREATING，重试不会发布半成品。
func TestUnsealedDatasetCannotAttach(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	project := fmt.Sprintf("p-ds-seal-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "ds-seal-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM volume_attachments WHERE volume_id='vol-dss'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM machines WHERE app_id='app-v14d'`)
		_, _ = s.pool.Exec(ctx, `DELETE FROM apps WHERE project_id=$1`, project)
		_, _ = s.pool.Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedV14Machine(t, s, project, "m-dss", "exec-dss", 3)
	seedVolume(t, s, project, "vol-dss", "DATASET_RO", "CREATING")

	att := VolumeAttachment{
		VolumeID: "vol-dss", MachineID: "m-dss", ExecutionID: "exec-dss",
		MountPath: "/data", Readonly: true,
	}
	p := EnqueueOperationParams{
		OperationID: "op-dss-1", ProjectID: project, MachineID: "m-dss",
		ExecutionID: "exec-dss", Generation: 1, Kind: "volume_attach",
		Request: []byte(`{}`), DispatchNodeID: "node-v14d",
	}
	if _, err := s.ClaimDatasetAttachmentAndEnqueue(ctx, att, p); !errors.Is(err, ErrVolumeStateConflict) {
		t.Fatalf("attach unsealed dataset = %v, want conflict", err)
	}
	// 导入 digest 不匹配 → SealDataset 拒绝，volume 保持 CREATING。
	if err := s.SealDataset(ctx, "vol-dss", "sha256:"+repeatHex(9, 64), 1024); !errors.Is(err, ErrDatasetDigest) {
		t.Fatalf("seal with wrong digest = %v, want ErrDatasetDigest", err)
	}
	v, err := s.GetVolume(ctx, "vol-dss")
	if err != nil || v.State != "CREATING" {
		t.Fatalf("volume state after failed seal = %q err=%v", v.State, err)
	}
	// 正确 digest 才能发布 READY。
	if err := s.SealDataset(ctx, "vol-dss", "sha256:"+repeatHex(5, 64), 1024); err != nil {
		t.Fatalf("seal with authorized digest: %v", err)
	}
	if v, _ := s.GetVolume(ctx, "vol-dss"); v.State != "READY" || v.ImportStatus != "sealed" {
		t.Fatalf("sealed volume = %+v", v)
	}
	// overlay 尺寸进入项目磁盘配额记账（reserveVolumeQuota 路径），负值拒绝。
	att.OverlaySizeBytes = -1
	if _, err := s.ClaimDatasetAttachmentAndEnqueue(ctx, att, p); err == nil {
		t.Fatal("negative overlay size must be rejected")
	}
	_ = time.Now()
}
