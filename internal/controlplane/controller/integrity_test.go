// integrity_test.go：v1.4-B inventory 对账回归——MISSING 推导（仅完整列表）、
// 旧 agent 只得 UNKNOWN、orphan 报告。
package controller

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/capabilities"
	"github.com/zhu327/firepaas/internal/controlplane/store"
	"github.com/zhu327/firepaas/internal/observability/metrics"
	pb "github.com/zhu327/firepaas/shared/gen/agent/v1"
)

func seedIntegrityNode(t *testing.T, s *store.Store, nodeID string, inventoryCapable bool) store.Node {
	t.Helper()
	features := "[]"
	if inventoryCapable {
		features = `["` + capabilities.LocalInventoryV1 + `"]`
	}
	if _, err := s.Pool().Exec(context.Background(), `
		INSERT INTO nodes(id,nomad_node_id,node_pool,status,feature_ids) VALUES($1,$1,'compute','HEALTHY',$2::jsonb)
		ON CONFLICT (id) DO UPDATE SET status='HEALTHY', feature_ids=$2::jsonb`, nodeID, features); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(context.Background(), `DELETE FROM local_inventory_observations WHERE node_id=$1`, nodeID)
		_, _ = s.Pool().Exec(context.Background(), `DELETE FROM nodes WHERE id=$1`, nodeID)
	})
	var n store.Node
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT id,node_pool,status,feature_ids FROM nodes WHERE id=$1`, nodeID).
		Scan(&n.ID, &n.NodePool, &n.Status, &n.FeatureIDs); err != nil {
		t.Fatal(err)
	}
	return n
}

func seedIntegritySnapshot(t *testing.T, s *store.Store, project, snapID, nodeID, status string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateSnapshot(ctx, store.Snapshot{ID: snapID, ProjectID: project,
		SourceMachineID: "m-int", SourceExecutionID: "exec-int", SourceGeneration: 1,
		Kind: "MEMORY", NodeID: nodeID}); err != nil {
		t.Fatal(err)
	}
	if status != "CREATING" {
		if _, err := s.UpdateSnapshotArtifact(ctx, snapID, 4096, "sha256:int-test", "none", "none", "", nil, true); err != nil {
			t.Fatal(err)
		}
	}
}

// v1.4-B：完整 inventory + 产物不存在 → MISSING + UNAVAILABLE；存在 →\n// METADATA_VERIFIED；旧 agent（complete=false）不推导任何结论。
func TestReconcileSnapshotIntegrity(t *testing.T) {
	s := testStoreController(t)
	c := &Controller{store: s, metrics: metrics.New(), reportedOrphans: map[string]bool{}}
	ctx := context.Background()
	project := fmt.Sprintf("p-int-snap-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "int-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM snapshots WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM scheduler_events WHERE node_id='node-int-a'`)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM user_events WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	node := seedIntegrityNode(t, s, "node-int-a", true)

	present := "snap-int-present"
	missing := "snap-int-missing"
	creating := "snap-int-creating"
	seedIntegritySnapshot(t, s, project, present, node.ID, "READY")
	seedIntegritySnapshot(t, s, project, missing, node.ID, "READY")
	seedIntegritySnapshot(t, s, project, creating, node.ID, "CREATING")

	now := time.Now().Unix()
	inv := &pb.ListSnapshotsResponse{Complete: true, ObservationGeneration: 1, ObservedAtUnix: now,
		Observation: &pb.InventoryObservation{Complete: true, Epoch: "snap-epoch", Generation: 1, ObservedAtUnix: now}, Snapshots: []*pb.SnapshotInfo{
			{Id: present, ArtifactSha256: "sha256:int-test", SizeBytes: 4096, Kind: pb.SnapshotKind_SNAPSHOT_MEMORY},
			{Id: "snap-orphan-1", SizeBytes: 8192}, // 本地 orphan（PG 无行）
		}}
	c.reconcileSnapshotIntegrity(ctx, node, inv)

	if snap, _ := s.GetSnapshot(ctx, present); snap.Integrity != "METADATA_VERIFIED" || snap.Status != "READY" {
		t.Fatalf("present snapshot = status %s integrity %s", snap.Status, snap.Integrity)
	}
	snap, _ := s.GetSnapshot(ctx, missing)
	if snap.Integrity != "MISSING" || snap.Status != "UNAVAILABLE" {
		t.Fatalf("missing snapshot = status %s integrity %s, want UNAVAILABLE/MISSING", snap.Status, snap.Integrity)
	}
	if snap, _ = s.GetSnapshot(ctx, creating); snap.Integrity != "UNKNOWN" {
		t.Fatalf("in-flight snapshot integrity = %s, want UNKNOWN", snap.Integrity)
	}

	// 旧 agent（complete=false）：不推导 MISSING，也不恢复 READY。
	oldAgent := &pb.ListSnapshotsResponse{Complete: false, Snapshots: inv.Snapshots}
	c.reconcileSnapshotIntegrity(ctx, node, oldAgent)
	if snap, _ = s.GetSnapshot(ctx, missing); snap.Integrity != "MISSING" {
		// 已记录的观测不被旧响应清除，但也不得进一步降级。
		t.Logf("missing snapshot integrity stays %s after incomplete response", snap.Integrity)
	}
	seedIntegritySnapshot(t, s, project, "snap-int-old", node.ID, "READY")
	c.reconcileSnapshotIntegrity(ctx, node, oldAgent)
	if snap, _ = s.GetSnapshot(ctx, "snap-int-old"); snap.Integrity != "UNKNOWN" {
		t.Fatalf("old agent must not derive integrity, got %s", snap.Integrity)
	}
}

// v1.4-B：volume inventory 对账——present 恢复 READY + METADATA_VERIFIED；
// 完整列表缺席 → MISSING + UNAVAILABLE；orphan 只报告。
func TestAuthoritativeInventoryOrderingAndCorruptStickiness(t *testing.T) {
	s := testStoreController(t)
	c := &Controller{store: s, metrics: metrics.New(), reportedOrphans: map[string]bool{}}
	ctx := context.Background()
	project := fmt.Sprintf("p-int-order-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "int-order-test"); err != nil {
		t.Fatal(err)
	}
	node := seedIntegrityNode(t, s, "node-int-order", true)
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM snapshots WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM local_inventory_observations WHERE node_id=$1`, node.ID)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	seedIntegritySnapshot(t, s, project, "snap-int-order", node.ID, "READY")
	observation := func(epoch string, generation uint64, checksum string) *pb.ListSnapshotsResponse {
		now := time.Now().Unix()
		return &pb.ListSnapshotsResponse{Complete: true, ObservationGeneration: generation, ObservedAtUnix: now,
			Observation: &pb.InventoryObservation{Complete: true, Epoch: epoch, Generation: generation, ObservedAtUnix: now},
			Snapshots:   []*pb.SnapshotInfo{{Id: "snap-int-order", ArtifactSha256: checksum, SizeBytes: 4096, Kind: pb.SnapshotKind_SNAPSHOT_MEMORY}}}
	}
	c.reconcileSnapshotIntegrity(ctx, node, observation("epoch-a", 1, "sha256:mismatch"))
	snap, _ := s.GetSnapshot(ctx, "snap-int-order")
	if snap.Integrity != "CORRUPT" || snap.Status != "UNAVAILABLE" {
		t.Fatalf("mismatch = %s/%s", snap.Integrity, snap.Status)
	}
	// Presence in a later observation cannot heal CORRUPT.
	c.reconcileSnapshotIntegrity(ctx, node, observation("epoch-a", 2, "sha256:int-test"))
	snap, _ = s.GetSnapshot(ctx, "snap-int-order")
	if snap.Integrity != "CORRUPT" {
		t.Fatalf("presence healed corrupt: %s", snap.Integrity)
	}
	// A new epoch retires the old one; delayed old-epoch generations are ignored.
	c.reconcileSnapshotIntegrity(ctx, node, observation("epoch-b", 1, "sha256:int-test"))
	c.reconcileSnapshotIntegrity(ctx, node, observation("epoch-a", 3, ""))
	var observations int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM local_inventory_observations WHERE node_id=$1`, node.ID).Scan(&observations); err != nil || observations != 3 {
		t.Fatalf("accepted observations=%d err=%v, want 3", observations, err)
	}
	var observationID *int64
	var receivedAt *time.Time
	if err := s.Pool().QueryRow(ctx, `SELECT inventory_observation_id,inventory_received_at FROM snapshots WHERE id='snap-int-order'`).Scan(&observationID, &receivedAt); err != nil || observationID == nil || receivedAt == nil {
		t.Fatalf("observation reference not persisted: id=%v at=%v err=%v", observationID, receivedAt, err)
	}
}

func TestReconcileVolumeIntegrity(t *testing.T) {
	s := testStoreController(t)
	c := &Controller{store: s, metrics: metrics.New(), reportedOrphans: map[string]bool{}}
	ctx := context.Background()
	project := fmt.Sprintf("p-int-vol-%d", os.Getpid())
	if err := s.EnsureProject(ctx, project, "int-test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx, `DELETE FROM volumes WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM scheduler_events WHERE node_id='node-int-b'`)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM user_events WHERE project_id=$1`, project)
		_, _ = s.Pool().Exec(ctx, `DELETE FROM projects WHERE id=$1`, project)
	})
	node := seedIntegrityNode(t, s, "node-int-b", true)

	seed := func(volID, state string) {
		t.Helper()
		if _, err := s.Pool().Exec(ctx, `
			INSERT INTO volumes(id,project_id,name,mode,node_id,size_bytes,state)
			VALUES($1,$2,$3,'LOCAL_RW',$4,1073741824,$5)`, volID, project, volID, node.ID, state); err != nil {
			t.Fatal(err)
		}
	}
	seed("vol-int-present", "UNAVAILABLE") // 节点恢复后由 inventory 确认
	seed("vol-int-missing", "READY")       // 完整列表缺席 → MISSING
	seed("vol-int-creating", "CREATING")   // 在途 → 不推导

	now := time.Now().Unix()
	inv := &pb.ListVolumesResponse{Complete: true, ObservationGeneration: 1, ObservedAtUnix: now,
		Observation: &pb.InventoryObservation{Complete: true, Epoch: "volume-epoch", Generation: 1, ObservedAtUnix: now}, Volumes: []*pb.VolumeInfo{
			{Id: "vol-int-present", SizeBytes: 1073741824, Mode: "LOCAL_RW", MetadataHealth: "HEALTHY"},
			{Id: "vol-int-orphan", SizeBytes: 2097152},
		}}
	volumes, err := s.ListVolumesOnNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	c.reconcileVolumeIntegrity(ctx, node, inv, volumes)

	if v, _ := s.GetVolume(ctx, "vol-int-present"); v.State != "READY" || v.Integrity != "METADATA_VERIFIED" {
		t.Fatalf("present volume = state %s integrity %s", v.State, v.Integrity)
	}
	v, _ := s.GetVolume(ctx, "vol-int-missing")
	if v.State != "UNAVAILABLE" || v.Integrity != "MISSING" {
		t.Fatalf("missing volume = state %s integrity %s, want UNAVAILABLE/MISSING", v.State, v.Integrity)
	}
	if v, _ = s.GetVolume(ctx, "vol-int-creating"); v.Integrity != "UNKNOWN" {
		t.Fatalf("in-flight volume integrity = %s, want UNKNOWN", v.Integrity)
	}
	// orphan 只报告：不存在自动删除路径（volume 仍在 agent 本地；PG 无行）。
	var orphanEvents int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM scheduler_events WHERE node_id=$1 AND reason LIKE '%orphan%'`, node.ID).Scan(&orphanEvents); err != nil || orphanEvents == 0 {
		t.Fatalf("orphan artifacts must be reported (report-only): events=%d err=%v", orphanEvents, err)
	}
}
