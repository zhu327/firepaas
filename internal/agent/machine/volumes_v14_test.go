// volumes_v14_test.go：v1.4-D dataset 加固回归——崩溃 spool 清理与
// volume-scoped spool 命名。
package machine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCleanupStaleDatasetSpool(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	stale := filepath.Join(dir, "firepaas-dataset-vol-old-1.tar.gz")
	fresh := filepath.Join(dir, "firepaas-dataset-vol-fresh-1.tar.gz")
	active := filepath.Join(dir, "firepaas-dataset-vol-active-1.tar.gz")
	other := filepath.Join(dir, "unrelated-file")
	for _, p := range []string{stale, fresh, active, other} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-2 * time.Hour)
	for _, p := range []string{stale, active} {
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}
	activeDatasetSpools.Store(active, struct{}{})
	t.Cleanup(func() { activeDatasetSpools.Delete(active) })
	removed, err := CleanupStaleDatasetSpool(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale spool must be removed")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("fresh spool (possible in-flight import) must survive")
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatal("active spool must survive regardless of age")
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatal("unrelated files must not be touched")
	}
}

// R2-7：CopyTo 上传 spool（firepaas-copy-to-*）同窗清理——崩溃窗口下其
// defer Remove 不执行，超龄文件必须随 dataset spool 一起回收。
func TestCleanupStaleSpoolCoversCopyToUploads(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	staleCopy := filepath.Join(dir, "firepaas-copy-to-12345")
	freshCopy := filepath.Join(dir, "firepaas-copy-to-67890")
	for _, p := range []string{staleCopy, freshCopy} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(staleCopy, past, past); err != nil {
		t.Fatal(err)
	}
	removed, err := CleanupStaleDatasetSpool(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1 (only the stale copy-to spool)", removed)
	}
	if _, err := os.Stat(staleCopy); !os.IsNotExist(err) {
		t.Fatal("stale copy-to spool must be removed")
	}
	if _, err := os.Stat(freshCopy); err != nil {
		t.Fatal("fresh copy-to spool (possible in-flight session) must survive")
	}
}

func TestCleanupStaleDatasetSpoolReportsStatErrors(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	dangling := filepath.Join(dir, "firepaas-dataset-dangling")
	if err := os.Symlink(filepath.Join(dir, "missing"), dangling); err != nil {
		t.Fatal(err)
	}
	if _, err := CleanupStaleDatasetSpool(time.Hour); err == nil ||
		!strings.Contains(err.Error(), "stat dataset spool") {
		t.Fatalf("cleanup must report stat error, got %v", err)
	}
}

func TestDatasetSpoolPatternIncludesBoundedVolumeAndOperationCoordinates(t *testing.T) {
	pattern := datasetSpoolPattern("volume/secret", "operation/secret")
	if !strings.HasPrefix(pattern, "firepaas-dataset-v") || !strings.Contains(pattern, "-o") {
		t.Fatalf("pattern lacks volume/operation coordinates: %q", pattern)
	}
	if strings.Contains(pattern, "secret") || strings.Contains(pattern, "/") || len(pattern) > 100 {
		t.Fatalf("pattern is unsafe or unbounded: %q", pattern)
	}
}
