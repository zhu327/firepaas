package state

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestFencesCheckAdvance(t *testing.T) {
	f, err := OpenFences(filepath.Join(t.TempDir(), "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	// 未知 machine：任何 generation 通过。
	if err := f.Check("m", 1); err != nil {
		t.Fatalf("fresh machine should pass any generation: %v", err)
	}
	if err := f.Advance("m", 2, "e1"); err != nil {
		t.Fatal(err)
	}
	// 相等/更高通过（同代请求的幂等性由 ledger 承担）。
	if err := f.Check("m", 2); err != nil {
		t.Fatalf("equal generation should pass: %v", err)
	}
	if err := f.Check("m", 3); err != nil {
		t.Fatalf("higher generation should pass: %v", err)
	}
	// 更低拒绝。
	if err := f.Check("m", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("want ErrStaleGeneration, got %v", err)
	}
	// Advance 低代 no-op：高水位不回退。
	if err := f.Advance("m", 1, "e0"); err != nil {
		t.Fatal(err)
	}
	if err := f.Check("m", 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("high-water mark regressed after low Advance: %v", err)
	}
	// 另一台 machine 互不影响。
	if err := f.Check("other", 1); err != nil {
		t.Fatalf("unrelated machine should pass: %v", err)
	}
}

// agent 重启后高水位必须保留（P0-2 验收：重启后旧 generation 仍被拒绝）。
func TestFencesRestartPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fences.json")
	f, err := OpenFences(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Advance("m", 5, "e1"); err != nil {
		t.Fatal(err)
	}
	f2, err := OpenFences(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f2.Check("m", 4); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("fence lost after restart: %v", err)
	}
	if err := f2.Check("m", 5); err != nil {
		t.Fatalf("equal generation should pass after restart: %v", err)
	}
}

// machine 删除后高水位保留（旧 generation 的 re-create 被拒绝），
// 直到 GC 窗口过期。
func TestFencesPruneBefore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fences.json")
	f, err := OpenFences(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Advance("m-old", 3, "e"); err != nil {
		t.Fatal(err)
	}
	// 手动把条目变老。
	f.mu.Lock()
	e := f.entries["m-old"]
	e.UpdatedAt = time.Now().Add(-48 * time.Hour)
	f.entries["m-old"] = e
	f.mu.Unlock()

	if err := f.Advance("m-new", 1, "e"); err != nil {
		t.Fatal(err)
	}
	removed, err := f.PruneBefore(time.Now().Add(-24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	// 过期条目移除后，旧 generation 重新放行（有界窗口语义）。
	if err := f.Check("m-old", 1); err != nil {
		t.Fatalf("expired fence should no longer reject: %v", err)
	}
	// 未过期条目仍生效：等代通过、低代被拒。
	if err := f.Check("m-new", 1); err != nil {
		t.Fatalf("fresh fence lost: %v", err)
	}
	if err := f.Check("m-new", 0); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("fresh fence must still reject lower generation, got %v", err)
	}
}

// R2-6：fence GC 绑定 machine 存活——活 machine 的高水位永不年龄回收；
// 已删 machine（不在存活清单）照常过期。
func TestFencesPruneBeforeUnlessLive(t *testing.T) {
	f, err := OpenFences(filepath.Join(t.TempDir(), "fences.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m-live", "m-gone"} {
		if err := f.Advance(id, 3, "e"); err != nil {
			t.Fatal(err)
		}
		// 两台都把条目变老到保留窗口之外。
		f.mu.Lock()
		e := f.entries[id]
		e.UpdatedAt = time.Now().Add(-48 * time.Hour)
		f.entries[id] = e
		f.mu.Unlock()
	}
	cutoff := time.Now().Add(-24 * time.Hour)
	removed, err := f.PruneBeforeUnlessLive(cutoff, func(id string) bool { return id == "m-live" })
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1 (only the deleted machine's fence)", removed)
	}
	// 活 machine：过期高水位仍拒绝旧请求。
	if err := f.Check("m-live", 2); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("live machine fence must survive age GC, got %v", err)
	}
	// 已删 machine：照常过期，旧代重新放行。
	if err := f.Check("m-gone", 1); err != nil {
		t.Fatalf("deleted machine fence should be age-GC'd: %v", err)
	}
}
