package reservations

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testManager(t *testing.T) (*Manager, *redis.Client) {
	t.Helper()
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS=127.0.0.1:6379 to run reservation tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	m := New(rdb, 120*time.Second)
	t.Cleanup(func() {
		nodeKeys, _ := rdb.Keys(context.Background(), "resv:node:*").Result()
		opKeys, _ := rdb.Keys(context.Background(), "resv:op:*").Result()
		projKeys, _ := rdb.Keys(context.Background(), "resv:project:*").Result()
		rdb.Del(context.Background(), append(append(nodeKeys, opKeys...), projKeys...)...)
	})
	return m, rdb
}

func TestAcquireCommitRelease(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()

	if err := m.Acquire(ctx, "op-1", "n1", "dev", 1, 512, 64, 65536, 100, 32768); err != nil {
		t.Fatal(err)
	}
	rec, err := m.Get(ctx, "op-1")
	if err != nil || rec == nil {
		t.Fatalf("want record, got %v %v", rec, err)
	}
	if rec.NodeID != "n1" || rec.VCPU != 1 || rec.MemMib != 512 {
		t.Fatalf("bad record %+v", rec)
	}
	pending, err := m.PendingByNode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("pending mismatch: %v", pending)
	}
	if err := m.Commit(ctx, "op-1"); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{0, 0} {
		t.Fatalf("commit must clear pending: %v", pending)
	}
	if err := m.Release(ctx, "op-1"); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("double release must be ErrNotHeld, got %v", err)
	}
}

func TestProjectQuotaExceeded(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()
	if err := m.Acquire(ctx, "op-q", "n1", "dev", 50, 512, 64, 65536, 10, 32768); !errors.Is(err, ErrProjectQuota) {
		t.Fatalf("want ErrProjectQuota, got %v", err)
	}
}

func TestNodeCapacityExceeded(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()
	if err := m.Acquire(ctx, "op-mem", "n1", "dev", 1, 70000, 64, 65536, 100, 32768); !errors.Is(err, ErrNodeCapacity) {
		t.Fatalf("want ErrNodeCapacity, got %v", err)
	}
	if err := m.Acquire(ctx, "op-cpu", "n1", "dev", 64*4+1, 512, 64, 65536, 100, 32768); !errors.Is(err, ErrNodeCapacity) {
		t.Fatalf("want ErrNodeCapacity for cpu, got %v", err)
	}
}

func TestConcurrentAcquireNeverExceedsQuota(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()
	const n = 100
	ok := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			ok <- m.Acquire(ctx, "op-c"+string(rune('a'+i%26))+string(rune('0'+i/26%10))+string(rune('0'+i/100)),
				"n1", "dev", 1, 1024, 64, 65536, 10, 10240)
		}(i)
	}
	got, quota := 0, 0
	for i := 0; i < n; i++ {
		err := <-ok
		switch {
		case err == nil:
			got++
		case errors.Is(err, ErrProjectQuota):
			quota++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if got != 10 || quota != 90 {
		t.Fatalf("quota 10 should admit exactly 10, got %d ok / %d quota", got, quota)
	}
}

// P2-2：op 键 TTL 先过期、hash 增量永久残留的节点假满泄漏，由
// PruneStaleOps + Reset + 重新 Acquire 的全量重建修复。
func TestRebuildResetsHashes(t *testing.T) {
	m, rdb := testManager(t)
	ctx := context.Background()
	// 模拟泄漏：op 键过期后 hash 里残留 pending。
	if err := m.Acquire(ctx, "op-leak", "n1", "dev", 4, 4096, 64, 65536, 100, 32768); err != nil {
		t.Fatal(err)
	}
	rdb.Del(ctx, "resv:op:op-leak") // 模拟 TTL 过期（Rebuild 永远看不到它）
	// 活跃预约：仍在 PG 在途集合。
	if err := m.Acquire(ctx, "op-active", "n1", "dev", 1, 512, 64, 65536, 100, 32768); err != nil {
		t.Fatal(err)
	}

	// 全量重建：先清非活跃 op 键，再 Reset（原子：清零 hash + 从存活 op 键重放）。
	pruned, err := m.PruneStaleOps(ctx, map[string]bool{"op-active": true})
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 0 { // op-leak 键已“过期”，本就不可见
		t.Fatalf("want 0 pruned, got %d", pruned)
	}
	cleared, err := m.Reset(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cleared == 0 {
		t.Fatal("reset should clear leaked hash keys")
	}
	// 重放只包含存活 op 键：泄漏增量被清除，活跃承诺被恢复。
	pending, _ := m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("reset must drop leak and replay active op, got %v", pending["n1"])
	}

	// 幂等（P3-1）：同 opID 同参数重复 Acquire 不双计（不动 hash）。
	if err := m.Acquire(ctx, "op-active", "n1", "dev", 1, 512, 64, 65536, 100, 32768); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("idempotent acquire must not double count, got %v", pending["n1"])
	}
	// 换节点重试：旧节点 hash 扣回，新节点入账。
	if err := m.Acquire(ctx, "op-active", "n2", "dev", 1, 512, 64, 65536, 100, 32768); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{0, 0} || pending["n2"] != [2]int64{1, 512} {
		t.Fatalf("rebind must move pending n1->n2, got %v", pending)
	}
}
