package reservations

import (
	"context"
	"errors"
	"fmt"
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
		// 测试清理（非生产路径）枚举全部 epoch 键空间 + 指针/序号键。
		keys, _ := rdb.Keys(context.Background(), "resv:*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
	})
	return m, rdb
}

func TestAcquireCommitRelease(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()

	if err := m.Acquire(ctx, "op-1", "n1", "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
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
	if err := m.Acquire(ctx, "op-q", "n1", "dev", 50, 512, 1, 64, 65536, 1048576, 10, 32768, 1048576); !errors.Is(
		err,
		ErrProjectQuota,
	) {
		t.Fatalf("want ErrProjectQuota, got %v", err)
	}
}

func TestNodeCapacityExceeded(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()
	if err := m.Acquire(ctx, "op-mem", "n1", "dev", 1, 70000, 1, 64, 65536, 1048576, 100, 32768, 1048576); !errors.Is(
		err,
		ErrNodeCapacity,
	) {
		t.Fatalf("want ErrNodeCapacity, got %v", err)
	}
	if err := m.Acquire(ctx, "op-cpu", "n1", "dev", 64*4+1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); !errors.Is(
		err,
		ErrNodeCapacity,
	) {
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
				"n1", "dev", 1, 1024, 1, 64, 65536, 1048576, 10, 10240, 1048576)
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

func TestConcurrentAcquireNeverExceedsMachineConcurrency(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()
	const n = 100
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			results <- m.Acquire(ctx, fmt.Sprintf("op-machine-%d", i), "n1", "machine-project",
				1, 1, 1, 1000, 1000, 1000, 1000, 1000, 1000, 10)
		}(i)
	}
	admitted := 0
	for i := 0; i < n; i++ {
		err := <-results
		if err == nil {
			admitted++
		} else if !errors.Is(err, ErrProjectQuota) {
			t.Fatalf("unexpected acquire error: %v", err)
		}
	}
	if admitted != 10 {
		t.Fatalf("machine concurrency 10 admitted %d of 100", admitted)
	}
}

// P2-2：op 键 TTL 先过期、hash 增量永久残留的节点假满泄漏，由
// PruneStaleOps + Reset + 重新 Acquire 的全量重建修复。
func TestRebuildResetsHashes(t *testing.T) {
	m, rdb := testManager(t)
	ctx := context.Background()
	// 模拟泄漏：op 键过期后 hash 里残留 pending。
	if err := m.Acquire(ctx, "op-leak", "n1", "dev", 4, 4096, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	epoch, err := m.activeEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟 op 记录 TTL 过期（Rebuild 永远看不到它）；注意 ops 集合里仍留存
	// 悬空成员——这与真实 TTL 过期一致，PruneStaleOps 会顺手清掉它。
	rdb.Del(ctx, epochPfx(epoch)+"op:op-leak")
	// 活跃预约：仍在 PG 在途集合。
	if err := m.Acquire(ctx, "op-active", "n1", "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}

	// 全量重建：先清非活跃 op 记录，再 Reset（原子：新 epoch 重放存活 op
	// → 切换指针 → 旧 epoch 挂 TTL 过期）。
	pruned, err := m.PruneStaleOps(ctx, map[string]bool{"op-active": true})
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 { // op-leak 的 ops 集合悬空成员被清理（记录键已“过期”）
		t.Fatalf("want 1 pruned (dangling leaky op membership), got %d", pruned)
	}
	cleared, err := m.Reset(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cleared == 0 {
		t.Fatal("reset should retire the old epoch hash keys")
	}
	// 重放只包含存活 op 键：泄漏增量被清除，活跃承诺被恢复。
	pending, _ := m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("reset must drop leak and replay active op, got %v", pending["n1"])
	}

	// 幂等（P3-1）：同 opID 同参数重复 Acquire 不双计（不动 hash）。
	if err := m.Acquire(ctx, "op-active", "n1", "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("idempotent acquire must not double count, got %v", pending["n1"])
	}
	// 换节点重试：旧节点 hash 扣回，新节点入账。
	if err := m.Acquire(ctx, "op-active", "n2", "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{0, 0} || pending["n2"] != [2]int64{1, 512} {
		t.Fatalf("rebind must move pending n1->n2, got %v", pending)
	}
}

// v1.2-E（ADR-0035）：磁盘维度预约——节点容量与项目配额两级。
func TestDiskReservationLimits(t *testing.T) {
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS to run reservation tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	_ = rdb.FlushDB(context.Background()).Err()
	m := New(rdb, time.Minute)
	ctx := context.Background()

	// 节点磁盘容量：pending + req > nodeDiskTotal → ErrNodeCapacity。
	if err := m.Acquire(ctx, "op-disk-node", "n1", "dev", 1, 512, 11000, 64, 65536, 10240, 100, 32768, 1048576); !errors.Is(
		err,
		ErrNodeCapacity,
	) {
		t.Fatalf("node disk cap: want ErrNodeCapacity, got %v", err)
	}
	// 项目磁盘配额：pending + req > projectDiskQuota → ErrProjectQuota。
	if err := m.Acquire(ctx, "op-disk-proj", "n1", "dev", 1, 512, 11, 64, 65536, 1048576, 100, 32768, 10); !errors.Is(
		err,
		ErrProjectQuota,
	) {
		t.Fatalf("project disk quota: want ErrProjectQuota, got %v", err)
	}
	// 正常路径 + 释放幂等。
	if err := m.Acquire(ctx, "op-disk-ok", "n1", "dev", 1, 512, 1024, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(ctx, "op-disk-ok"); err != nil {
		t.Fatal(err)
	}
}

// 契约 C-3：节点 CPU 超售比为脚本入参，必须与调度器 BestOfKConfig.R 同源。
func TestAcquireRHonoursSchedulerR(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()
	// R=2：上限 = 4×2 = 8。旧硬编码 4 会把 9 错误放行（≤16）。
	if err := m.AcquireR(ctx, "op-r2-over", "n1", "dev", 9, 512, 1, 4, 65536, 1048576, 100, 32768, 1048576, 2); !errors.Is(
		err,
		ErrNodeCapacity,
	) {
		t.Fatalf("R=2 must reject 9 > 4*2, got %v", err)
	}
	// 边界值必须接受（与 scheduler canFit 的 > 判定一致）。
	if err := m.AcquireR(ctx, "op-r2-edge", "n1", "dev", 8, 512, 1, 4, 65536, 1048576, 100, 32768, 1048576, 2); err != nil {
		t.Fatalf("R=2 must accept 8 == 4*2, got %v", err)
	}
	// R<=0 退到兼容默认 4（存量调用方语义）。
	if err := m.AcquireR(ctx, "op-r4-edge", "n2", "dev", 16, 512, 1, 4, 65536, 1048576, 100, 32768, 1048576, 0); err != nil {
		t.Fatalf("default R=4 must accept 16 == 4*4, got %v", err)
	}
	if err := m.AcquireR(ctx, "op-r4-over", "n2", "dev", 1, 512, 1, 4, 65536, 1048576, 100, 32768, 1048576, 0); !errors.Is(
		err,
		ErrNodeCapacity,
	) {
		t.Fatalf("default R=4 must reject 17 > 4*4, got %v", err)
	}
}

// D-3：Reset 的 epoch 切换必须保持记账正确——切换前的在途预约被完整重放进
// 新 epoch（不丢不重），切换后新 Acquire 落新 epoch，旧 epoch 释放路径照常。
func TestResetEpochSwitchPreservesAccounting(t *testing.T) {
	m, rdb := testManager(t)
	ctx := context.Background()

	if err := m.Acquire(ctx, "op-a", "n1", "dev", 2, 1024, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	if err := m.Acquire(ctx, "op-b", "n2", "dev", 3, 2048, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	oldEpoch, err := m.activeEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := m.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	newEpoch, err := m.activeEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if newEpoch == oldEpoch {
		t.Fatalf("reset must switch active epoch, still %q", newEpoch)
	}
	// 重放保真：两笔预约的 pending 原样进入新 epoch。
	pending, _ := m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{2, 1024} || pending["n2"] != [2]int64{3, 2048} {
		t.Fatalf("replay must preserve accounting exactly, got %v", pending)
	}
	// 预约记录在新 epoch 键空间可读（释放/幂等判决依赖它）。
	for _, opID := range []string{"op-a", "op-b"} {
		rec, err := m.Get(ctx, opID)
		if err != nil || rec == nil {
			t.Fatalf("op %s must survive epoch switch, got %v %v", opID, rec, err)
		}
		if raw, _ := rdb.Get(ctx, epochPfx(newEpoch)+"op:"+opID).Result(); raw == "" {
			t.Fatalf("op %s must be copied into epoch %s", opID, newEpoch)
		}
	}
	// 切换前获取的预约在切换后照常释放（释放在新 epoch 账上扣回）。
	if err := m.Release(ctx, "op-a"); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{0, 0} || pending["n2"] != [2]int64{3, 2048} {
		t.Fatalf("release across switch must debit new epoch, got %v", pending)
	}
	// 切换后的新预约只落新 epoch。
	if err := m.Acquire(ctx, "op-c", "n1", "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	if rdb.Exists(ctx, epochPfx(oldEpoch)+"op:op-c").Val() != 0 {
		t.Fatal("post-switch acquire must not land in the old epoch")
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("post-switch acquire must count once in new epoch, got %v", pending["n1"])
	}
	// 重放绝不双计：再一次 Reset 后账目不变。
	if _, err := m.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	pending, _ = m.PendingByNode(ctx)
	if pending["n1"] != [2]int64{1, 512} || pending["n2"] != [2]int64{3, 2048} {
		t.Fatalf("second reset must not double count, got %v", pending)
	}
}

// D-3：旧 epoch 键挂租约 TTL 自然过期，不需要任何 KEYS/SCAN 清理路径；
// 过期不影响新 epoch 的记账。
func TestOldEpochExpiresViaTTL(t *testing.T) {
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS to run reservation tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	m := New(rdb, time.Second) // 1s 租约 → 旧 epoch 1s 内自我过期
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), "resv:*").Result()
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
	})
	ctx := context.Background()

	if err := m.Acquire(ctx, "op-ttl", "n1", "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
		t.Fatal(err)
	}
	oldEpoch, err := m.activeEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	// 旧 epoch 全体键必须带 TTL 且不超过租约 TTL。
	for _, suffix := range []string{"node:n1", "project:dev", "op:op-ttl", "nodes", "projects", "ops"} {
		ttl := rdb.TTL(ctx, epochPfx(oldEpoch)+suffix).Val()
		if ttl <= 0 || ttl > time.Second {
			t.Fatalf("old epoch key %s must carry lease TTL, got %v", suffix, ttl)
		}
	}
	// 新 epoch 记账在旧 epoch 过期前即正确。
	if pending, _ := m.PendingByNode(ctx); pending["n1"] != [2]int64{1, 512} {
		t.Fatalf("new epoch accounting before old expiry: %v", pending)
	}
	// 轮询等待自然过期（TTL 是确定性的，避免裸 sleep 碰运气）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if rdb.Exists(ctx, epochPfx(oldEpoch)+"node:n1").Val() == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if rdb.Exists(ctx, epochPfx(oldEpoch)+"node:n1").Val() != 0 {
		t.Fatal("old epoch must self-expire via TTL")
	}
	// 旧 epoch 过期后 op 记录也随之消失：释放报 ErrNotHeld，账目不受影响。
	if err := m.Release(ctx, "op-ttl"); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("post-expiry release must be ErrNotHeld, got %v", err)
	}
}

// D-3：预约的全部读写路径禁止 KEYS/SCAN（单线程阻塞源）。用 redis.Hook
// 在驱动层拦截命令名，走完整生命周期（acquire/rebind/get/pending/prune/
// reset/release）断言无一越线。
func TestManagerNeverUsesKeysOrScan(t *testing.T) {
	addr := os.Getenv("FIREPAAS_TEST_REDIS")
	if addr == "" {
		t.Skip("set FIREPAAS_TEST_REDIS to run reservation tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	forbidden := &forbiddenCmdHook{t: t}
	rdb.AddHook(forbidden)
	t.Cleanup(func() {
		keys, _ := rdb.Keys(context.Background(), "resv:*").Result() // 清理本身豁免（非生产路径）
		forbidden.disarm = true
		if len(keys) > 0 {
			rdb.Del(context.Background(), keys...)
		}
	})

	m := New(rdb, time.Minute)
	ctx := context.Background()
	mustAcquire := func(opID, nodeID string) {
		t.Helper()
		if err := m.Acquire(ctx, opID, nodeID, "dev", 1, 512, 1, 64, 65536, 1048576, 100, 32768, 1048576); err != nil {
			t.Fatal(err)
		}
	}
	mustAcquire("op-h1", "n1")
	mustAcquire("op-h2", "n1")
	mustAcquire("op-h1", "n2") // rebind 路径
	if _, err := m.Get(ctx, "op-h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.PendingByNode(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.PruneStaleOps(ctx, map[string]bool{"op-h1": true, "op-h2": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(ctx, "op-h1"); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit(ctx, "op-h2"); err != nil {
		t.Fatal(err)
	}
	if forbidden.violations != 0 {
		t.Fatalf("manager issued %d KEYS/SCAN commands", forbidden.violations)
	}
}

// forbiddenCmdHook 在驱动层拦截 KEYS/SCAN（生产路径零容忍）。
type forbiddenCmdHook struct {
	t          *testing.T
	disarm     bool
	violations int
}

func (h *forbiddenCmdHook) check(cmd string) {
	if h.disarm {
		return
	}
	if cmd == "keys" || cmd == "scan" {
		h.violations++
	}
}

func (h *forbiddenCmdHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *forbiddenCmdHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.check(cmd.Name())
		return next(ctx, cmd)
	}
}

func (h *forbiddenCmdHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.check(cmd.Name())
		}
		return next(ctx, cmds)
	}
}

// D-3：重建（epoch 切换）与并发 Acquire 交错时记账不丢不重——快照与指针
// 切换在同一原子脚本内，任何成功的 Acquire 最终被统计恰好一次。
func TestConcurrentAcquiresAcrossRebuild(t *testing.T) {
	m, _ := testManager(t)
	ctx := context.Background()

	const acquires = 60
	errs := make(chan error, acquires)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 4; i++ { // 重建在 acquire 洪峰中穿插 epoch 切换
			if _, err := m.Reset(ctx); err != nil {
				t.Errorf("reset: %v", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	for i := 0; i < acquires; i++ {
		go func(i int) {
			errs <- m.Acquire(ctx, fmt.Sprintf("op-x-%d", i), "n1", "dev",
				1, 128, 1, 64, 65536, 1048576, 1000, 1<<20, 1048576)
		}(i)
	}
	succeeded := 0
	for i := 0; i < acquires; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("acquire: %v", err)
		}
		succeeded++
	}
	<-done
	// 重建洪峰结束后再做一次 Reset 收敛，然后账目必须精确等于成功预约数。
	if _, err := m.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := m.PendingByNode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := pending["n1"]; got != [2]int64{int64(succeeded), int64(succeeded * 128)} {
		t.Fatalf("accounting must equal exactly the successful acquires, got %v want %d", got, succeeded)
	}
	// 每个成功预约的记录都必须存活（没有“切换窗口丢失”的预约）。
	for i := 0; i < acquires; i++ {
		rec, err := m.Get(ctx, fmt.Sprintf("op-x-%d", i))
		if err != nil || rec == nil {
			t.Fatalf("op-x-%d lost across rebuilds: %v %v", i, rec, err)
		}
	}
}
