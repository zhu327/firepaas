package slot

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// TestPersistDurablePermissions 验证 slots.json 落盘纪律的可观测面：0600
// 权限、无 .tmp 残留、Load 往返一致（temp/fsync/rename/fsync(dir) 序列
// 本身无法在单测中直接断言）。
func TestPersistDurablePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "slots.json")
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	m.slots["m1"] = Slot{Index: 3, MachineID: "m1", Tap: "hype-tap1", GuestIP: "10.100.0.5"}
	if err := m.persistLocked(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("slots.json perm = %o, want 600", perm)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file must not survive rename: %v", err)
	}
	m2, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Load(); err != nil {
		t.Fatal(err)
	}
	s, ok := m2.SlotFor("m1")
	if !ok || s.Index != 3 || s.GuestIP != "10.100.0.5" {
		t.Fatalf("reload mismatch: %+v ok=%v", s, ok)
	}
}

// cleanupStaleTestNetns 删除无 TAP 的 fp-slot-* netns（无 TAP = 无 firecracker
// 持有它，删除不影响业务机；带 TAP 的 slot 属于 live VM，必须保留）。
// 用途：上次测试失败退出时可能残留无 TAP 的 slot，先清再跑。
func cleanupStaleTestNetns(t *testing.T) {
	t.Helper()
	// 默认前缀的 manager（仅用于名字推导；不落盘：路径用临时文件）。
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: filepath.Join(t.TempDir(), "slots.json")})
	if err != nil {
		t.Fatal(err)
	}
	idxs, err := m.listStrayNetns()
	if err != nil {
		t.Fatalf("list netns: %v", err)
	}
	for _, idx := range idxs {
		out, err := exec.Command("ip", "netns", "exec", m.nsName(idx),
			"ip", "-o", "link", "show").CombinedOutput()
		if err != nil {
			continue
		}
		if strings.Contains(string(out), "hype-") {
			continue // 有 TAP：live VM 的 slot，不碰
		}
		_ = m.deleteNetns(idx)
	}
}

// TestSlotCycleLeak 是 1000 次快循环泄漏测试（无 VM，纯内核对象生命周期）。
// sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/slot/ -run TestSlotCycleLeak
func TestSlotCycleLeak(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	cleanupStaleTestNetns(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: t.TempDir() + "/slots.json"})
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("m-cycle-%d", i)
		ip := fmt.Sprintf("10.100.7.%d", i%200+10)
		if err := m.AttachNetns(ctx, api.NetnsSpec{MachineID: id, GuestIP: ip}); err != nil {
			t.Fatalf("cycle %d attach: %v", i, err)
		}
		if err := m.DetachNetns(ctx, id); err != nil {
			t.Fatalf("cycle %d release: %v", i, err)
		}
	}
	strays, err := m.listStrayNetns()
	if err != nil {
		t.Fatal(err)
	}
	if len(strays) != 0 {
		t.Fatalf("leaked netns after %d cycles: %v", n, strays)
	}
	if _, err := os.Stat("/sys/class/net/fp-vp0"); !os.IsNotExist(err) {
		t.Fatal("leaked veth fp-vp0")
	}
	if m.Count() != 0 {
		t.Fatalf("state not empty: %d", m.Count())
	}
}

func TestVethAddrs(t *testing.T) {
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: filepath.Join(t.TempDir(), "slots.json")})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		idx  int
		host string
		ns   string
	}{
		{0, "10.12.0.1", "10.12.0.2"},
		{1, "10.12.0.5", "10.12.0.6"},
		{62, "10.12.0.249", "10.12.0.250"},
		{63, "10.12.1.1", "10.12.1.2"},
		{126, "10.12.2.1", "10.12.2.2"},
	}
	for _, c := range cases {
		host, ns, err := m.vethAddrs(c.idx)
		if err != nil {
			t.Fatalf("idx %d: %v", c.idx, err)
		}
		if host != c.host || ns != c.ns {
			t.Errorf("idx %d: got %s/%s want %s/%s", c.idx, host, ns, c.host, c.ns)
		}
	}
	// /30 块内不重叠：每 4 个地址一个块，host 与 ns 相邻。
	for i := 0; i < 200; i++ {
		h1, n1, err := m.vethAddrs(i)
		if err != nil {
			t.Fatal(err)
		}
		h2, n2, err := m.vethAddrs(i + 1)
		if err != nil {
			t.Fatal(err)
		}
		if h1 == h2 || n1 == n2 || h1 == n1 || h2 == n2 {
			t.Fatalf("addr collision at %d/%d: %s %s %s %s", i, i+1, h1, n1, h2, n2)
		}
	}
}

func TestDeriveGateway(t *testing.T) {
	gw, err := deriveGateway("10.100.0.0/16")
	if err != nil || gw != "10.100.0.1" {
		t.Fatalf("got %q err %v", gw, err)
	}
	gw, err = deriveGateway("10.100.0.0/24")
	if err != nil || gw != "10.100.0.1" {
		t.Fatalf("got %q err %v", gw, err)
	}
	if _, err := deriveGateway("not-a-cidr"); err == nil {
		t.Fatal("expected error for bad cidr")
	}
}

func TestManagerNewAndLoad(t *testing.T) {
	dir := t.TempDir()
	statePath := dir + "/slots.json"
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: statePath})
	if err != nil {
		t.Fatal(err)
	}
	if m.cfg.Gateway != "10.100.0.1" {
		t.Fatalf("derived gateway = %q", m.cfg.Gateway)
	}
	if err := m.persistLocked(); err != nil {
		t.Fatal(err)
	}
	m2, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: statePath})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Load(); err != nil {
		t.Fatal(err)
	}
	if m2.Count() != 0 {
		t.Fatalf("count = %d", m2.Count())
	}
}

// TestSlotLifecycle 是 root-only 真机集成测试：attach→隔离验证→release→无残留。
// 需要 root + iproute2/nft：sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/slot/ -run TestSlotLifecycle -v
func TestSlotLifecycle(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	cleanupStaleTestNetns(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dir := t.TempDir()
	m, err := New(Config{
		SubnetCIDR: "10.100.0.0/24",
		StatePath:  dir + "/slots.json",
	})
	if err != nil {
		t.Fatal(err)
	}

	const guestIP = "10.100.5.42"
	// 模拟 hypeman TAP：真实测试环境会传入真 TAP；这里造一个 dummy TAP 验证
	// 移入 netns 的路径（vmnet 语义等价）。若 /dev/net/tun 可用则创建，否则跳过 TAP 检查。
	tap := ""
	if err := exec.Command("ip", "tuntap", "add", "dev", "fp-testtap", "mode", "tap").Run(); err == nil {
		tap = "fp-testtap"
		defer func() { _ = exec.Command("ip", "link", "del", "fp-testtap").Run() }()
	}

	if err := m.AttachNetns(ctx, api.NetnsSpec{MachineID: "m-test", Tap: tap, GuestIP: guestIP}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if s, ok := m.SlotFor("m-test"); !ok || s.Index != 0 {
		t.Fatalf("index = %d ok=%v", s.Index, ok)
	}

	// 内核对象存在。
	if exists, _ := m.netnsExists(0); !exists {
		t.Fatal("netns fp-slot-0 missing")
	}
	if _, err := os.Stat("/sys/class/net/fp-vp0"); err != nil {
		t.Fatal("veth fp-vp0 missing")
	}
	// root 侧 guest /32 路由存在。
	out, err := exec.Command("ip", "route", "show", guestIP+"/32").Output()
	if err != nil || !strings.Contains(string(out), "fp-vp0") {
		t.Fatalf("guest route missing: %s %v", out, err)
	}
	// nft 集合含本 veth。
	out, err = exec.Command("nft", "list", "set", "ip", "fp-isolation", "slot-veths").Output()
	if err != nil || !strings.Contains(string(out), "fp-vp0") {
		t.Fatalf("nft set missing veth: %s %v", out, err)
	}

	// 幂等重复 attach。
	if err := m.AttachNetns(ctx, api.NetnsSpec{MachineID: "m-test", Tap: tap, GuestIP: guestIP}); err != nil {
		t.Fatalf("re-attach: %v", err)
	}

	// release → 全部消失（netlink 清理异步，轮询确认）。
	if err := m.DetachNetns(ctx, "m-test"); err != nil {
		t.Fatalf("release: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		exists, _ := m.netnsExists(0)
		_, vethErr := os.Stat("/sys/class/net/fp-vp0")
		if !exists && os.IsNotExist(vethErr) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slot objects still exist after release")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if m.Count() != 0 {
		t.Fatalf("count = %d", m.Count())
	}

	// 快循环泄漏测试：30 次 attach/release（完整 1000 次在 e2e-m3 脚本里跑）。
	for i := 0; i < 30; i++ {
		id := fmt.Sprintf("m-cycle-%d", i)
		if err := m.AttachNetns(ctx, api.NetnsSpec{MachineID: id, GuestIP: "10.100.5." + fmt.Sprint(i+10)}); err != nil {
			t.Fatalf("cycle %d attach: %v", i, err)
		}
		if err := m.DetachNetns(ctx, id); err != nil {
			t.Fatalf("cycle %d release: %v", i, err)
		}
	}
	strays, err := m.listStrayNetns()
	if err != nil {
		t.Fatal(err)
	}
	if len(strays) != 0 {
		t.Fatalf("stray netns: %v", strays)
	}
}

// TestIsolationRuleGeneration 是 nft 规则生成级测试（R2：无需 root）：隔离脚本
// 的私网 drop 必含 canonical 集合关键段（CGNAT/loopback，与 dataset SSRF
// 校验同源），且存在 ip6 family 的 slot veth 默认拒绝表。
func TestIsolationRuleGeneration(t *testing.T) {
	if _, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: filepath.Join(t.TempDir(), "slots.json")}); err != nil {
		t.Fatal(err)
	}
	var v4 strings.Builder
	for _, args := range nftIsolationStepsIPv4(18080, 18443, VethRange) {
		v4.WriteString(strings.Join(args, " ") + "\n")
	}
	for _, want := range []string{"100.64.0.0/10", "127.0.0.0/8", "daddr", "masquerade"} {
		if !strings.Contains(v4.String(), want) {
			t.Fatalf("ip isolation steps missing %q:\n%s", want, v4.String())
		}
	}
	var v6 strings.Builder
	for _, args := range ip6IsolationSteps() {
		v6.WriteString(strings.Join(args, " ") + "\n")
	}
	for _, want := range []string{
		"nft add table ip6 fp-isolation",
		"nft add rule ip6 fp-isolation in iifname @slot-veths drop",
		"nft add rule ip6 fp-isolation fwdchain iifname @slot-veths drop",
	} {
		if !strings.Contains(v6.String(), want) {
			t.Fatalf("ip6 isolation steps missing %q:\n%s", want, v6.String())
		}
	}
	// ip6 默认拒绝：唯一放行口是 NDP（RS/RA/NS/NA/redirect——v6 邻居解析
	// 是 slot v6 接线与 mesh 链路的前提，真机 G2c 验收抓到 fallback 下 NS
	// 被 drop → NDP 恒 FAILED）；不得有其它 accept（无 established 口）。
	if n := strings.Count(v6.String(), "accept"); n != 1 || !strings.Contains(v6.String(), "nd-neighbor-solicit") {
		t.Fatalf("ip6 isolation must be default-deny with NDP-only accept:\n%s", v6.String())
	}
}

// TestParseSlotULA6：ULA 校验 fail closed（非法输入拒绝 attach，绝不进内核）。
func TestParseSlotULA6(t *testing.T) {
	if _, err := parseSlotULA6("fd7a:9a55:1::5"); err != nil {
		t.Fatalf("valid ULA rejected: %v", err)
	}
	for _, bad := range []string{"", "10.0.0.5", "not-an-ip", "fd7a:9a55:1::5/128", "::ffff:10.0.0.5"} {
		if _, err := parseSlotULA6(bad); err == nil {
			t.Fatalf("invalid ULA %q accepted", bad)
		}
	}
}

// TestSlotV6Plumbing 是 root-only 真机测试：attach（GuestIP6）→ root /128
// 路由 + NDP 代理 + netns 默认路由存在 → detach 后无残留。
// 需要 root + iproute2：sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/slot/ -run TestSlotV6Plumbing -v
func TestSlotV6Plumbing(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Geteuid() != 0 {
		t.Fatal("must run as root")
	}
	dir := t.TempDir()
	m, err := New(Config{
		SubnetCIDR: "10.12.0.0/16", Gateway: "10.12.0.1",
		StatePath: filepath.Join(dir, "slots.json"),
		Backend:   NewNftBackend(0, 0, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const ula = "fd7a:9a55:1::5"
	machineID := fmt.Sprintf("v6-%d", os.Getpid())
	// 真实 TAP：覆盖 ULA slot 的 TAP MTU 1280 接线（W5；与 guest eth0 对齐）。
	tapName := fmt.Sprintf("tapv6%d", os.Getpid()%100000)
	if out, err := exec.Command("ip", "tuntap", "add", "mode", "tap", "name", tapName).CombinedOutput(); err != nil {
		t.Fatalf("create tap: %v: %s", err, out)
	}
	if err := m.AttachNetns(ctx, api.NetnsSpec{MachineID: machineID, Tap: tapName, GuestIP: "10.12.0.9", GuestIP6: ula}); err != nil {
		t.Fatalf("attach v6: %v", err)
	}
	defer func() {
		if err := m.DetachNetns(ctx, machineID); err != nil {
			t.Fatalf("detach: %v", err)
		}
		if out, _ := exec.Command("ip", "-6", "route", "show", ula+"/128").CombinedOutput(); strings.Contains(
			string(out),
			ula,
		) {
			t.Fatalf("stale v6 route after detach: %s", out)
		}
	}()
	// root 侧 /128 路由存在。
	if out, err := exec.Command("ip", "-6", "route", "show", ula+"/128").CombinedOutput(); err != nil ||
		!strings.Contains(string(out), ula) {
		t.Fatalf("v6 host route missing: %v: %s", err, out)
	}
	// netns 内默认 v6 路由经 root。
	st, ok := m.CurrentNetns(machineID)
	if !ok {
		t.Fatal("slot missing after attach")
	}
	out, err := exec.Command("ip", "netns", "exec", m.nsName(st.Index),
		"ip", "-6", "route", "show", "default").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "fe80::1") {
		t.Fatalf("v6 default route missing in netns: %v: %s", err, out)
	}
	// TAP MTU 1280（W5：WG 封装开销下无分片黑洞；guest MSS 自动派生）。
	out, err = exec.Command("ip", "netns", "exec", m.nsName(st.Index),
		"ip", "link", "show", tapName).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "mtu 1280") {
		t.Fatalf("tap mtu 1280 missing: %v: %s", err, out)
	}
}

// TestStateFileLockMutualExclusion（P1 独立评审）：跨（进程/描述符）互斥——
// 后拿锁者必须阻塞到先持锁者释放之后；锁路径与 CNI 侧单实现同源。
func TestStateFileLockMutualExclusion(t *testing.T) {
	state := filepath.Join(t.TempDir(), "slots.json")
	if got := StateLockPath(state); got != state+".lock" {
		t.Fatalf("lock path = %q", got)
	}
	acquired := make(chan struct{})
	release := make(chan struct{})
	var released atomic.Int64
	var acquiredAfter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = WithStateFileLock(state, func() error {
			close(acquired)
			<-release // 持锁等待，制造确定性重叠窗口
			released.Store(time.Now().UnixNano())
			return nil
		})
	}()
	<-acquired
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = WithStateFileLock(state, func() error {
			acquiredAfter.Store(time.Now().UnixNano())
			return nil
		})
	}()
	time.Sleep(50 * time.Millisecond) // 确保第二个 goroutine 已阻塞在 flock
	close(release)
	wg.Wait()
	if acquiredAfter.Load() < released.Load() {
		t.Fatal("second holder acquired the lock before first holder released it")
	}
}
