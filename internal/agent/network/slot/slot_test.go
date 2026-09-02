package slot

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	idxs, err := listStrayNetns()
	if err != nil {
		t.Fatalf("list netns: %v", err)
	}
	for _, idx := range idxs {
		out, err := exec.Command("ip", "netns", "exec", nsName(idx),
			"ip", "-o", "link", "show").CombinedOutput()
		if err != nil {
			continue
		}
		if strings.Contains(string(out), "hype-") {
			continue // 有 TAP：live VM 的 slot，不碰
		}
		_ = deleteNetns(idx)
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
		if _, err := m.Attach(ctx, id, "", ip); err != nil {
			t.Fatalf("cycle %d attach: %v", i, err)
		}
		if err := m.Release(ctx, id); err != nil {
			t.Fatalf("cycle %d release: %v", i, err)
		}
	}
	strays, err := listStrayNetns()
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
		host, ns, err := vethAddrs(c.idx)
		if err != nil {
			t.Fatalf("idx %d: %v", c.idx, err)
		}
		if host != c.host || ns != c.ns {
			t.Errorf("idx %d: got %s/%s want %s/%s", c.idx, host, ns, c.host, c.ns)
		}
	}
	// /30 块内不重叠：每 4 个地址一个块，host 与 ns 相邻。
	for i := 0; i < 200; i++ {
		h1, n1, err := vethAddrs(i)
		if err != nil {
			t.Fatal(err)
		}
		h2, n2, err := vethAddrs(i + 1)
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

	s, err := m.Attach(ctx, "m-test", tap, guestIP)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if s.Index != 0 {
		t.Fatalf("index = %d", s.Index)
	}

	// 内核对象存在。
	if exists, _ := netnsExists(0); !exists {
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
	if _, err := m.Attach(ctx, "m-test", tap, guestIP); err != nil {
		t.Fatalf("re-attach: %v", err)
	}

	// release → 全部消失（netlink 清理异步，轮询确认）。
	if err := m.Release(ctx, "m-test"); err != nil {
		t.Fatalf("release: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		exists, _ := netnsExists(0)
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
		if _, err := m.Attach(ctx, id, "", "10.100.5."+fmt.Sprint(i+10)); err != nil {
			t.Fatalf("cycle %d attach: %v", i, err)
		}
		if err := m.Release(ctx, id); err != nil {
			t.Fatalf("cycle %d release: %v", i, err)
		}
	}
	strays, err := listStrayNetns()
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
	m, err := New(Config{SubnetCIDR: "10.100.0.0/24", StatePath: filepath.Join(t.TempDir(), "slots.json")})
	if err != nil {
		t.Fatal(err)
	}
	var v4 strings.Builder
	for _, args := range m.isolationStepsIPv4() {
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
	// ip6 默认拒绝不得有意外放行口（无 established accept）。
	if strings.Contains(v6.String(), "accept") {
		t.Fatalf("ip6 isolation must be default-deny without accepts:\n%s", v6.String())
	}
}
