package ebpf

// TestEbpfRootEgressNAT（T3，ADR-0040 §7）：eBPF 后端不建 nft fp-isolation
// 表，必须由 EnsureNode 自行幂等保证 root 对 slot veth 源段的 masquerade；
// 缺此规则时 slot 内一级 NAT 改写后的源（链路地址）拿不到公网回程——全新
// eBPF 节点上非代理南北向流量断流（升级节点靠旧 nft 表残留掩盖）。
//
//	sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/ebpf/ -run TestEbpfRootEgressNAT -v

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestEbpfRootEgressNAT(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	table := fmt.Sprintf("fp-egress-test-%d", os.Getpid())
	cidr := "10.99.0.0/24"
	pinDir := fmt.Sprintf("/sys/fs/bpf/firepaas-nat-%d", os.Getpid())
	_ = exec.Command("nft", "delete", "table", "ip", table).Run()
	_ = exec.Command("umount", pinDir).Run()
	_ = os.Remove(pinDir)
	t.Cleanup(func() {
		_ = exec.Command("nft", "delete", "table", "ip", table).Run()
		_ = exec.Command("umount", pinDir).Run()
		_ = os.Remove(pinDir)
	})

	b, err := New(Options{
		PinDir:       pinDir,
		VethCIDR:     cidr,
		RootNATTable: table,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	assertRule := func() {
		t.Helper()
		out, err := exec.Command("nft", "list", "table", "ip", table).CombinedOutput()
		if err != nil {
			t.Fatalf("list root NAT table: %v: %s", err, out)
		}
		if !strings.Contains(string(out), cidr) || !strings.Contains(string(out), "masquerade") {
			t.Fatalf("root NAT rule missing in table %s:\n%s", table, out)
		}
	}
	if err := b.EnsureNode(ctx); err != nil {
		t.Fatalf("EnsureNode: %v", err)
	}
	assertRule()
	// 幂等：重复调用不报错、不产生第二条规则。
	if err := b.EnsureNode(ctx); err != nil {
		t.Fatalf("EnsureNode (second call): %v", err)
	}
	out, _ := exec.Command("nft", "-a", "list", "chain", "ip", table, "post").CombinedOutput()
	if n := strings.Count(string(out), cidr+" masquerade"); n != 1 {
		t.Fatalf("root NAT rule count = %d, want 1:\n%s", n, out)
	}
	// 不侵入既有 fp-isolation 表（nft 后端/遗留节点的出口 NAT 语义不变）。
	legacy, _ := exec.Command("nft", "list", "table", "ip", "fp-isolation").CombinedOutput()
	if strings.Contains(string(legacy), cidr) {
		t.Fatalf("EnsureNode must not touch fp-isolation:\n%s", legacy)
	}
}
