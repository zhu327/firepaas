package ebpf

// TestEbpfFabricPolicyAtomicSwitch（W2-2）：fabric 策略集双缓冲 + 一次翻转。
// 断言：写入发生在非活动集、翻转后旧集仍完整；校验失败的快照不翻转、
// 不改变 datapath 可见状态（旧集继续生效）。
//
//	sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/ebpf/ -run TestEbpfFabricPolicyAtomicSwitch -v

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

func TestEbpfFabricPolicyAtomicSwitch(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	pinDir := fmt.Sprintf("/sys/fs/bpf/firepaas-switch-%d", os.Getpid())
	_ = exec.Command("umount", pinDir).Run()
	_ = os.Remove(pinDir)
	t.Cleanup(func() {
		_ = exec.Command("umount", pinDir).Run()
		_ = os.Remove(pinDir)
	})
	b, err := New(Options{PinDir: pinDir, FlowSampleEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ulaA := netip.MustParseAddr("fd7a:9a55:1::5").As16()
	ulaB := netip.MustParseAddr("fd7a:9a55:1::9").As16()
	lookupIdentity := func(idx uint32, ula [16]byte) (uint32, bool) {
		ipcache, _, _, _ := b.fabricSet(idx)
		var id uint32
		if err := ipcache.Lookup(ula, &id); err != nil {
			return 0, false
		}
		return id, true
	}
	lookupNodeULA := func(idx uint32, ula [16]byte) bool {
		_, _, _, nodeULA := b.fabricSet(idx)
		var v uint8
		return nodeULA.Lookup(ula, &v) == nil
	}
	nodePrefix := netip.MustParseAddr("fd7a:9a55:1::").As16()

	if got := b.activeFabricSet(); got != 0 {
		t.Fatalf("initial active set = %d, want 0", got)
	}
	snapA := api.FabricPolicySnapshot{
		Generation: 1,
		Identities: []api.IdentityMapEntry{{IdentityID: 1, ULA: "fd7a:9a55:1::5"}},
		NodeULA:    "fd7a:9a55:1::",
	}
	if err := b.ApplyFabricPolicy(ctx, snapA); err != nil {
		t.Fatal(err)
	}
	if got := b.activeFabricSet(); got != 1 {
		t.Fatalf("after apply A active set = %d, want 1", got)
	}
	if id, ok := lookupIdentity(1, ulaA); !ok || id != 1 {
		t.Fatalf("set1 ipcache = %d,%v, want identity 1", id, ok)
	}
	if _, ok := lookupIdentity(0, ulaA); ok {
		t.Fatal("inactive set must not be written before flip")
	}
	if !lookupNodeULA(1, nodePrefix) {
		t.Fatal("set1 node_ula missing (platform traffic allowlist)")
	}

	snapB := api.FabricPolicySnapshot{
		Generation: 2,
		Identities: []api.IdentityMapEntry{{IdentityID: 2, ULA: "fd7a:9a55:1::9"}},
		NodeULA:    "fd7a:9a55:1::",
	}
	if err := b.ApplyFabricPolicy(ctx, snapB); err != nil {
		t.Fatal(err)
	}
	if got := b.activeFabricSet(); got != 0 {
		t.Fatalf("after apply B active set = %d, want 0", got)
	}
	if id, ok := lookupIdentity(0, ulaB); !ok || id != 2 {
		t.Fatalf("set0 ipcache = %d,%v, want identity 2", id, ok)
	}
	// 旧集在下次复用前保持完整（回滚/排障可见）。
	if id, ok := lookupIdentity(1, ulaA); !ok || id != 1 {
		t.Fatalf("old set must stay intact: %d,%v", id, ok)
	}
	if !lookupNodeULA(0, nodePrefix) {
		t.Fatal("set0 node_ula missing after flip")
	}

	// 校验失败的快照：不翻转、不改变可见状态。
	bad := api.FabricPolicySnapshot{
		Generation: 3,
		Identities: []api.IdentityMapEntry{{IdentityID: 3, ULA: "not-an-ula"}},
	}
	if err := b.ApplyFabricPolicy(ctx, bad); err == nil {
		t.Fatal("invalid snapshot must be rejected")
	}
	if got := b.activeFabricSet(); got != 0 {
		t.Fatalf("failed apply flipped active set to %d", got)
	}
	if id, ok := lookupIdentity(0, ulaB); !ok || id != 2 {
		t.Fatalf("failed apply changed visible set: %d,%v", id, ok)
	}
}
