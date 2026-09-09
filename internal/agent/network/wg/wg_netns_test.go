package wg

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/api"
)

// netnsRunner 把命令包进 `ip netns exec <ns>`（测试在两套 netns 里各自驱动
// Manager 的真实代码路径）。
type netnsRunner struct{ ns string }

func (r netnsRunner) Run(ctx context.Context, stdin, name string, args ...string) (string, error) {
	full := append([]string{"netns", "exec", r.ns, name}, args...)
	cmd := exec.CommandContext(ctx, "ip", full...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf(
			"netns %s: %s %s: %v: %s",
			r.ns,
			name,
			strings.Join(args, " "),
			err,
			strings.TrimSpace(string(out)),
		)
	}
	return strings.TrimSpace(string(out)), nil
}

// TestWGMeshNetns 真实内核验证：两个 netns + veth，两个 Manager 各自建设备/
// 密钥/peer 全量替换，跨隧道 TCP 握手成功；peer 移除后断开，重建后恢复。
//
//	sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/wg/ -run TestWGMeshNetns -v
func TestWGMeshNetns(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		nsA       = "fp-wg-a"
		nsB       = "fp-wg-b"
		vethA     = "v-wga"
		vethB     = "v-wgb"
		prefixA   = "fd7a:9a55:1::/64"
		prefixB   = "fd7a:9a55:2::/64"
		readyA    = "/tmp/fp-wg-netns-ready" // 测试临时文件；netns 只隔离网络，路径共享
		readyB    = "/tmp/fp-wg-netns-ready-b"
		handshake = "[fd7a:9a55:2::]:51999"
	)

	cleanupTestNetns(t, nsA, nsB, vethA, vethB)
	t.Cleanup(func() { cleanupTestNetns(t, nsA, nsB, vethA, vethB) })

	for _, c := range [][]string{
		{"ip", "netns", "add", nsA},
		{"ip", "netns", "add", nsB},
		{"ip", "link", "add", vethA, "type", "veth", "peer", "name", vethB},
		{"ip", "link", "set", vethA, "netns", nsA},
		{"ip", "link", "set", vethB, "netns", nsB},
	} {
		if out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", c, err, out)
		}
	}
	for _, c := range [][]string{
		{"ip", "netns", "exec", nsA, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", nsB, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", nsA, "ip", "addr", "add", "10.200.0.1/24", "dev", vethA},
		{"ip", "netns", "exec", nsB, "ip", "addr", "add", "10.200.0.2/24", "dev", vethB},
		{"ip", "netns", "exec", nsA, "ip", "link", "set", vethA, "up"},
		{"ip", "netns", "exec", nsB, "ip", "link", "set", vethB, "up"},
	} {
		if out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", c, err, out)
		}
	}

	mgrA, err := New(Options{
		Iface: "fp-wg0", KeyDir: filepath.Join(t.TempDir(), "a"),
		ListenPort: 51820, NodePrefix: prefixA, Runner: netnsRunner{ns: nsA},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgrB, err := New(Options{
		Iface: "fp-wg0", KeyDir: filepath.Join(t.TempDir(), "b"),
		ListenPort: 51820, NodePrefix: prefixB, Runner: netnsRunner{ns: nsB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgrA.Ensure(ctx); err != nil {
		t.Fatalf("ensure A: %v", err)
	}
	if err := mgrB.Ensure(ctx); err != nil {
		t.Fatalf("ensure B: %v", err)
	}
	peerA := api.Peer{
		NodeID: "node-a", Pubkey: mgrA.PublicKey(), Endpoint: "10.200.0.1:51820",
		NodePrefix: netip.MustParsePrefix(prefixA), FabricGeneration: 1,
	}
	peerB := api.Peer{
		NodeID: "node-b", Pubkey: mgrB.PublicKey(), Endpoint: "10.200.0.2:51820",
		NodePrefix: netip.MustParsePrefix(prefixB), FabricGeneration: 1,
	}
	if err := mgrA.UpdatePeers(ctx, 1, []api.Peer{peerB}); err != nil {
		t.Fatalf("update A: %v", err)
	}
	if err := mgrB.UpdatePeers(ctx, 1, []api.Peer{peerA}); err != nil {
		t.Fatalf("update B: %v", err)
	}

	// 握手 1：ns-a dial → ns-b listen（源地址必须来自节点 A 的 /64）。
	os.Remove(readyB)
	runHelperAsync(t, nsB, "listener", "[::]:51999", readyB, "")
	waitReady(t, ctx, readyB)
	if !runHelper(t, nsA, "dialer", handshake, "", "fd7a:9a55:1::") {
		t.Fatal("cross-node handshake failed")
	}

	// 轮换 1：A 侧移除 peer → dial 必须失败（路由随 peer 摘除）。
	if err := mgrA.UpdatePeers(ctx, 2, nil); err != nil {
		t.Fatalf("remove peer A: %v", err)
	}
	if runHelper(t, nsA, "dialer", handshake, "", "fd7a:9a55:1::") {
		t.Fatal("dial succeeded after peer removal")
	}

	// 轮换 2：peer 重建 → 握手恢复（全量替换往返幂等）。
	if err := mgrA.UpdatePeers(ctx, 3, []api.Peer{peerB}); err != nil {
		t.Fatalf("re-add peer A: %v", err)
	}
	os.Remove(readyB)
	runHelperAsync(t, nsB, "listener", "[::]:51999", readyB, "")
	waitReady(t, ctx, readyB)
	if !runHelper(t, nsA, "dialer", handshake, "", "fd7a:9a55:1::") {
		t.Fatal("handshake did not recover after peer re-add")
	}

	// 旧 generation 重放拒绝（G1 验收：peer/fabric 旧代重放拒）。
	if err := mgrA.UpdatePeers(ctx, 1, []api.Peer{peerB}); err == nil {
		t.Fatal("stale underlay generation accepted")
	}
}

// cleanupTestNetns 幂等删除测试 netns/veth。
func cleanupTestNetns(t *testing.T, nsA, nsB, vethA, vethB string) {
	t.Helper()
	for _, c := range [][]string{
		{"ip", "netns", "del", nsA},
		{"ip", "netns", "del", nsB},
		{"ip", "link", "del", vethA},
		{"ip", "link", "del", vethB},
	} {
		_ = exec.Command(c[0], c[1:]...).Run()
	}
}

func runHelperAsync(t *testing.T, ns, role, addr, readyFile, wantSrc string) {
	t.Helper()
	go func() {
		cmd := helperCommand(t, ns, role, addr, readyFile, wantSrc)
		_ = cmd.Run() // 结果由后续握手/断言体现；listener 生命周期由测试控制
	}()
}

func runHelper(t *testing.T, ns, role, addr, readyFile, wantSrc string) bool {
	t.Helper()
	cmd := helperCommand(t, ns, role, addr, readyFile, wantSrc)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("helper %s/%s: %v: %s", ns, role, err, out)
		return false
	}
	return true
}

func helperCommand(t *testing.T, ns, role, addr, readyFile, wantSrc string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("ip", "netns", "exec", ns, os.Args[0], "-test.run", "^TestWGMeshHelper$")
	cmd.Env = append(os.Environ(),
		"FIREPAAS_WG_ROLE="+role,
		"FIREPAAS_WG_ADDR="+addr,
		"FIREPAAS_WG_READY="+readyFile,
		"FIREPAAS_WG_WANT_SRC="+wantSrc,
	)
	return cmd
}

func waitReady(t *testing.T, ctx context.Context, readyFile string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(readyFile); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("helper listener not ready in time")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestWGMeshHelper 是子进程角色：在指定 netns 内 listen/dial（真实数据面）。
func TestWGMeshHelper(t *testing.T) {
	role := os.Getenv("FIREPAAS_WG_ROLE")
	if role == "" {
		t.Skip("helper process")
	}
	addr := os.Getenv("FIREPAAS_WG_ADDR")
	readyFile := os.Getenv("FIREPAAS_WG_READY")
	wantSrc := os.Getenv("FIREPAAS_WG_WANT_SRC")
	switch role {
	case "listener":
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		if readyFile != "" {
			_ = os.WriteFile(readyFile, []byte("ready"), 0o600)
		}
		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("pong")); err != nil {
			t.Fatal(err)
		}
		if wantSrc != "" && !addrHasPrefix(conn.RemoteAddr(), wantSrc) {
			t.Fatalf("source %s, want prefix %s", conn.RemoteAddr(), wantSrc)
		}
	case "dialer":
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "pong" {
			t.Fatalf("reply = %q", buf)
		}
		if wantSrc != "" && !addrHasPrefix(conn.LocalAddr(), wantSrc) {
			t.Fatalf("local addr %s, want prefix %s", conn.LocalAddr(), wantSrc)
		}
	default:
		t.Fatalf("unknown role %q", role)
	}
}

func addrHasPrefix(a net.Addr, prefix string) bool {
	ap, err := netip.ParseAddrPort(a.String())
	if err != nil {
		return false
	}
	return strings.HasPrefix(ap.Addr().String(), prefix)
}
