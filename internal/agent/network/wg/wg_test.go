package wg

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/state"
)

// compile-time 契约断言：Manager 必须完整实现 api.Underlay。
var _ api.Underlay = (*Manager)(nil)

// fakeRunner 记录命令序列并按预设脚本返回。stdin 单独记录（wg pubkey 经
// stdin 传私钥），argv 与 stdin 分离——用于断言私钥从未进入 argv。
type fakeRunner struct {
	mu     sync.Mutex
	calls  []string
	stdins []string
	script map[string]string // key = name + " " + args → stdout
	errors map[string]error
}

func (f *fakeRunner) Run(_ context.Context, stdin, name string, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, call)
	f.stdins = append(f.stdins, stdin)
	if err, ok := f.errors[call]; ok {
		return "", err
	}
	if out, ok := f.script[call]; ok {
		return out, nil
	}
	return "", nil
}

func (f *fakeRunner) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeRunner) joined(sep string) string { return strings.Join(f.snapshot(), sep) }

const testPriv = "test-private-key-material"

func newTestManager(t *testing.T, script map[string]string) (*Manager, *fakeRunner) {
	t.Helper()
	run := &fakeRunner{script: script}
	dir := t.TempDir()
	m, err := New(Options{
		Iface:      "fp-wg0",
		KeyDir:     filepath.Join(dir, "fabric"),
		ListenPort: 51820,
		NodePrefix: "fd7a:9a55:1::/64",
		Runner:     run,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m, run
}

func TestEnsureGeneratesKeyAndBuildsDevice(t *testing.T) {
	m, run := newTestManager(t, map[string]string{
		"wg genkey":                         testPriv,
		"wg pubkey":                         "pk-AAAA",
		"ip link add fp-wg0 type wireguard": "",
	})
	ctx := context.Background()
	if err := m.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if m.PublicKey() != "pk-AAAA" {
		t.Fatalf("pubkey = %q", m.PublicKey())
	}
	// 私钥文件 0600 落盘且内容正确。
	raw, err := os.ReadFile(m.keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != testPriv {
		t.Fatalf("private key file = %q", raw)
	}
	fi, err := os.Stat(m.keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("private key mode = %v, want 0600", fi.Mode().Perm())
	}
	// 私钥绝不进 argv：private-key 以文件路径形式出现。
	if c := run.joined("|"); strings.Contains(c, testPriv) {
		t.Fatalf("private key leaked into argv: %s", c)
	}
	// 设备创建 / 端口设置 / up / pubkey 派生序列齐全。
	for _, want := range []string{
		"ip link add fp-wg0 type wireguard",
		"wg set fp-wg0 private-key ",
		"listen-port 51820",
		"ip link set fp-wg0 up",
	} {
		if !strings.Contains(run.joined("|"), want) {
			t.Fatalf("missing command %q in %s", want, run.joined("|"))
		}
	}
}

func TestEnsureReusesExistingKeyAndDevice(t *testing.T) {
	m, run := newTestManager(t, map[string]string{"wg pubkey": "pk-BBBB"})
	dir := filepath.Dir(m.keyPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m.keyPath, []byte(testPriv+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// 设备已存在（重启恢复路径）。
	run.errors = map[string]error{
		"ip link add fp-wg0 type wireguard": errors.New("RTNETLINK answers: File exists"),
	}
	if err := m.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.PublicKey() != "pk-BBBB" {
		t.Fatalf("pubkey = %q", m.PublicKey())
	}
	// genkey 不得重跑。
	if c := run.joined("|"); strings.Contains(c, "genkey") {
		t.Fatalf("genkey re-ran on existing key: %s", c)
	}
}

func TestUpdatePeersFullReplaceAndRouting(t *testing.T) {
	m, run := newTestManager(t, map[string]string{
		"wg genkey": testPriv,
		"wg pubkey": "pk-node-a",
	})
	ctx := context.Background()
	peerB := api.Peer{
		NodeID: "node-b", Pubkey: "pk-b", Endpoint: "10.0.0.2:51820",
		NodePrefix: netip.MustParsePrefix("fd7a:9a55:2::/64"), FabricGeneration: 1,
	}
	if err := m.UpdatePeers(ctx, 1, []api.Peer{peerB}); err != nil {
		t.Fatal(err)
	}
	// syncconf 文件内容：公钥/endpoint/AllowedIPs/keepalive，无私钥。
	conf, err := os.ReadFile(filepath.Join(filepath.Dir(m.keyPath), "wg-sync.conf"))
	if errors.Is(err, os.ErrNotExist) {
		// syncconf 后文件被清理，属预期；内容断言在 fake 里以临时文件检查不可行，
		// 改在 writeSyncConf 直测。
		_ = conf
	} else if err != nil {
		t.Fatal(err)
	} else {
		t.Fatalf("sync conf not removed after syncconf: %s", conf)
	}
	joined := run.joined("|")
	for _, want := range []string{
		"wg syncconf fp-wg0 ",
		"ip -6 addr replace fd7a:9a55:1::/64 dev fp-wg0",
		"ip -6 route replace fd7a:9a55:2::/64 dev fp-wg0",
		// syncconf 后重断言（旧版 wg-tools 会清私钥/随机端口）。
		"wg set fp-wg0 private-key " + m.keyPath + " listen-port 51820",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %s", want, joined)
		}
	}
	// 全量替换：node-b 换 pubkey、新增 node-c。
	peerB2 := peerB
	peerB2.Pubkey = "pk-b-rotated"
	peerC := api.Peer{
		NodeID: "node-c", Pubkey: "pk-c", Endpoint: "10.0.0.3:51820",
		NodePrefix: netip.MustParsePrefix("fd7a:9a55:3::/64"), FabricGeneration: 2,
	}
	if err := m.UpdatePeers(ctx, 2, []api.Peer{peerB2, peerC}); err != nil {
		t.Fatal(err)
	}
	joined = run.joined("|")
	if !strings.Contains(joined, "ip -6 route replace fd7a:9a55:3::/64 dev fp-wg0") {
		t.Fatalf("new peer route missing in %s", joined)
	}
	// 移除全部 peer：两处 /64 路由删除，无 replace。
	if err := m.UpdatePeers(ctx, 3, nil); err != nil {
		t.Fatal(err)
	}
	joined = run.joined("|")
	if !strings.Contains(joined, "ip -6 route del fd7a:9a55:2::/64 dev fp-wg0") ||
		!strings.Contains(joined, "ip -6 route del fd7a:9a55:3::/64 dev fp-wg0") {
		t.Fatalf("removed peer routes missing in %s", joined)
	}
}

func TestUpdatePeersStaleGenerationRejected(t *testing.T) {
	m, _ := newTestManager(t, map[string]string{"wg genkey": testPriv, "wg pubkey": "pk"})
	ctx := context.Background()
	if err := m.UpdatePeers(ctx, 5, nil); err != nil {
		t.Fatal(err)
	}
	err := m.UpdatePeers(ctx, 4, nil)
	if !errors.Is(err, ErrStaleUnderlayGeneration) {
		t.Fatalf("err = %v, want ErrStaleUnderlayGeneration", err)
	}
	// 同代重放（幂等）通过；零代拒绝。
	if err := m.UpdatePeers(ctx, 5, nil); err != nil {
		t.Fatalf("same-gen replay: %v", err)
	}
	if err := m.UpdatePeers(ctx, 0, nil); err == nil {
		t.Fatal("zero generation must be rejected")
	}
}

func TestUpdatePeersFencedByFabricState(t *testing.T) {
	dir := t.TempDir()
	fabric, err := state.OpenFabric(filepath.Join(dir, "fabric.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fabric.Apply(state.FabricSnapshot{
		NodeID: "node-a", Generation: 7, NodePrefix: "fd7a:9a55:1::/64",
	}); err != nil {
		t.Fatal(err)
	}
	run := &fakeRunner{script: map[string]string{"wg genkey": testPriv, "wg pubkey": "pk"}}
	m, err := New(Options{
		Iface: "fp-wg0", KeyDir: filepath.Join(dir, "fabric-dir"),
		ListenPort: 51820, Fabric: fabric, Runner: run,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.UpdatePeers(ctx, 6, nil); !errors.Is(err, ErrStaleUnderlayGeneration) {
		t.Fatalf("stale-vs-fabric err = %v", err)
	}
	// 节点前缀取 fabric 当前快照而非 Options.NodePrefix。
	if err := m.UpdatePeers(ctx, 7, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(run.joined("|"), "ip -6 addr replace fd7a:9a55:1::/64 dev fp-wg0") {
		t.Fatalf("fabric node prefix not applied: %s", run.joined("|"))
	}
}

func TestWriteSyncConfContent(t *testing.T) {
	m, _ := newTestManager(t, nil)
	peers := []api.Peer{{
		NodeID: "node-b", Pubkey: "pk-b", Endpoint: "10.0.0.2:51820",
		NodePrefix: netip.MustParsePrefix("fd7a:9a55:2::/64"),
	}}
	if err := m.writeSyncConf(peers); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(m.keyPath), "wg-sync.conf"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for _, want := range []string{
		"[Peer]",
		"PublicKey = pk-b",
		"Endpoint = 10.0.0.2:51820",
		"AllowedIPs = fd7a:9a55:2::/64",
		"PersistentKeepalive = 25",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("sync conf missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "[Interface]") || strings.Contains(s, "PrivateKey") {
		t.Fatalf("sync conf must not contain interface/private key material:\n%s", s)
	}
	if strings.Contains(s, testPriv) {
		t.Fatal("private key leaked into sync conf")
	}
	fi, err := os.Stat(filepath.Join(filepath.Dir(m.keyPath), "wg-sync.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("sync conf mode = %v, want 0600", fi.Mode().Perm())
	}
}

func TestPeersFromSnapshot(t *testing.T) {
	peers, err := PeersFromSnapshot(state.FabricSnapshot{Peers: []state.FabricPeer{
		{
			NodeID: "node-b", Pubkey: "pk", Endpoint: "10.0.0.2:51820",
			NodePrefix: "fd7a:9a55:2::/64", FabricGeneration: 3,
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].NodePrefix.String() != "fd7a:9a55:2::/64" ||
		peers[0].FabricGeneration != 3 {
		t.Fatalf("peers = %+v", peers)
	}
	if _, err := PeersFromSnapshot(state.FabricSnapshot{Peers: []state.FabricPeer{
		{NodeID: "bad", NodePrefix: "not-a-prefix"},
	}}); err == nil {
		t.Fatal("bad prefix must fail closed")
	}
}
