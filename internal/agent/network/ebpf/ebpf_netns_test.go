package ebpf

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
)

// TestEbpfDatapathNetns 真机内核验证（ADR-0040 §10/§13 对等断言的最小集）：
//
//  1. 隔离：slot→host 非代理端口新连接被 tc_ingress 丢弃；
//
//  2. 代理 redirect + 反向 NAT：guest 视角 src 是原始 dst:80（回流不 masquerade）；
//
//  3. conn limit：per-execution 新 TCP 上限（含 SYN 重传去重）；
//
//  4. deny/allow LPM：deny 段 timeout（BPF drop）vs allow 段快速不可达；
//
//  5. v6 源绑定 + ipcache/policy：合法 ULA 对放行、无 policy/未知 src drop。
//
//     sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/ebpf/ -run TestEbpfDatapathNetns -v
//
// testProxyPort 读取测试代理端口（默认值兼容历史；同主机 lab 占用默认端口时
// 可用 FIREPAAS_TEST_PROXY_PORT80/443 切换，避免 bind 冲突）。
func testProxyPort(t *testing.T, env string, def int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(env))
	if raw == "" {
		return def
	}
	p, err := strconv.Atoi(raw)
	if err != nil || p < 1 || p > 65535 {
		t.Fatalf("%s=%q invalid", env, raw)
	}
	return p
}

func TestEbpfDatapathNetns(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		ns      = "fp-ebpf-test"
		guestNS = "fp-ebpf-guest"
		vh      = "v-ebpf-h"
		vg      = "v-ebpf-g"
		vhGuest = "v-ebpf-hg"
		vgGuest = "v-ebpf-gg"
		br      = "fp-br-ebpf"
		hostIP  = "10.12.9.1/24"
		nsIP    = "10.12.9.2/24"
		guestIP = "10.100.0.5"
		gwIP    = "10.100.0.1/24"
		ulaSrc  = "fd7a:9a55:1::5"
		ulaDst  = "fd7a:9a55:2::9"
	)
	for _, c := range [][]string{
		{"ip", "netns", "del", ns},
		{"ip", "netns", "del", guestNS},
		{"ip", "link", "del", vh},
		{"ip", "link", "del", vhGuest},
	} {
		_ = exec.Command(c[0], c[1:]...).Run()
	}
	t.Cleanup(func() {
		_ = exec.Command("ip", "netns", "del", ns).Run()
		_ = exec.Command("ip", "netns", "del", guestNS).Run()
		_ = exec.Command("ip", "link", "del", vh).Run()
		_ = exec.Command("ip", "link", "del", vhGuest).Run()
		_ = exec.Command("ip", "-6", "addr", "del", ulaDst+"/128", "dev", "lo").Run()
		_ = exec.Command("ip", "-6", "route", "del", ulaSrc+"/128").Run()
		_ = exec.Command("ip", "route", "del", guestIP+"/32").Run()
	})

	// 诚实的三层拓扑（与真机 slot 同构）：
	//   guest ns（10.100.0.5）──veth── slot 内 bridge fp-br-ebpf（gw 10.100.0.1）
	//   slot ns ──veth── root ns（10.12.9.1/10.12.9.2）
	// guest 流量以转发路径进入 slot → prerouting DNAT 生效（本地生成不会）。
	for _, c := range [][]string{
		{"ip", "netns", "add", ns},
		{"ip", "netns", "add", guestNS},
		{"ip", "link", "add", vh, "type", "veth", "peer", "name", vg},
		{"ip", "link", "set", vg, "netns", ns},
		{"ip", "link", "add", vhGuest, "type", "veth", "peer", "name", vgGuest},
		{"ip", "link", "set", vgGuest, "netns", ns},
		{"ip", "link", "set", vhGuest, "netns", guestNS},
		// slot 内 bridge = guest 网关
		{"ip", "netns", "exec", ns, "ip", "link", "add", br, "type", "bridge"},
		{"ip", "netns", "exec", ns, "ip", "addr", "add", gwIP, "dev", br},
		{"ip", "netns", "exec", ns, "ip", "link", "set", vgGuest, "master", br},
		{"ip", "netns", "exec", ns, "ip", "link", "set", br, "up"},
		{"ip", "netns", "exec", ns, "ip", "link", "set", vgGuest, "up"},
		// slot ↔ root 点对点
		{"ip", "addr", "add", hostIP, "dev", vh},
		{"ip", "link", "set", vh, "up"},
		{"ip", "netns", "exec", ns, "ip", "addr", "add", nsIP, "dev", vg},
		{"ip", "netns", "exec", ns, "ip", "link", "set", vg, "up"},
		{"ip", "netns", "exec", ns, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", ns, "ip", "route", "replace", "default", "via", "10.12.9.1"},
		// guest
		{"ip", "netns", "exec", guestNS, "ip", "addr", "add", guestIP + "/24", "dev", vhGuest},
		{"ip", "netns", "exec", guestNS, "ip", "link", "set", vhGuest, "up"},
		{"ip", "netns", "exec", guestNS, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", guestNS, "ip", "route", "replace", "default", "via", "10.100.0.1"},
		// guest /32 回程 + v6 测试面
		{"ip", "route", "replace", guestIP + "/32", "via", "10.12.9.2", "dev", vh},
		{"ip", "-6", "addr", "add", "fd7a:9a55:fe::1/64", "dev", vh},
		{"ip", "netns", "exec", ns, "ip", "-6", "addr", "add", "fd7a:9a55:fe::2/64", "dev", vg},
		{"ip", "-6", "addr", "add", ulaDst + "/128", "dev", "lo"},
		{"ip", "-6", "route", "replace", ulaSrc + "/128", "via", "fd7a:9a55:fe::2", "dev", vh},
		{"ip", "netns", "exec", ns, "ip", "-6", "route", "replace", ulaSrc + "/128", "dev", br},
		// ulaDst 经 root 口 vh 的 fe::1（slot ns 内非本地，确定性有效）。
		{"ip", "netns", "exec", ns, "ip", "-6", "route", "replace", ulaDst + "/128", "via", "fd7a:9a55:fe::1", "dev", vg},
		// bridge 的 v6 网关地址必须与点对点两端都不同：曾与 root vh 同为
		// fe::1，导致 via-local 路由在 DAD 时序下时而接受、事后失效，v6 平面
		// 从未真正连通过。guest 经 fe::3 上 slot，再经 vg 出 root。
		{"ip", "netns", "exec", ns, "ip", "-6", "addr", "add", "fd7a:9a55:fe::3/64", "dev", br},
		{"ip", "netns", "exec", guestNS, "ip", "-6", "addr", "add", "fd7a:9a55:fe::5/64", "dev", vhGuest},
		// v6 拨测源地址必须本地存在：helper 以 bind 方式指定 src（无地址则
		// bind 直接失败，测不到 BPF 裁决）。guest 拥有 ulaSrc；::6 用于
		// “未知 src 被源绑定拒绝”断言（同样需要 bind 成功）。
		{"ip", "netns", "exec", guestNS, "ip", "-6", "addr", "add", ulaSrc + "/128", "dev", vhGuest},
		{"ip", "netns", "exec", guestNS, "ip", "-6", "addr", "add", "fd7a:9a55:1::6/128", "dev", vhGuest},
		{"ip", "netns", "exec", guestNS, "ip", "-6", "route", "replace", ulaDst + "/128", "via", "fd7a:9a55:fe::3"},
	} {
		if out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", c, err, out)
		}
	}
	// 同一网段下一跳：guest→bridge 需要 onlink（10.100.0.1 是桥自地址）。
	_ = exec.Command("ip", "netns", "exec", guestNS, "ip", "route", "replace", "default",
		"via", "10.100.0.1", "dev", vhGuest, "onlink").Run()
	_ = exec.Command("ip", "netns", "exec", ns, "sysctl", "-w", "net.ipv4.ip_forward=1").Run()
	_ = exec.Command("ip", "netns", "exec", ns, "sysctl", "-w", "net.ipv6.conf.all.forwarding=1").Run()
	_ = exec.Command("sysctl", "-w", "net.ipv4.ip_forward=1").Run()
	_ = exec.Command("sysctl", "-w", "net.ipv6.conf.all.forwarding=1").Run()

	pinDir := fmt.Sprintf("/sys/fs/bpf/firepaas-test-%d", os.Getpid())
	_ = exec.Command("umount", pinDir).Run()
	_ = os.Remove(pinDir)
	t.Cleanup(func() {
		_ = exec.Command("umount", pinDir).Run()
		_ = os.Remove(pinDir)
	})

	b, err := New(Options{
		PinDir:         pinDir,
		EgressProxy80:  testProxyPort(t, "FIREPAAS_TEST_PROXY_PORT80", 18080),
		EgressProxy443: testProxyPort(t, "FIREPAAS_TEST_PROXY_PORT443", 18443),
		// 独立 root NAT 表：避免与同主机 lab/agentd 的 fp-egress 表互扰。
		VethCIDR:     "10.12.9.0/24",
		RootNATTable: fmt.Sprintf("fp-egress-test-%d", os.Getpid()),
		// 测试需看到每个 allow 事件（生产默认 1/128 采样）。
		FlowSampleEvery: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = exec.Command("nft", "delete", "table", "ip", fmt.Sprintf("fp-egress-test-%d", os.Getpid())).Run()
	})
	ref := slot.SlotRef{
		Index: 0, VethHost: vh, VethGuest: vg,
		HostAddr: "10.12.9.1", NsAddr: "10.12.9.2", Netns: ns, GuestIP: guestIP,
		GuestIP6: ulaSrc,
	}
	if os.Getenv("EBPF_DEBUG_NO_TC") == "" {
		// 与 slot.Manager.ensureNodeBackend 同序：EnsureNode（节点级设施，
		// 含 host_addrs4 本机地址集）先于 AttachSlot。
		if err := b.EnsureNode(ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.AttachSlot(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.EnsureSlotNAT(ctx, ref); err != nil {
		t.Fatal(err)
	}

	// 1) 隔离：slot→host 非代理端口新连接必须失败（SYN 被 ingress drop）。
	isolListener, err := net.Listen("tcp", "0.0.0.0:19999")
	if err != nil {
		t.Fatal(err)
	}
	defer isolListener.Close()
	if os.Getenv("EBPF_DEBUG_NO_TC") == "" && runHelper(t, ctx, guestNS, dialSpec{
		target: "10.12.9.1:19999", timeout: 1200 * time.Millisecond,
	}) {
		t.Fatal("slot→host non-proxy connection must be dropped by isolation")
	}

	// 1b) host 发起连接的回程必须放行（探针路径回归）：host 拨 guest:19997，
	// 回包 dst = host 私网地址，凭 host_addrs4+ACK 先于 private drop 放行
	//（真机 G2c 验收抓到缺此语义时探针回包全丢 → readiness 恒 NOT_READY）。
	if os.Getenv("EBPF_DEBUG_NO_TC") == "" {
		srv := exec.CommandContext(ctx, "ip", "netns", "exec", guestNS, "python3", "-c",
			"import socket\ns=socket.socket()\ns.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"+
				"s.bind((\"0.0.0.0\",19997))\ns.listen(4)\n"+
				"import sys\nprint(\"READY\",flush=True)\n"+
				"while True:\n c,_=s.accept()\n c.sendall(b\"probe-ok\")\n c.close()\n")
		out, _ := srv.StdoutPipe()
		if err := srv.Start(); err != nil {
			t.Fatalf("guest listener: %v", err)
		}
		defer func() { _ = srv.Process.Kill() }()
		go func() { _, _ = io.Copy(io.Discard, out) }()
		// 等监听就绪（有界）：TCP 拨通即成功。
		var conn net.Conn
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if conn, err = net.DialTimeout("tcp", guestIP+":19997", time.Second); err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if conn == nil {
			t.Fatalf("host→guest dial never succeeded: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 8)
		n, rerr := conn.Read(buf)
		conn.Close()
		if rerr != nil || string(buf[:n]) != "probe-ok" {
			t.Fatalf("host→guest reply broken (probe path regression): n=%d err=%v data=%q", n, rerr, buf[:n])
		}
	}

	// 2) 代理 redirect + 反向 NAT（回流 src 还原为原始 dst:80）。
	proxy, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", testProxyPort(t, "FIREPAAS_TEST_PROXY_PORT80", 18080)))
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	proxyErr := make(chan error, 1)
	accepted := 0
	go func() {
		for {
			conn, err := proxy.Accept()
			if err != nil {
				proxyErr <- err
				return
			}
			accepted++
			t.Logf("proxy accepted conn from %s", conn.RemoteAddr())
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4)
				if _, err := io.ReadFull(c, buf); err != nil {
					// holder 连接（只占位不限回显）正常关闭即 EOF：不是代理
					// 错误，不上报（否则 step-3 的两个 holder 必触发终检误杀）。
					if err != io.EOF && err != io.ErrUnexpectedEOF {
						proxyErr <- err
						return
					}
					return
				}
				_, _ = c.Write([]byte("pong"))
			}(conn)
		}
	}()
	if !runHelper(t, ctx, guestNS, dialSpec{
		target: "203.0.113.10:80", src: guestIP, timeout: 5 * time.Second,
		echo: "ping", want: "pong", wantRemote: "203.0.113.10:80",
	}) {
		dumpEbpfDebug(t, ctx, ns, guestNS, b, guestIP)
		t.Fatal("proxied dial with reverse NAT failed")
	}

	// 3) conn limit：per-execution 上限 2（含 SYN 重传去重）。
	if err := b.ApplyEgress(ctx, ref, &api.PolicySnapshot{
		Mode: "allowlist", AllowedCIDRs: []string{"203.0.113.0/24"},
		MaxTCPConns: 2, Generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	readyA := filepath.Join(t.TempDir(), "hold-a")
	readyB := filepath.Join(t.TempDir(), "hold-b")
	go runHelperAsync(t, ctx, guestNS, dialSpec{
		target: "203.0.113.10:80", src: guestIP, timeout: 5 * time.Second,
		hold: 4 * time.Second, ready: readyA,
	})
	go runHelperAsync(t, ctx, guestNS, dialSpec{
		target: "203.0.113.10:80", src: guestIP, timeout: 5 * time.Second,
		hold: 4 * time.Second, ready: readyB,
	})
	waitReady(t, readyA, 5*time.Second)
	waitReady(t, readyB, 5*time.Second)
	if runHelper(t, ctx, guestNS, dialSpec{
		target: "203.0.113.10:80", src: guestIP, timeout: 1200 * time.Millisecond,
	}) {
		out, _ := exec.Command("ip", "netns", "exec", ns, "nft", "list", "table", "ip", "fp-slot").CombinedOutput()
		t.Logf("ns fp-slot table:\n%s", out)
		t.Fatal("third connection must exceed conn limit")
	}

	// 4) deny/allow LPM：同构差分——两个 host 本地监听地址，deny 段被 BPF
	//    drop（timeout），allow 段放行（连接成功）。地址必须在 canonical 保留集
	//    之外：保留段（含 203.0.113.0/24、198.51.100.0/24）在 nft/ebpf 两端隔离层
	//    即被 drop（parity 一致），不能用来断言 egress allow 行为。
	if err := b.ApplyEgress(ctx, ref, &api.PolicySnapshot{
		Mode: "deny_all", DeniedCIDRs: []string{"22.22.22.0/24"},
		AllowedCIDRs: []string{"11.11.11.0/24"}, Generation: 2,
	}); err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{"11.11.11.9/32", "22.22.22.9/32"} {
		_ = exec.Command("ip", "addr", "add", addr, "dev", "lo").Run()
	}
	t.Cleanup(func() {
		_ = exec.Command("ip", "addr", "del", "11.11.11.9/32", "dev", "lo").Run()
		_ = exec.Command("ip", "addr", "del", "22.22.22.9/32", "dev", "lo").Run()
	})
	deniedLn, err := net.Listen("tcp", "22.22.22.9:8080")
	if err != nil {
		t.Fatal(err)
	}
	defer deniedLn.Close()
	allowedLn, err := net.Listen("tcp", "11.11.11.9:8080")
	if err != nil {
		t.Fatal(err)
	}
	defer allowedLn.Close()
	echoServer := func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4)
				if _, err := io.ReadFull(c, buf); err != nil {
					return
				}
				_, _ = c.Write([]byte("echo"))
			}(conn)
		}
	}
	go echoServer(deniedLn)
	go echoServer(allowedLn)
	if out := helperOutput(t, ctx, guestNS, dialSpec{
		target: "22.22.22.9:8080", timeout: 1200 * time.Millisecond,
	}); !strings.Contains(out, "timeout") {
		t.Fatalf("denied cidr err = %s, want timeout (BPF drop)", out)
	}
	if !runHelper(t, ctx, guestNS, dialSpec{
		target: "11.11.11.9:8080", src: guestIP, timeout: 5 * time.Second,
		echo: "ping", want: "echo",
	}) {
		t.Fatal("allowed cidr dial must pass BPF and reach host listener")
	}

	// 4b) deny_all 无 allow 条目 = 默认拒绝（等价 nft egress-fwd 末尾 drop；
	// mode_drop 回归断言）。
	if err := b.ApplyEgress(ctx, ref, &api.PolicySnapshot{
		Mode: "deny_all", Generation: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if out := helperOutput(t, ctx, guestNS, dialSpec{
		target: "11.11.11.9:8080", timeout: 1200 * time.Millisecond,
	}); !strings.Contains(out, "timeout") {
		t.Fatalf("deny_all default err = %s, want timeout (BPF drop)", out)
	}

	// 5) v6 源绑定 + ipcache/policy/policy_ports（G2a 逐端口放行）。
	if err := b.ApplyFabricPolicy(ctx, api.FabricPolicySnapshot{
		Generation: 1,
		Identities: []api.IdentityMapEntry{
			{IdentityID: 1, ULA: ulaSrc},
			{IdentityID: 2, ULA: ulaDst},
		},
		Entries: []api.FabricPolicyEntry{{SrcIdentity: 1, DstIdentity: 2, Generation: 1, Ports: []uint32{39999}}},
	}); err != nil {
		t.Fatal(err)
	}
	v6ln, err := net.Listen("tcp", "["+ulaDst+"]:39999")
	if err != nil {
		t.Fatal(err)
	}
	defer v6ln.Close()
	go func() {
		conn, err := v6ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("v6ok"))
	}()
	if !runHelper(t, ctx, guestNS, dialSpec{
		target: "[" + ulaDst + "]:39999", src: ulaSrc, timeout: 5 * time.Second,
		want: "v6ok",
	}) {
		t.Fatal("east-west v6 with policy entry must pass")
	}

	// 未放行端口 → deny（规则最小化：policy 条目存在但 policy_ports 无 key）。
	v6ln2, err := net.Listen("tcp", "["+ulaDst+"]:40001")
	if err != nil {
		t.Fatal(err)
	}
	defer v6ln2.Close()
	go func() {
		conn, err := v6ln2.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("v6bad"))
	}()
	if runHelper(t, ctx, guestNS, dialSpec{
		target: "[" + ulaDst + "]:40001", src: ulaSrc, timeout: 1200 * time.Millisecond,
	}) {
		t.Fatal("east-west v6 to non-allowed port must drop")
	}

	// 全量替换清空条目 → default deny（下线身份即时失效）。
	if err := b.ApplyFabricPolicy(ctx, api.FabricPolicySnapshot{
		Generation: 2,
		Identities: []api.IdentityMapEntry{
			{IdentityID: 1, ULA: ulaSrc},
			{IdentityID: 2, ULA: ulaDst},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if runHelper(t, ctx, guestNS, dialSpec{
		target: "[" + ulaDst + "]:39999", src: ulaSrc, timeout: 1200 * time.Millisecond,
	}) {
		t.Fatal("default-deny policy must drop east-west after snapshot clear")
	}
	// 未知 src（不在 ipcache）→ v6 源绑定拒绝。
	if runHelper(t, ctx, guestNS, dialSpec{
		target: "[" + ulaDst + "]:39999", src: "fd7a:9a55:1::6", timeout: 1200 * time.Millisecond,
	}) {
		t.Fatal("unknown v6 source must be dropped (source binding)")
	}

	select {
	case err := <-proxyErr:
		t.Fatalf("proxy server: %v", err)
	default:
	}
}

// dialSpec 描述一次 helper 连接。
type dialSpec struct {
	target     string
	src        string // 绑定源（空 = 不绑定）
	timeout    time.Duration
	echo       string // 连接后发送
	want       string // 期望回读
	wantRemote string // 期望对端地址前缀（反向 NAT 断言）
	hold       time.Duration
	ready      string // 连接建立后写 ready 文件
}

func helperEnv(spec dialSpec) []string {
	env := []string{
		"FIREPAAS_DIAL_TARGET=" + spec.target,
		"FIREPAAS_DIAL_SRC=" + spec.src,
		"FIREPAAS_DIAL_TIMEOUT=" + spec.timeout.String(),
	}
	if spec.echo != "" {
		env = append(env, "FIREPAAS_DIAL_ECHO="+spec.echo)
	}
	if spec.want != "" {
		env = append(env, "FIREPAAS_DIAL_WANT="+spec.want)
	}
	if spec.wantRemote != "" {
		env = append(env, "FIREPAAS_DIAL_WANT_REMOTE="+spec.wantRemote)
	}
	if spec.hold > 0 {
		env = append(env, "FIREPAAS_DIAL_HOLD="+spec.hold.String())
	}
	if spec.ready != "" {
		env = append(env, "FIREPAAS_DIAL_READY="+spec.ready)
	}
	return env
}

func runHelper(t *testing.T, ctx context.Context, ns string, spec dialSpec) bool {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ip", "netns", "exec", ns, os.Args[0],
		"-test.run", "^TestEbpfDialHelper$")
	cmd.Env = append(os.Environ(), helperEnv(spec)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf("helper %s: %v: %s", spec.target, err, strings.TrimSpace(string(out)))
		return false
	}
	return true
}

func helperOutput(t *testing.T, ctx context.Context, ns string, spec dialSpec) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "ip", "netns", "exec", ns, os.Args[0],
		"-test.run", "^TestEbpfDialHelper$")
	cmd.Env = append(os.Environ(), helperEnv(spec)...)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func runHelperAsync(t *testing.T, ctx context.Context, ns string, spec dialSpec) {
	t.Helper()
	go func() {
		cmd := exec.CommandContext(ctx, "ip", "netns", "exec", ns, os.Args[0],
			"-test.run", "^TestEbpfDialHelper$")
		cmd.Env = append(os.Environ(), helperEnv(spec)...)
		_ = cmd.Run()
	}()
}

func waitReady(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper not ready: %s", path)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// TestEbpfDialHelper 在 netns 内执行真实连接（验证连通性/超时/反向 NAT）。
func TestEbpfDialHelper(t *testing.T) {
	target := os.Getenv("FIREPAAS_DIAL_TARGET")
	if target == "" {
		t.Skip("helper process")
	}
	timeout, _ := time.ParseDuration(os.Getenv("FIREPAAS_DIAL_TIMEOUT"))
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	if src := os.Getenv("FIREPAAS_DIAL_SRC"); src != "" {
		if ip := net.ParseIP(src); ip != nil {
			if ip.To4() != nil {
				d.LocalAddr = &net.TCPAddr{IP: ip}
			} else {
				d.LocalAddr = &net.TCPAddr{IP: ip}
			}
		}
	}
	conn, err := d.Dial("tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if ready := os.Getenv("FIREPAAS_DIAL_READY"); ready != "" {
		_ = os.WriteFile(ready, []byte("1"), 0o600)
	}
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	if echo := os.Getenv("FIREPAAS_DIAL_ECHO"); echo != "" {
		if _, err := conn.Write([]byte(echo)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if want := os.Getenv("FIREPAAS_DIAL_WANT"); want != "" && string(buf[:n]) != want {
			t.Fatalf("reply = %q, want %q", buf[:n], want)
		}
	}
	if wantRemote := os.Getenv("FIREPAAS_DIAL_WANT_REMOTE"); wantRemote != "" {
		got := conn.RemoteAddr().String()
		if !strings.HasPrefix(got, wantRemote) {
			t.Fatalf("remote = %s, want prefix %s", got, wantRemote)
		}
	}
	if hold, _ := time.ParseDuration(os.Getenv("FIREPAAS_DIAL_HOLD")); hold > 0 {
		time.Sleep(hold)
	}
}

func dumpEbpfDebug(t *testing.T, ctx context.Context, ns, guestNS string, b *Backend, guestIP string) {
	t.Helper()
	key, _ := ipv4Key(guestIP)
	var host uint32
	_ = b.host4.Lookup(key, &host)
	t.Logf("host4[%s]=%x", guestIP, host)
	out, _ := exec.Command("ip", "netns", "exec", ns, "nft", "list", "table", "ip", "fp-slot").CombinedOutput()
	t.Logf("ns fp-slot table:\n%s", out)
	out, _ = exec.Command("ip", "netns", "exec", ns, "tc", "filter", "show", "dev", "v-ebpf-g", "egress").
		CombinedOutput()
	t.Logf("ns tc filters:\n%s", out)
	out, _ = exec.Command("tc", "filter", "show", "dev", "v-ebpf-h", "ingress").CombinedOutput()
	t.Logf("host tc filters:\n%s", out)
	// 抓包看 SYN 是否被重写并送到 host 侧（重试一次真实拨号，边拨边抓）。
	go runHelper(t, context.Background(), guestNS, dialSpec{
		target: "203.0.113.10:80", src: guestIP, timeout: 2500 * time.Millisecond,
	})
	time.Sleep(400 * time.Millisecond)
	out, _ = exec.Command("timeout", "2", "tcpdump", "-vv", "-i", "v-ebpf-h", "-n", "-c", "3").CombinedOutput()
	t.Logf("host any tcpdump:\n%s", out)
	go runHelper(t, context.Background(), guestNS, dialSpec{
		target: "203.0.113.10:80", src: guestIP, timeout: 2500 * time.Millisecond,
	})
	time.Sleep(400 * time.Millisecond)
	out, _ = exec.Command("ip", "netns", "exec", ns, "timeout", "2", "tcpdump", "-vv",
		"-i", "v-ebpf-g", "-n", "-c", "1", "tcp", "port", "80").CombinedOutput()
	t.Logf("ns veth tcpdump:\n%s", out)
}
