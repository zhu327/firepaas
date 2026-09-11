package ebpf

// TestEbpfEgressPerSlotIsolation 双 slot egress 隔离回归（W1 P0）：
// A=deny_all、B=unrestricted 同时生效且互不覆盖；交换后反向成立。
// 旧全局表实现下后一次 ApplyEgress 会清掉前一次的策略，本测试必失败。
//
//	sudo FIREPAAS_TEST_NETNS=1 go test ./internal/agent/network/ebpf/ -run TestEbpfEgressPerSlotIsolation -v

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cilium/ebpf"
	"github.com/zhu327/firepaas/internal/agent/network/api"
	"github.com/zhu327/firepaas/internal/agent/network/slot"
)

func TestEbpfEgressPerSlotIsolation(t *testing.T) {
	if os.Getenv("FIREPAAS_TEST_NETNS") != "1" {
		t.Skip("set FIREPAAS_TEST_NETNS=1 (root) to run kernel networking test")
	}
	if os.Getuid() != 0 {
		t.Fatal("must run as root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		nsA, nsB   = "fp-ebpf-iso-a", "fp-ebpf-iso-b"
		gA, gB     = "fp-ebpf-iso-ga", "fp-ebpf-iso-gb"
		vhA, vgA   = "v-iso-ah", "v-iso-ag"
		vhB, vgB   = "v-iso-bh", "v-iso-bg"
		vhGA, vgGA = "v-iso-gah", "v-iso-gag"
		vhGB, vgGB = "v-iso-gbh", "v-iso-gbg"
		brA, brB   = "fp-br-iso-a", "fp-br-iso-b"
		guestA     = "10.101.91.5"
		guestB     = "10.101.92.5"
		target     = "11.11.12.9:8080"
		targetIP   = "11.11.12.9/32"
		// v6 身份面：guestA 拥有 ulaA，guestB 拥有 ulaB；ulaC 是远端
		// identity 3（仅存在于 ipcache/policy，用于冒用源测试）。
		ulaA      = "fd7a:9a55:91::5"
		ulaB      = "fd7a:9a55:92::5"
		ulaC      = "fd7a:9a55:93::5"
		udpOK     = 39998
		udpNo     = 39997
		tcpOK     = 39996
		proxyPort = 38080
	)
	for _, c := range [][]string{
		{"ip", "netns", "del", nsA},
		{"ip", "netns", "del", nsB},
		{"ip", "netns", "del", gA},
		{"ip", "netns", "del", gB},
		{"ip", "link", "del", vhA},
		{"ip", "link", "del", vhB},
		{"ip", "link", "del", vhGA},
		{"ip", "link", "del", vhGB},
	} {
		_ = exec.Command(c[0], c[1:]...).Run()
	}
	t.Cleanup(func() {
		for _, c := range [][]string{
			{"ip", "netns", "del", nsA},
			{"ip", "netns", "del", nsB},
			{"ip", "netns", "del", gA},
			{"ip", "netns", "del", gB},
			{"ip", "link", "del", vhA},
			{"ip", "link", "del", vhB},
			{"ip", "link", "del", vhGA},
			{"ip", "link", "del", vhGB},
			{"ip", "addr", "del", targetIP, "dev", "lo"},
		} {
			_ = exec.Command(c[0], c[1:]...).Run()
		}
	})

	setup := func(ns, g, vh, vg, vhG, vgG, br, linkNet, guest, gw string) {
		steps := [][]string{
			{"ip", "netns", "add", ns},
			{"ip", "netns", "add", g},
			{"ip", "link", "add", vh, "type", "veth", "peer", "name", vg},
			{"ip", "link", "set", vg, "netns", ns},
			{"ip", "link", "add", vhG, "type", "veth", "peer", "name", vgG},
			{"ip", "link", "set", vgG, "netns", ns},
			{"ip", "link", "set", vhG, "netns", g},
			{"ip", "netns", "exec", ns, "ip", "link", "add", br, "type", "bridge"},
			{"ip", "netns", "exec", ns, "ip", "addr", "add", gw + "/24", "dev", br},
			{"ip", "netns", "exec", ns, "ip", "link", "set", vgG, "master", br},
			{"ip", "netns", "exec", ns, "ip", "link", "set", br, "up"},
			{"ip", "netns", "exec", ns, "ip", "link", "set", vgG, "up"},
			{"ip", "addr", "add", linkNet + ".1/24", "dev", vh},
			{"ip", "link", "set", vh, "up"},
			{"ip", "netns", "exec", ns, "ip", "addr", "add", linkNet + ".2/24", "dev", vg},
			{"ip", "netns", "exec", ns, "ip", "link", "set", vg, "up"},
			{"ip", "netns", "exec", ns, "ip", "link", "set", "lo", "up"},
			{"ip", "netns", "exec", ns, "ip", "route", "replace", "default", "via", linkNet + ".1"},
			{"ip", "netns", "exec", g, "ip", "addr", "add", guest + "/24", "dev", vhG},
			{"ip", "netns", "exec", g, "ip", "link", "set", vhG, "up"},
			{"ip", "netns", "exec", g, "ip", "link", "set", "lo", "up"},
			{"ip", "route", "replace", guest + "/32", "via", linkNet + ".2", "dev", vh},
		}
		for _, c := range steps {
			if out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput(); err != nil {
				t.Fatalf("%v: %v: %s", c, err, out)
			}
		}
		for _, c := range [][]string{
			{"ip", "netns", "exec", g, "ip", "route", "replace", "default", "via", gw, "dev", vhG, "onlink"},
			{"ip", "netns", "exec", ns, "sysctl", "-w", "net.ipv4.ip_forward=1"},
			{"ip", "netns", "exec", ns, "sysctl", "-w", "net.ipv6.conf.all.forwarding=1"},
		} {
			_ = exec.Command(c[0], c[1:]...).Run()
		}
	}
	setup(nsA, gA, vhA, vgA, vhGA, vgGA, brA, "10.13.91", guestA, "10.101.91.1")
	setup(nsB, gB, vhB, vgB, vhGB, vgGB, brB, "10.13.92", guestB, "10.101.92.1")
	_ = exec.Command("sysctl", "-w", "net.ipv4.ip_forward=1").Run()

	pinDir := fmt.Sprintf("/sys/fs/bpf/firepaas-iso-%d", os.Getpid())
	_ = exec.Command("umount", pinDir).Run()
	_ = os.Remove(pinDir)
	t.Cleanup(func() {
		_ = exec.Command("umount", pinDir).Run()
		_ = os.Remove(pinDir)
	})
	// 代理端口显式配置（默认 18080 可能被同主机 lab 占用）：用于验证
	// ingress 对代理端口的放行只限本机目的，跨 slot 目标仍走 private drop。
	b, err := New(Options{
		PinDir:         pinDir,
		EgressProxy80:  proxyPort,
		EgressProxy443: proxyPort + 2,
		VethCIDR:       "10.13.91.0/24",
		RootNATTable:   fmt.Sprintf("fp-egress-test-%d", os.Getpid()),
		// 测试需看到每个 allow 事件（生产默认 1/128 采样）。
		FlowSampleEvery: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = exec.Command("nft", "delete", "table", "ip", fmt.Sprintf("fp-egress-test-%d", os.Getpid())).Run()
	})
	refA := slot.SlotRef{
		Index: 0, VethHost: vhA, VethGuest: vgA,
		HostAddr: "10.13.91.1", NsAddr: "10.13.91.2", Netns: nsA, GuestIP: guestA,
		GuestIP6: ulaA,
	}
	refB := slot.SlotRef{
		Index: 1, VethHost: vhB, VethGuest: vgB,
		HostAddr: "10.13.92.1", NsAddr: "10.13.92.2", Netns: nsB, GuestIP: guestB,
		GuestIP6: ulaB,
	}
	if err := b.EnsureNode(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []slot.SlotRef{refA, refB} {
		if err := b.AttachSlot(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if err := b.EnsureSlotNAT(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}

	_ = exec.Command("ip", "addr", "add", targetIP, "dev", "lo").Run()
	ln, err := net.Listen("tcp", target)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
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
	}()

	// A=deny_all、 B=unrestricted：必须同时成立（旧全局表下 B 的 apply 会
	// 清掉 A 的 deny，A 拨测将错误通过）。
	if err := b.ApplyEgress(ctx, refA, &api.PolicySnapshot{Mode: "deny_all", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.ApplyEgress(ctx, refB, &api.PolicySnapshot{Mode: "unrestricted", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	dumpEgressState(t, b, refA, refB)
	if out := helperOutput(t, ctx, gA, dialSpec{target: target, timeout: 1200 * time.Millisecond}); !strings.Contains(
		out,
		"timeout",
	) {
		t.Fatalf("slot A (deny_all) dial err = %s, want timeout (must stay denied after B applied)", out)
	}
	if !runHelper(
		t,
		ctx,
		gB,
		dialSpec{target: target, src: guestB, timeout: 5 * time.Second, echo: "ping", want: "echo"},
	) {
		t.Fatal("slot B (unrestricted) dial must pass")
	}

	// 交换：A=unrestricted、B=deny_all，反向成立。
	if err := b.ApplyEgress(ctx, refA, &api.PolicySnapshot{Mode: "unrestricted", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if err := b.ApplyEgress(ctx, refB, &api.PolicySnapshot{Mode: "deny_all", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if !runHelper(
		t,
		ctx,
		gA,
		dialSpec{target: target, src: guestA, timeout: 5 * time.Second, echo: "ping", want: "echo"},
	) {
		t.Fatal("slot A (unrestricted) dial must pass after swap")
	}
	if out := helperOutput(t, ctx, gB, dialSpec{target: target, timeout: 1200 * time.Millisecond}); !strings.Contains(
		out,
		"timeout",
	) {
		t.Fatalf("slot B (deny_all) dial err = %s, want timeout (must stay denied after A applied)", out)
	}

	// T1（P0）：代理端口放行只限本机目的。A→B 的代理端口（跨 slot、dst 为
	// 私网）必须被 private4 drop（timeout）；fresh eBPF 节点没有遗留 nft
	// FORWARD 兜底，旧实现里该包会直达 B（无监听 → refused）。
	if out := helperOutput(t, ctx, gA, dialSpec{
		target: guestB + fmt.Sprintf(":%d", proxyPort), timeout: 1200 * time.Millisecond,
	}); !strings.Contains(out, "timeout") {
		t.Fatalf("cross-slot proxy port must be dropped by private4, got %s", out)
	}

	// 6) v6 UDP 逐端口白名单（W1）：forward 规则有 ports → 逐包检查；回包凭
	// 对向纯回程条目（has_ports=0）放行；未放行端口 deny。topology：guestA
	// ulaA → guestB ulaB，经 vhA/vhB ingress 策决（与主测试同构，独立 /64）。
	// （ulaA/ulaB/udpOK/udpNo 已在上方 const 块声明。）
	for _, c := range [][]string{
		{"ip", "netns", "exec", nsA, "ip", "-6", "addr", "add", "fd7a:9a55:fd91::2/64", "dev", vgA, "nodad"},
		{"ip", "netns", "exec", nsA, "ip", "-6", "addr", "add", "fd7a:9a55:fd91::3/64", "dev", brA, "nodad"},
		{"ip", "netns", "exec", nsB, "ip", "-6", "addr", "add", "fd7a:9a55:fd92::2/64", "dev", vgB, "nodad"},
		{"ip", "netns", "exec", nsB, "ip", "-6", "addr", "add", "fd7a:9a55:fd92::3/64", "dev", brB, "nodad"},
		{"ip", "-6", "addr", "add", "fd7a:9a55:fd91::1/64", "dev", vhA, "nodad"},
		{"ip", "-6", "addr", "add", "fd7a:9a55:fd92::1/64", "dev", vhB, "nodad"},
		{"ip", "netns", "exec", gA, "ip", "-6", "addr", "add", "fd7a:9a55:fd91::5/64", "dev", vhGA, "nodad"},
		{"ip", "netns", "exec", gA, "ip", "-6", "addr", "add", ulaA + "/128", "dev", vhGA, "nodad"},
		{"ip", "netns", "exec", gB, "ip", "-6", "addr", "add", "fd7a:9a55:fd92::5/64", "dev", vhGB, "nodad"},
		{"ip", "netns", "exec", gB, "ip", "-6", "addr", "add", ulaB + "/128", "dev", vhGB, "nodad"},
		{"ip", "netns", "exec", gA, "ip", "-6", "route", "replace", "default", "via", "fd7a:9a55:fd91::3"},
		{"ip", "netns", "exec", gB, "ip", "-6", "route", "replace", "default", "via", "fd7a:9a55:fd92::3"},
		{"ip", "netns", "exec", nsA, "ip", "-6", "route", "replace", ulaA + "/128", "dev", brA},
		{"ip", "netns", "exec", nsB, "ip", "-6", "route", "replace", ulaB + "/128", "dev", brB},
		{"ip", "-6", "route", "replace", ulaA + "/128", "via", "fd7a:9a55:fd91::2", "dev", vhA},
		{"ip", "-6", "route", "replace", ulaB + "/128", "via", "fd7a:9a55:fd92::2", "dev", vhB},
		{"ip", "netns", "exec", nsA, "ip", "-6", "route", "replace", ulaB + "/128", "via", "fd7a:9a55:fd91::1", "dev", vgA},
		{"ip", "netns", "exec", nsB, "ip", "-6", "route", "replace", ulaA + "/128", "via", "fd7a:9a55:fd92::1", "dev", vgB},
	} {
		if out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", c, err, out)
		}
	}
	t.Cleanup(func() {
		for _, c := range [][]string{
			{"ip", "-6", "route", "del", ulaA + "/128", "via", "fd7a:9a55:fd91::2", "dev", vhA},
			{"ip", "-6", "route", "del", ulaB + "/128", "via", "fd7a:9a55:fd92::2", "dev", vhB},
			{"ip", "-6", "addr", "del", "fd7a:9a55:fd91::1/64", "dev", vhA},
			{"ip", "-6", "addr", "del", "fd7a:9a55:fd92::1/64", "dev", vhB},
		} {
			_ = exec.Command(c[0], c[1:]...).Run()
		}
	})
	if err := b.ApplyFabricPolicy(ctx, api.FabricPolicySnapshot{
		Generation: 1,
		Identities: []api.IdentityMapEntry{
			{IdentityID: 1, ULA: ulaA},
			{IdentityID: 2, ULA: ulaB},
			{IdentityID: 3, ULA: ulaC},
		},
		Entries: []api.FabricPolicyEntry{
			{SrcIdentity: 1, DstIdentity: 2, Generation: 1, Ports: []uint32{udpOK, tcpOK}},
			// 对称回程条目：端口集按源端口匹配（SrcPorts）。
			{SrcIdentity: 2, DstIdentity: 1, Generation: 1, Ports: []uint32{udpOK, tcpOK}, SrcPorts: true},
			// B→A 也声明（双向 EastWest）：其正向 + 对称回程与 entry1 的
			// 正/回程叠加后，(1,2)/(2,1) 都合并出 DPORT|SPORT；回归“A 发起
			// 的 TCP 已建立流（ACK/数据）不得因 sport 不匹配被丢”。
			{SrcIdentity: 2, DstIdentity: 1, Generation: 1, Ports: []uint32{tcpOK}},
			{SrcIdentity: 1, DstIdentity: 2, Generation: 1, Ports: []uint32{tcpOK}, SrcPorts: true},
			// 冒用源测试用：identity 2 到 identity 3 的放行。没有 per-veth
			// 源绑定时，slot A 伪造 ulaB 源可借 identity 2 通过该条目。
			{SrcIdentity: 2, DstIdentity: 3, Generation: 1, Ports: []uint32{udpOK}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	udpEcho := fmt.Sprintf("import socket\n"+
		"s=socket.socket(socket.AF_INET6,socket.SOCK_DGRAM)\n"+
		"s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"+
		"s.bind(('%s',%%d))\n"+
		"import sys\nprint('READY',flush=True)\n"+
		"d,a=s.recvfrom(64)\n"+
		"s.sendto(b'udp-ok',a)\n", ulaB)
	srv := exec.CommandContext(ctx, "ip", "netns", "exec", gB, "python3", "-c", fmt.Sprintf(udpEcho, udpOK))
	srvOut, _ := srv.StdoutPipe()
	if err := srv.Start(); err != nil {
		t.Fatalf("udp echo server: %v", err)
	}
	defer func() { _ = srv.Process.Kill() }()
	// 等服务端 bind 就绪（DAD tentative 窗口内 bind 会 EADDRNOTAVAIL，用 READY
	// 行同步替代固定 sleep）。
	ready := make(chan struct{})
	go func() {
		buf := make([]byte, 16)
		for {
			n, err := srvOut.Read(buf)
			if err != nil {
				return
			}
			if strings.Contains(string(buf[:n]), "READY") {
				close(ready)
				return
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("udp echo server never ready")
	}
	// flow 收集器：诊断 v6 决策（ping/UDP 失败时看 verdict 区分 policy 与 underlay）。
	flows := &flowCollector{}
	if err := b.StartFlows(ctx, fakeResolver{}, flows); err != nil {
		t.Logf("flow events unavailable: %v", err)
	}
	// ipcache 内容自检（deny_src 时区分“没写入”与“内核查不到”）。
	{
		ipcache, _, _, _ := b.fabricSet(b.activeFabricSet())
		it := ipcache.Iterate()
		var k [16]byte
		var v uint32
		for it.Next(&k, &v) {
			t.Logf("ipcache[%x]=%d", k, v)
		}
		if err := it.Err(); err != nil {
			t.Logf("ipcache iterate: %v", err)
		}
	}
	// 先断言 v6 underlay 可达（ping 必须 -I 绑定 ULA 源，否则源绑定按预期丢弃；
	// ping 走 policy entry，不涉及端口白名单；先证路由/ND 通，再证 UDP 端口语义）。
	if out, err := exec.CommandContext(ctx, "ip", "netns", "exec", gA,
		"ping", "-6", "-c2", "-W2", "-I", ulaA, ulaB).CombinedOutput(); err != nil {
		for _, dc := range [][]string{
			{"ip", "netns", "exec", gA, "ip", "-6", "route", "get", ulaB},
			{"ip", "netns", "exec", gA, "ip", "-6", "neigh", "show"},
			{"ip", "netns", "exec", nsA, "ip", "-6", "route", "get", ulaB},
			{"ip", "netns", "exec", nsA, "ip", "-6", "neigh", "show"},
			{"ip", "-6", "route", "get", ulaB},
			{"ip", "-6", "neigh", "show"},
			{"ip", "netns", "exec", nsB, "ip", "-6", "route", "get", ulaA},
			{"ip", "netns", "exec", nsB, "ip", "-6", "neigh", "show"},
		} {
			dout, _ := exec.CommandContext(ctx, dc[0], dc[1:]...).CombinedOutput()
			t.Logf("%v => %s", dc, strings.TrimSpace(string(dout)))
		}
		t.Logf("flow events during ping: %s", flows.summary())
		t.Fatalf("v6 underlay unreachable guestA→guestB: %v: %s (routing/ND, not policy)", err, out)
	}
	udpClient := func(port int) string {
		var out []byte
		// 客户端 bind 同样受 DAD 影响，有界重试 3 次。
		for i := 0; i < 3; i++ {
			out, _ = exec.CommandContext(ctx, "ip", "netns", "exec", gA, "python3", "-c",
				fmt.Sprintf("import socket\ns=socket.socket(socket.AF_INET6,socket.SOCK_DGRAM)\n"+
					"s.bind(('%s',0))\ns.settimeout(1.2)\n"+
					"s.sendto(b'q',('%s',%d))\n"+
					"print(s.recv(64).decode())\n", ulaA, ulaB, port)).CombinedOutput()
			if !strings.Contains(string(out), "Cannot assign requested address") {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		return string(out)
	}
	if out := udpClient(udpOK); !strings.Contains(out, "udp-ok") {
		t.Fatalf("udp allowed port err = %q, want udp-ok (reply via return entry)", out)
	}
	if out := udpClient(udpNo); strings.Contains(out, "udp-ok") {
		t.Fatalf("udp denied port unexpectedly passed: %q", out)
	}

	// 双向声明下的 TCP 已建立流（W2 回归）：A→B 的 ACK/数据 sport 是 A 的
	// 临时端口，只查 SPORT 会把连接打在 SYN 之后丢掉（表现为握手超时）。
	tcpSrv := exec.CommandContext(ctx, "ip", "netns", "exec", gB, "python3", "-c",
		fmt.Sprintf("import socket\n"+
			"s=socket.socket(socket.AF_INET6,socket.SOCK_STREAM)\n"+
			"s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"+
			"s.bind(('%s',%d))\ns.listen(4)\n"+
			"import sys\nprint('READY',flush=True)\n"+
			"while True:\n"+
			" c,_=s.accept()\n"+
			" d=c.recv(64)\n"+
			" c.sendall(b'pong')\n"+
			" c.close()\n", ulaB, tcpOK))
	tcpOut, _ := tcpSrv.StdoutPipe()
	if err := tcpSrv.Start(); err != nil {
		t.Fatalf("tcp echo server: %v", err)
	}
	defer func() { _ = tcpSrv.Process.Kill() }()
	tcpReady := make(chan struct{})
	go func() {
		buf := make([]byte, 16)
		for {
			n, err := tcpOut.Read(buf)
			if err != nil {
				return
			}
			if strings.Contains(string(buf[:n]), "READY") {
				close(tcpReady)
				return
			}
		}
	}()
	select {
	case <-tcpReady:
	case <-time.After(5 * time.Second):
		t.Fatal("tcp echo server never ready")
	}
	if !runHelper(t, ctx, gA, dialSpec{
		target: "[" + ulaB + "]:" + fmt.Sprint(tcpOK), src: ulaA,
		timeout: 5 * time.Second, echo: "ping", want: "pong",
	}) {
		t.Fatalf("bidirectional TCP flow must stay established (flow: %s)", flows.summary())
	}

	// T2（P0）：per-veth v6 源绑定。模拟恶意 workload：slot A 把 slot B 的
	// ULA 绑到自己接口上（guest 本身即 root，可直接伪造源），向 ulaC 发
	// UDP——无绑定时该包以 identity 2 通过 {2→3} 放行条目（verdict=allow
	// 且 src=2/dst=3）；有绑定时必须在 tc_ingress 判为 deny_src（ipcache
	// 命中不再是源绑定的充分条件）。断言只看新产生的 proto=17 事件，
	// 不依赖环境 ICMPv6/ND 噪声的计数。
	start := flows.count()
	if out, err := exec.CommandContext(ctx, "ip", "netns", "exec", gA, "ip", "-6", "addr", "add",
		ulaB+"/128", "dev", vhGA, "nodad").CombinedOutput(); err != nil {
		t.Fatalf("spoof source addr: %v: %s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("ip", "netns", "exec", gA, "ip", "-6", "addr", "del",
			ulaB+"/128", "dev", vhGA).Run()
	})
	// 让 spoof 包真正到达 vhA：nsA 无 ulaC 路由时会在 netns 内直接 no-route
	// 丢弃，测不到 tc_ingress。root 用 blackhole 收尾避免回 ICMPv6 干扰。
	for _, c := range [][]string{
		{"ip", "netns", "exec", nsA, "ip", "-6", "route", "replace", ulaC + "/128", "via", "fd7a:9a55:fd91::1", "dev", vgA},
		{"ip", "-6", "route", "replace", "blackhole", ulaC + "/128"},
	} {
		if out, err := exec.CommandContext(ctx, c[0], c[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("%v: %v: %s", c, err, out)
		}
	}
	t.Cleanup(func() {
		_ = exec.Command("ip", "-6", "route", "del", "blackhole", ulaC+"/128").Run()
	})
	spoofOut, _ := exec.CommandContext(ctx, "ip", "netns", "exec", gA, "python3", "-c",
		fmt.Sprintf("import socket\ns=socket.socket(socket.AF_INET6,socket.SOCK_DGRAM)\n"+
			"s.bind(('%s',0))\ns.settimeout(0.5)\n"+
			"try:\n s.sendto(b'x',('%s',%d))\nexcept Exception as e:\n print('ERR',e)\n"+
			"print('sent')\n", ulaB, ulaC, udpOK)).CombinedOutput()
	if !strings.Contains(string(spoofOut), "sent") {
		t.Fatalf("spoof client did not send: %s", spoofOut)
	}
	deadline := time.Now().Add(3 * time.Second)
	verdict, seen := FlowVerdict(0), false
	for time.Now().Before(deadline) {
		if v, ok := flows.spoofVerdict(start, 3); ok {
			verdict, seen = v, true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("spoof UDP never reached tc_ingress (flow: %s)", flows.summary())
	}
	if verdict != FlowDenySrc {
		t.Fatalf("spoofed ULA source not rejected by per-veth binding: verdict=%s (flow: %s)",
			verdict, flows.summary())
	}
}

// flowCollector 测试用 flow 汇点（verdict 诊断）。
type flowCollector struct {
	mu   sync.Mutex
	recs []FlowRecord
}

func (c *flowCollector) Observe(rec FlowRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, rec)
}

func (c *flowCollector) summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := map[string]int{}
	for _, r := range c.recs {
		counts[r.Verdict.String()]++
	}
	return fmt.Sprintf("%d events %v", len(c.recs), counts)
}

func (c *flowCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recs)
}

// spoofVerdict 在 start 之后的事件里找冒用测试的 UDP 决策：
// proto=17 且（deny_src 或 allow 且 dstID=期望的 dst identity）。
func (c *flowCollector) spoofVerdict(start int, dstID uint32) (FlowVerdict, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := start; i < len(c.recs); i++ {
		r := c.recs[i]
		if r.Proto != 17 {
			continue
		}
		if r.Verdict == FlowDenySrc || (r.Verdict == FlowAllow && r.DstID == dstID) {
			return r.Verdict, true
		}
	}
	return 0, false
}

func dumpEgressState(t *testing.T, b *Backend, refs ...slot.SlotRef) {
	t.Helper()
	for _, ref := range refs {
		out, _ := exec.Command("tc", "filter", "show", "dev", ref.VethHost, "ingress").CombinedOutput()
		t.Logf("%s ingress filters:\n%s", ref.VethHost, out)
		out, _ = exec.Command("ip", "netns", "exec", ref.Netns, "tc", "filter", "show", "dev", ref.VethGuest, "egress").
			CombinedOutput()
		t.Logf("%s egress filters:\n%s", ref.VethGuest, out)
		key, _ := slotEgressKey(ref)
		var mode egressModeVal
		if err := b.egressMode.Lookup(key, &mode); err != nil {
			t.Logf("egress_mode[%s]: lookup err %v", ref.NsAddr, err)
		} else {
			t.Logf("egress_mode[%s]=%+v", ref.NsAddr, mode)
		}
		for _, dd := range []struct {
			name  string
			outer *ebpf.Map
		}{
			{"allow", b.egressAllow},
			{"deny", b.egressDeny},
		} {
			var innerID uint32
			if err := dd.outer.Lookup(key, &innerID); err != nil {
				t.Logf("egress_%s[%s]: outer lookup err %v", dd.name, ref.NsAddr, err)
				continue
			}
			t.Logf("egress_%s[%s]: inner id %d", dd.name, ref.NsAddr, innerID)
		}
	}
}
