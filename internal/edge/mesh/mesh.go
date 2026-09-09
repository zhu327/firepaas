// Package mesh 是 edge 侧 mesh 直达数据面（ADR-0040 §14，G2d）。
//
// 组成：
//   - WG：edge hub 的 WireGuard 接口（密钥本地生成、公钥由运维配置到控制面
//     注册为具名 peer；peer 集 = mesh:peer:{node} Redis 投影，周期同步，
//     投影断联时保留当前 peer 集——WG 加密即 peer 准入）；
//   - Endpoints：mesh:endpoint:{machine}:{execution} 投影的带 TTL 缓存
//     （回源 Redis；断联窗口内降级复用 last-known-good，预算与 route/token
//     同源的 serve-stale 语义）；
//   - Direct：HTTP Transport 走 [节点 ULA]:ingress_port（凭证为唯一路由
//     依据，无 X-Firepaas-Machine/Execution 头）。任一失败（无 endpoint/
//     拨号失败）即回落 legacy :5107 路径——回滚 = 关 FIREPAAS_EDGE_MESH_DIRECT。
package mesh

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhu327/firepaas/internal/controlplane/catalog"
	"github.com/zhu327/firepaas/internal/controlplane/traffic"
)

// ---- WG ----

// WG 管理 edge hub 的 WireGuard 接口与 peer 集。
type WG struct {
	iface   string
	keyDir  string
	listen  uint16
	priv    string
	pub     string
	mu      sync.Mutex
	applied string // 已应用的 peers 指纹
	// appliedRoutes 已安装的 peer 前缀路由（setconf 不管路由：AllowedIPs
	// 只做 WG 内 cryptokey 选路，进接口的包仍需主表路由指到本接口——
	// W4 实测：无路由时 ULA 包走默认口明文外发，直达恒失败）。
	appliedRoutes map[string]bool
}

// NewWG 构造（未建接口；Ensure 幂等创建密钥/接口/peer）。
func NewWG(iface, keyDir string, listen uint16) *WG {
	if iface == "" {
		iface = "fp-edge0"
	}
	if listen == 0 {
		listen = 51821
	}
	return &WG{iface: iface, keyDir: keyDir, listen: listen, appliedRoutes: map[string]bool{}}
}

// PublicKey 返回本机 WG 公钥（base64；运维把它配置到控制面
// FIREPAAS_MESH_EDGE_PUBKEY）。密钥首次生成（0600，wg genkey）。
func (w *WG) PublicKey(ctx context.Context) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pub != "" {
		return w.pub, nil
	}
	if err := os.MkdirAll(w.keyDir, 0o700); err != nil {
		return "", fmt.Errorf("mesh: key dir: %w", err)
	}
	keyPath := w.keyDir + "/edge.key"
	raw, err := os.ReadFile(keyPath)
	switch {
	case err == nil:
		w.priv = strings.TrimSpace(string(raw))
	case os.IsNotExist(err):
		out, err := exec.CommandContext(ctx, "wg", "genkey").Output()
		if err != nil {
			return "", fmt.Errorf("mesh: genkey: %w", err)
		}
		w.priv = strings.TrimSpace(string(out))
		if err := os.WriteFile(keyPath, []byte(w.priv+"\n"), 0o600); err != nil {
			return "", fmt.Errorf("mesh: persist key: %w", err)
		}
	default:
		return "", fmt.Errorf("mesh: read key: %w", err)
	}
	if _, err := base64.StdEncoding.DecodeString(w.priv); err != nil {
		return "", fmt.Errorf("mesh: key file corrupt: %w", err)
	}
	// 私钥经 stdin 传入（不进 argv；与 agent wg.Manager 同纪律）。
	cmd := exec.CommandContext(ctx, "wg", "pubkey")
	cmd.Stdin = strings.NewReader(w.priv)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("mesh: pubkey: %v: %s", err, strings.TrimSpace(string(out)))
	}
	w.pub = strings.TrimSpace(string(out))
	return w.pub, nil
}

// SyncPeers 幂等应用 peer 集（全量替换；指纹相同跳过 wg 调用）。
// peers 为空 = 无 mesh 节点：清空 peer（保接口与密钥）。
// W4：同步维护各 peer 前缀经本接口的 v6 路由（replace 幂等）并回收已摘除
// peer 的路由；经过滤的自身记录由 SyncLoop 排除在外（不进 peer 集）。
func (w *WG) SyncPeers(ctx context.Context, peers []catalog.MeshPeerRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.priv == "" {
		return fmt.Errorf("mesh: key not initialized")
	}
	conf := w.buildPeerConfigLocked(peers)
	if conf != w.applied {
		if out, err := exec.CommandContext(ctx, "ip", "link", "show", w.iface).CombinedOutput(); err != nil {
			if _, err := exec.CommandContext(ctx, "ip", "link", "add", w.iface, "type", "wireguard").CombinedOutput(); err != nil {
				return fmt.Errorf("mesh: add %s: %w", w.iface, err)
			}
			_ = out
		}
		if out, err := runWgSetconf(ctx, w.iface, conf); err != nil {
			return fmt.Errorf("mesh: setconf: %v: %s", err, out)
		}
		// 接口 up（幂等；自地址由 SyncLoop 按本 hub 前缀配置——edge 只
		// 主动外联也需要源地址选择与回程可达）。
		_, _ = exec.CommandContext(ctx, "ip", "link", "set", w.iface, "up").CombinedOutput()
		w.applied = conf
	}
	return w.syncRoutesLocked(ctx, peers)
}

// syncRoutesLocked 按 peer 前缀装经本接口的 v6 路由并回收退役 peer 路由
// （调用方持有 w.mu；replace/del 均幂等）。
func (w *WG) syncRoutesLocked(ctx context.Context, peers []catalog.MeshPeerRecord) error {
	want := make(map[string]bool, len(peers))
	for _, p := range peers {
		if p.NodePrefix == "" {
			continue
		}
		want[p.NodePrefix] = true
		if w.appliedRoutes[p.NodePrefix] {
			continue
		}
		if out, err := exec.CommandContext(ctx, "ip", "-6", "route", "replace",
			p.NodePrefix, "dev", w.iface).CombinedOutput(); err != nil {
			return fmt.Errorf("mesh: route %s: %v: %s", p.NodePrefix, err, strings.TrimSpace(string(out)))
		}
		w.appliedRoutes[p.NodePrefix] = true
	}
	for prefix := range w.appliedRoutes {
		if !want[prefix] {
			_ = exec.CommandContext(ctx, "ip", "-6", "route", "del",
				prefix, "dev", w.iface).Run()
			delete(w.appliedRoutes, prefix)
		}
	}
	return nil
}

// buildPeerConfigLocked 构造 wg setconf 配置串（纯函数供单测断言形态）。
func (w *WG) buildPeerConfigLocked(peers []catalog.MeshPeerRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[Interface]\nPrivateKey = %s\nListenPort = %d\n", w.priv, w.listen)
	for _, p := range peers {
		fmt.Fprintf(&b, "\n[Peer]\nPublicKey = %s\nEndpoint = %s\nAllowedIPs = %s\n",
			p.Pubkey, p.Endpoint, p.NodePrefix)
	}
	return b.String()
}

func runWgSetconf(ctx context.Context, iface, conf string) (string, error) {
	cmd := exec.CommandContext(ctx, "wg", "setconf", iface, "/dev/stdin")
	cmd.Stdin = strings.NewReader(conf)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// ---- Endpoints 缓存 ----

// Endpoints 是 mesh:endpoint 投影的带 TTL 缓存（回源 Redis；断联窗口内
// last-known-good）。nil 投影（键过期/无记录）= 不缓存（每次回源——
// miss 是正常态：非 mesh_direct 服务恒 miss）。
type Endpoints struct {
	rdb   *redis.Client
	fresh time.Duration
	stale time.Duration
	// fetchFn 测试注入（nil = Redis 回源）。
	fetchFn func(ctx context.Context, machineID, executionID string) (*catalog.MeshEndpointRecord, error)

	mu    sync.Mutex
	cache map[string]endpointEntry
}

type endpointEntry struct {
	rec     catalog.MeshEndpointRecord
	fetched time.Time
	// miss = 确认无入口的负缓存（短 TTL，避免非 mesh backend 逐请求回源；
	// 与正缓存同 fresh 窗口，过期后重查）。
	miss bool
}

// NewEndpoints 构造；fresh = 缓存有效期，stale = 回源失败复用窗口
// （与 edge route/token 的 serve-stale 预算同源）。
func NewEndpoints(rdb *redis.Client, fresh, stale time.Duration) *Endpoints {
	return &Endpoints{rdb: rdb, fresh: fresh, stale: stale, cache: map[string]endpointEntry{}}
}

// Get 返回 (machine, execution) 的直达入口。ok=false = 无直达入口
// （非 mesh execution / 投影不可得）——调用方回落 legacy 路径。
func (e *Endpoints) Get(ctx context.Context, machineID, executionID string) (catalog.MeshEndpointRecord, bool) {
	key := machineID + "\x00" + executionID
	e.mu.Lock()
	if ent, ok := e.cache[key]; ok {
		if !ent.miss && time.Since(ent.fetched) < e.fresh {
			e.mu.Unlock()
			return ent.rec, true
		}
		if ent.miss && time.Since(ent.fetched) < e.fresh {
			e.mu.Unlock()
			return catalog.MeshEndpointRecord{}, false
		}
	}
	e.mu.Unlock()
	rec, err := e.fetch(ctx, machineID, executionID)
	if err != nil {
		// 回源失败：窗口内 last-known-good；超窗 fail-open 到回落。
		e.mu.Lock()
		defer e.mu.Unlock()
		if ent, ok := e.cache[key]; ok && !ent.miss && time.Since(ent.fetched) < e.stale {
			return ent.rec, true
		}
		return catalog.MeshEndpointRecord{}, false
	}
	if rec == nil {
		// 确认无直达入口：短 TTL 负缓存（miss 是正常态：非 mesh_direct
		// 服务恒 miss，不应逐请求回源 Redis）。
		e.mu.Lock()
		e.cache[key] = endpointEntry{miss: true, fetched: time.Now()}
		e.mu.Unlock()
		return catalog.MeshEndpointRecord{}, false
	}
	e.mu.Lock()
	e.cache[key] = endpointEntry{rec: *rec, fetched: time.Now()}
	e.mu.Unlock()
	return *rec, true
}

func (e *Endpoints) fetch(ctx context.Context, machineID, executionID string) (*catalog.MeshEndpointRecord, error) {
	if e.fetchFn != nil {
		return e.fetchFn(ctx, machineID, executionID)
	}
	cat := catalog.New(e.rdb)
	return cat.GetMeshEndpoint(ctx, machineID, executionID)
}

// ---- Direct transport ----

// Transport 是 mesh 直达 HTTP 载荷构造（Director 语义：改写目标为
// [节点 ULA]:ingress_port，凭证头为唯一身份，剥离全部 X-Firepaas 路由头）。
type Transport struct {
	Endpoints *Endpoints
	// HTTP 是底层 Transport（连接池；拨号经 edge WG 路由）。
	HTTP *http.Transport
}

// NewTransport 构造（连接池参数与 legacy 代理路径同阶）。
func NewTransport(ep *Endpoints) *Transport {
	return &Transport{
		Endpoints: ep,
		HTTP: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     30 * time.Second,
			// 直达不可达必须快速回落 legacy（节点失联/WG 断链时 SYN 无
			// RST，OS 默认重试 ~130s 会吞掉整个请求预算）——真机 G2d 验收
			// 抓到回落超时。拨号 3s + 首字节 30s（半开连接兜底；冷启动
			// 首字节慢于此的走回落/终态语义，见 handler 回落规则）。
			DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
			// 直达终结器是明文 HTTP（承载于 WG 加密之上；与 agent proxy
			// mTLS 的对等性 = WG peer 准入 + 凭证 HMAC）。
		},
	}
}

// RoundTrip 拦截式直达：请求 Host 形如 "mesh-direct"（占位），本方法按
// context 携带的 (machine, execution, credential, appPort) 改写目标并放行。
// 无入口/构造失败 → 返回 ErrNoDirect（调用方回落 legacy）。
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	d, ok := req.Context().Value(directKey{}).(DirectInfo)
	if !ok {
		return nil, ErrNoDirect
	}
	rec, ok := t.Endpoints.Get(req.Context(), d.MachineID, d.ExecutionID)
	if !ok {
		return nil, ErrNoDirect
	}
	out := req.Clone(req.Context())
	out.URL.Scheme = "http"
	out.URL.Host = net.JoinHostPort(rec.NodeULA, fmt.Sprintf("%d", rec.IngressPort))
	out.Host = out.URL.Host
	// 凭证是唯一路由依据：剥离全部 edge→agent 路由头，只留凭证与端口参数。
	for _, h := range []string{"X-Firepaas-Machine-ID", "X-Firepaas-Execution-ID", "X-Firepaas-Pin-Machine"} {
		out.Header.Del(h)
	}
	if d.Credential != "" {
		out.Header.Set(traffic.HeaderCredential, d.Credential)
	} else {
		out.Header.Del(traffic.HeaderCredential)
	}
	if d.AppPort > 0 {
		out.Header.Set("X-Firepaas-App-Port", fmt.Sprintf("%d", d.AppPort))
	} else {
		out.Header.Del("X-Firepaas-App-Port")
	}
	return t.HTTP.RoundTrip(out)
}

// DirectInfo 是直达请求的路由参数（context 携带）。
type DirectInfo struct {
	MachineID   string
	ExecutionID string
	Credential  string
	AppPort     int
}

type directKey struct{}

// WithDirectInfo 把直达路由参数挂入 context（edge handler → Transport）。
func WithDirectInfo(ctx context.Context, d DirectInfo) context.Context {
	return context.WithValue(ctx, directKey{}, d)
}

// ErrNoDirect 表示无直达入口/不可达——调用方回落 legacy :5107 路径。
var ErrNoDirect = fmt.Errorf("mesh: no direct endpoint")

// SyncLoop 周期同步 WG peer 集（mesh:peer 投影 → wg setconf + 路由）。
// 投影读失败保留当前 peer 集（降级；下轮重试）。间隔默认 10s。
// edgeID 是本 hub 在控制面的注册 ID（默认 edge-hub）：自身记录用于配置
// 本接口 ULA 地址（源地址选择与回程可达），不加入 peer 集。
func SyncLoop(ctx context.Context, wg *WG, rdb *redis.Client, every time.Duration, edgeID string) {
	if edgeID == "" {
		edgeID = "edge-hub"
	}
	if every <= 0 {
		every = 10 * time.Second
	}
	cat := catalog.New(rdb)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		peers, err := cat.ListMeshPeers(ctx)
		if err != nil {
			slog.Warn("edge mesh peers unavailable, keeping current set", "error", err)
		} else {
			self, filtered := splitSelfPeer(peers, edgeID)
			// 自身记录：配本接口 ULA（前缀基址；replace 幂等），不进 peer 集。
			// 失败记 Warn：自地址装不上时直达静默全失败（回落掩盖，排障困难）。
			if self != nil {
				addr := selfULAAddr(self.NodePrefix)
				if addr == "" {
					slog.Warn("edge mesh self prefix unparsable, skipping self addr",
						"edge_id", edgeID, "prefix", self.NodePrefix)
				} else if err := exec.CommandContext(ctx, "ip", "-6", "addr", "replace",
					addr, "dev", wg.iface).Run(); err != nil {
					slog.Warn("edge mesh self addr failed, direct may silently fail",
						"edge_id", edgeID, "addr", addr, "iface", wg.iface, "error", err)
				}
			}
			if err := wg.SyncPeers(ctx, filtered); err != nil {
				slog.Warn("edge mesh sync peers", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// splitSelfPeer 从 peer 投影中分离自身记录（纯函数，可单测）：自身用于
// 配置本接口 ULA，不加入 WG peer 集（peer 记录是给节点用的）。edgeID 两端
// 必须一致（控制面注册 ID vs FIREPAAS_MESH_EDGE_ID）；不一致时自身记录留
// 在 peer 集内（fail-closed：不配自地址、不断 peer，排障见 Warn 日志）。
func splitSelfPeer(peers []catalog.MeshPeerRecord, edgeID string) (*catalog.MeshPeerRecord, []catalog.MeshPeerRecord) {
	var self *catalog.MeshPeerRecord
	filtered := make([]catalog.MeshPeerRecord, 0, len(peers))
	for i, p := range peers {
		if p.NodeID == edgeID {
			if self == nil && p.NodePrefix != "" {
				self = &peers[i]
			}
			continue
		}
		filtered = append(filtered, p)
	}
	return self, filtered
}

// selfULAAddr 取节点前缀基址（含 /64 后缀，供 ip addr replace）。
func selfULAAddr(prefix string) string {
	ip, err := netip.ParsePrefix(prefix)
	if err != nil {
		return ""
	}
	return ip.Addr().String() + "/64"
}

// ---- 测试辅助（包内使用）----

// NewEndpointsForTest 构造固定入口的 Endpoints（测试注入；ula 为空 = 恒
// miss——模拟无 mesh:endpoint 投影）。导出仅供 edge 包测试复用。
func NewEndpointsForTest(ula string, port int) *Endpoints {
	return &Endpoints{
		fresh: time.Minute, stale: time.Minute,
		cache: map[string]endpointEntry{},
		fetchFn: func(ctx context.Context, machineID, executionID string) (*catalog.MeshEndpointRecord, error) {
			if ula == "" {
				return nil, nil
			}
			return &catalog.MeshEndpointRecord{
				MachineID: machineID, ExecutionID: executionID,
				NodeULA: ula, IngressPort: port,
			}, nil
		},
	}
}

// NewTransportForTest 构造直达 Transport（Endpoints 由测试注入）。
func NewTransportForTest(ep *Endpoints) *Transport {
	t := NewTransport(ep)
	return t
}
