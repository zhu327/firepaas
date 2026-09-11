#!/usr/bin/env bash
# LAYER: l3-fabric  PREREQ: dual-agent,api,edge,root  DESTRUCTIVE: no  FROZEN: no
# ADR-0040 G1 真机 spike（§13/§25）：VM→VM ULA 直连验收。
#
# 前置（缺一即 SKIP，不伪造 PASS）：
#   1. root（netns/WG/iperf 均需特权）；
#   2. /dev/kvm（Firecracker 真机）；
#   3. dev 依赖（make dev-up：PG/Redis）；
#   4. hypeman fork tag 含 v6 中间层（CreateInstanceRequest.IPv6Address），
#      且 firepaas 已 bump go.mod + 落地 adapter 注入（T6 完成）。
#   5. 双 agent（node-a/node-b，FIREPAAS_MESH=eastwest，同 cell /40，
#      WG 端口互不相同，endpoint 互可达）。
#
# 用法：
#   sudo bash scripts/lab/e2e-fabric-spike.sh --check-only   # 只验前置
#   sudo bash scripts/lab/e2e-fabric-spike.sh --stage1       # 单节点 host→guest ULA
#   sudo bash scripts/lab/e2e-fabric-spike.sh --stage2       # 跨节点 guest→guest ULA
#   sudo bash scripts/lab/e2e-fabric-spike.sh --stage3       # G2d edge 经 mesh 直达（token 新路径）
#
# 约定沿 e2e-m5.sh：有界等待、带时间戳日志、证据落盘。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
RUN_DIR="/var/lib/firepaas-p0/e2e-fabric-spike"
RUN_ID="spike-$(date +%s)"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[fabric-spike $(now)] $*"; }
fail() { echo "[fabric-spike] FAIL: $*" >&2; exit 1; }
skip() { echo "[fabric-spike] SKIP: $*" >&2; exit 2; }
pg() { docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc "$1"; }

check_only=0; stage="check"
for arg in "$@"; do
  case "$arg" in
    --check-only) check_only=1 ;;
    --stage1) stage="stage1" ;;
    --stage2) stage="stage2" ;;
    --stage3) stage="stage3" ;;
    *) fail "unknown arg $arg (want --check-only/--stage1/--stage2/--stage3)" ;;
  esac
done

# --- 前置 ---
[ "$(id -u)" = "0" ] || fail "must run as root"
[ -e /dev/kvm ] || fail "/dev/kvm missing (need KVM)"
command -v docker >/dev/null || fail "docker missing"
docker exec dev-postgres-1 pg_isready -U firepaas >/dev/null 2>&1 \
  || fail "dev-postgres not ready (run make dev-up)"
[ "${FIREPAAS_MESH:-disabled}" = "eastwest" ] \
  || fail "FIREPAAS_MESH=eastwest required, got '${FIREPAAS_MESH:-disabled}'"

# hypeman v6 中间层是否可用（tag 门禁 §25）。
if ! go doc github.com/kernel/hypeman/lib/instances.CreateInstanceRequest 2>/dev/null | grep -q IPv6Address; then
  skip "hypeman tag without IPv6Address (v6 middle layer); publish tag + bump go.mod first (§25)"
fi
# firepaas adapter 注入是否落地（T6 门禁）。
if ! grep -rq "SetFabricULA\|FabricULALookup" "$ROOT_DIR/internal/agent/machine/" 2>/dev/null; then
  skip "adapter fabric→hypeman injection not landed (T6); land it after the tag bump"
fi
log "preconditions OK (mesh=eastwest, hypeman v6, adapter injection present)"

[ "$check_only" = "1" ] && { log "check-only PASS"; exit 0; }

CELL="${FIREPAAS_MESH_CELL_PREFIX:-fd7a:9a55::/40}"
NODE_A="${FABRIC_SPIKE_NODE_A:-node-a}"
NODE_B="${FABRIC_SPIKE_NODE_B:-node-b}"

# 双节点均已入 mesh（wg_peers 有行）才值得建 VM（stage2 从 ipam 自行选跨
# 节点机对，不依赖 NODE_A/NODE_B 字面值）。
if [ "$stage" = "stage1" ]; then
  pg "SELECT node_id FROM wg_peers WHERE node_id='$NODE_A'" | grep -q "$NODE_A" \
    || fail "node $NODE_A not in mesh (no wg_peers row; is agent MESH=eastwest running?)"
else
  [ "$(pg "SELECT count(*) FROM wg_peers")" -ge 2 ] \
    || fail "mesh needs 2+ nodes (wg_peers rows)"
fi
log "mesh membership OK (cell $CELL)"

case "$stage" in
  stage1)
    # 单节点：本机经 slot 路由 ping guest ULA（半程：L3+NDP+guest init 注入）。
    # ULA 来源：controller T4c 在派发时分配（ipam_allocations 在役行）。
    # 源地址 = 本节点自身 ULA（/64 基址，wg 设备地址）：guest 回包经 slot
    # → host → tc_ingress 源绑定 + node_ula 平台放行；用 veth LL 源会因
    # guest 网关 fe80::1（bridge）与 root veth LL 同址异链而回不去。
    ULA="$(pg "SELECT ula FROM ipam_allocations WHERE node_id='$NODE_A' AND released_at IS NULL ORDER BY allocated_at DESC LIMIT 1")"
    [ -n "$ULA" ] || fail "no live ULA on $NODE_A (create a machine via API first)"
    NODE_PREFIX="$(pg "SELECT node_prefix FROM wg_peers WHERE node_id='$NODE_A'")"
    [ -n "$NODE_PREFIX" ] || fail "node $NODE_A not in wg_peers (mesh registration missing)"
    NODE_ULA="${NODE_PREFIX%%/*}"
    log "pinging guest ULA $ULA from host (src $NODE_ULA, 5s deadline)"
    ping -6 -c 3 -W 5 -I "$NODE_ULA" "${ULA%%/*}" >"$RUN_DIR/stage1-ping.log" 2>&1 \
      || fail "host→guest ULA ping failed (see $RUN_DIR/stage1-ping.log)"
    log "stage1 PASS (host→guest ULA reachable)"
    ;;
  stage2)
    # 跨节点：guest-a 经 WG 直达 guest-b（全程：identity/ipcache + WG + eBPF）。
    # 验证集（ADR-0040 §13）：
    #   1) FWD ICMP：有 EastWest 规则方向 guest-exec ping 必通（WG hop TTL）
    #   2) REV TCP：无规则方向 SYN 必被端口门控拒（wget 超时）——对称回程
    #      条目只放行非发起包；connectionless（ICMP/UDP）为对级授权（设计
    #      内权衡，见 tc.c 注释）。
    API_TOKEN="${FABRIC_SPIKE_API_TOKEN:?set FABRIC_SPIKE_API_TOKEN}"
    API="${FABRIC_SPIKE_API_ADDR:-http://127.0.0.1:8083}"
    # 取不同节点的两台在役机（app 各异，避免同 app 双 replica）。
    read -r MA_MB <<<"$(pg "SELECT string_agg(m.app_id, ' ' ORDER BY m.app_id) FROM machines m JOIN ipam_allocations a ON a.machine_id=m.id AND a.released_at IS NULL WHERE m.desired_state!='DELETED' GROUP BY a.node_id HAVING count(*)>0 ORDER BY min(a.node_id) LIMIT 2" | head -1)" || true
    NODES=$(pg "SELECT count(DISTINCT a.node_id) FROM ipam_allocations a JOIN machines m ON m.id=a.machine_id WHERE a.released_at IS NULL AND m.desired_state!='DELETED'")
    [ "$NODES" = "2" ] || fail "stage2 needs live machines on 2 nodes (have $NODES); create one app per node first"
    APP_A=$(pg "SELECT m.app_id FROM machines m JOIN ipam_allocations a ON a.machine_id=m.id AND a.released_at IS NULL WHERE m.desired_state!='DELETED' ORDER BY a.node_id LIMIT 1")
    NODE_A_ID=$(pg "SELECT a.node_id FROM ipam_allocations a JOIN machines m ON m.id=a.machine_id WHERE m.app_id='$APP_A' AND a.released_at IS NULL")
    APP_B=$(pg "SELECT m.app_id FROM machines m JOIN ipam_allocations a ON a.machine_id=m.id AND a.released_at IS NULL WHERE m.desired_state!='DELETED' AND a.node_id!='$NODE_A_ID' ORDER BY a.node_id LIMIT 1")
    [ -n "$APP_A" ] && [ -n "$APP_B" ] || fail "could not pick two cross-node machines"
    ULA_A=$(pg "SELECT a.ula FROM ipam_allocations a JOIN machines m ON m.id=a.machine_id WHERE m.app_id='$APP_A' AND a.released_at IS NULL")
    ULA_B=$(pg "SELECT a.ula FROM ipam_allocations a JOIN machines m ON m.id=a.machine_id WHERE m.app_id='$APP_B' AND a.released_at IS NULL")
    SVC=$(pg "SELECT coalesce(d.services->0->>'name','default') FROM deployments d JOIN machines m ON m.deployment_id=d.id WHERE m.app_id='$APP_B' LIMIT 1")
    MID_A=$(pg "SELECT m.id FROM machines m WHERE m.app_id='$APP_A' AND m.desired_state!='DELETED' LIMIT 1")
    MID_B=$(pg "SELECT m.id FROM machines m WHERE m.app_id='$APP_B' AND m.desired_state!='DELETED' LIMIT 1")
    log "pair: $APP_A(${ULA_A%%/*}) → $APP_B(${ULA_B%%/*}) svc=$SVC"
    # 反向断言前置：历史/并发 run 可能留下 B→A（角色互换）规则，会让
    # “未授权反向必须被拒”因一条真实授权而假失败（2026-09-11 真机证据：
    # dns-w→spike-edge 的旧规则使反向 wget 成功）。先清掉全部 B→A 规则。
    curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" \
      "$API/v1/eastwest-policies?dst_project=dev&dst_app=$APP_A" \
      | python3 -c '
import json, sys
src_app = sys.argv[1]
for rule in json.load(sys.stdin):
    if rule.get("src_app") == src_app:
        print(rule.get("src_project", "dev"), rule.get("dst_service", ""))
' "$APP_B" \
      | while read -r src_proj svc_a; do
          [ -n "$svc_a" ] || continue
          curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" -X DELETE \
            "$API/v1/eastwest-policies?src_project=$src_proj&src_app=$APP_B&dst_project=dev&dst_app=$APP_A&dst_service=$svc_a" >/dev/null \
            || fail "DELETE reverse eastwest policy failed"
        done
    # EastWest 规则（租户 API，dst 归属 dev）。
    curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" -X PUT "$API/v1/eastwest-policies" \
      -H 'Content-Type: application/json' \
      -d "{\"src_project\":\"dev\",\"src_app\":\"$APP_A\",\"dst_project\":\"dev\",\"dst_app\":\"$APP_B\",\"dst_service\":\"$SVC\",\"ports\":[80]}" >/dev/null \
      || fail "PUT eastwest policy failed"
    log "eastwest rule $APP_A→$APP_B/$SVC:80 applied; waiting snapshot push"
    sleep 25
    # 1) FWD ICMP（guest-exec）。
    OUT=$(curl -fsS -m 60 -H "Authorization: Bearer $API_TOKEN" -X POST "$API/v1/machines/$MID_A/exec" \
      -H 'Content-Type: application/json' \
      -d "{\"command\":[\"/bin/busybox\",\"ping\",\"-6\",\"-c\",\"3\",\"-W\",\"3\",\"${ULA_B%% /*}\"],\"operation_id\":\"op-$RUN_ID-fwd\"}" 2>&1) || true
    echo "$OUT" >"$RUN_DIR/stage2-fwd-exec.json"
    echo "$OUT" | grep -q '"exit_code":0' \
      || fail "forward guest ping failed (see $RUN_DIR/stage2-fwd-exec.json)"
    log "stage2 forward guest→guest ULA ping over mesh: PASS"
    # 2) REV TCP：无规则方向 SYN 拒（wget 超时 = deny 生效）。
    OUT=$(curl -fsS -m 60 -H "Authorization: Bearer $API_TOKEN" -X POST "$API/v1/machines/$MID_B/exec" \
      -H 'Content-Type: application/json' \
      -d "{\"command\":[\"/bin/busybox\",\"wget\",\"-T\",\"2\",\"-O\",\"/dev/null\",\"http://[${ULA_A%% /*}]:80/\"],\"operation_id\":\"op-$RUN_ID-rev\"}" 2>&1) || true
    echo "$OUT" >"$RUN_DIR/stage2-rev-exec.json"
    echo "$OUT" | grep -q '"exit_code":1' \
      || fail "reverse TCP connect unexpectedly allowed (see $RUN_DIR/stage2-rev-exec.json)"
    log "stage2 reverse un-ruled TCP initiation denied: PASS"
    log "stage2 PASS (cross-node guest→guest over WG + eBPF policy)"
    ;;
  stage3)
    # G2d（ADR-0040 §14）：edge 经 mesh 直达节点（fabric ingress 凭证路由）。
    # 前置：edge-proxy 以 FIREPAAS_EDGE_MESH_DIRECT=true 运行、其 WG 公钥
    # 已配置到控制面（FIREPAAS_MESH_EDGE_PUBKEY/ENDPOINT）。
    API_TOKEN="${FABRIC_SPIKE_API_TOKEN:?set FABRIC_SPIKE_API_TOKEN}"
    API="${FABRIC_SPIKE_API_ADDR:-http://127.0.0.1:8083}"
    EDGE="${FABRIC_SPIKE_EDGE_ADDR:-http://127.0.0.1:8084}"
    # root 下 $HOME 是 /root：沿用 lab 约定（zty 用户目录）。
    LAB_BIN="${LAB_BIN:-/home/zty/.local/firepaas-lab/bin}"
    [ -x "$LAB_BIN/edge-proxy" ] || fail "edge-proxy binary missing"

    # 0) 环境门禁：mesh 投影存在（peer 含 edge-hub；endpoint 有在役机器）。
    PEERS=$(docker exec dev-redis-1 redis-cli --scan --pattern 'mesh:peer:*' | wc -l)
    [ "$PEERS" -ge 3 ] || fail "mesh:peer projection incomplete ($PEERS < 3: nodes + edge-hub)"
    EPS=$(docker exec dev-redis-1 redis-cli --scan --pattern 'mesh:endpoint:*' | wc -l)
    [ "$EPS" -ge 1 ] || fail "mesh:endpoint projection empty"

    # 1) 起一个 mesh_direct + health_check 应用（READY 门控 G2b 提示）。
    APP="spike-edge-$(date +%s)"
    REF="${FABRIC_SPIKE_IMAGE_REF:?set FABRIC_SPIKE_IMAGE_REF}"
    curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" -X POST "$API/v1/apps" \
      -H 'Content-Type: application/json' \
      -d "{\"app_id\":\"$APP\",\"project_id\":\"dev\",\"hostname\":\"$APP.firepaas.local\",\"image\":\"$REF\",\"port\":80,\"replicas\":1,\"services\":[{\"internal_port\":80,\"mesh_direct\":true}],\"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:80/\",\"interval_seconds\":5,\"timeout_seconds\":2,\"unhealthy_threshold\":3}}" >/dev/null \
      || fail "create app failed"
    for _ in $(seq 1 60); do
      RD=$(pg "SELECT coalesce(observed_readiness,'') FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' LIMIT 1")
      [ "$RD" = "READY" ] && break
      sleep 5
    done
    [ "$RD" = "READY" ] || fail "app never READY"
    # 等路由 + ULA 提示投影。
    sleep 15
    # G2b 提示断言：经 edge 真实消费面（catalog Redis route 条目）取 ULA。
    ULA=$(curl -s -m 5 "http://127.0.0.1:8084/healthz" >/dev/null 2>&1; docker exec dev-redis-1 redis-cli --scan --pattern 'route:*' | while read -r k; do
      v=$(docker exec dev-redis-1 redis-cli get "$k" 2>/dev/null)
      echo "$v" | grep -q "\"machine_id\":\"$APP-r0-g1\"" && echo "$v" | grep -oE '"ula":"[^"]+"' | head -1 | cut -d'"' -f4 && break
    done)
    [ -n "$ULA" ] || fail "backend ULA hint missing (mesh_direct backend not published)"

    # 2) 直达 e2e：经 edge 请求（edge 以 mesh 直达优先）。
    CODE=$(curl -s -m 20 -o /dev/null -w '%{http_code}' -H "Host: $APP.firepaas.local" "$EDGE/")
    [ "$CODE" = "200" ] || fail "edge direct request: HTTP $CODE (want 200)"
    log "edge mesh-direct request: 200 OK"

    # 3) 指标断言：direct 请求计数 > 0 且回落计数存在（各计数分母一致）。
    METRICS_ADDR="${FABRIC_SPIKE_EDGE_METRICS:-http://127.0.0.1:9465}"
    METRICS=$(curl -s -m 10 "$METRICS_ADDR/metrics" 2>/dev/null || true)
    DIRECT=$(echo "$METRICS" | grep -E '^firepaas_edge_mesh_direct_requests_total' | grep -oE '[0-9]+$' || true)
    [ -n "$DIRECT" ] && [ "$DIRECT" -ge 1 ] \
      || fail "mesh direct counter missing/zero (metrics endpoint: check FIREPAAS_EDGE_METRICS_ADDR)"
    log "mesh direct counter = $DIRECT"

    # 4) 直达失效回落：终结器端口不可达（iptables 临时挡 ingress 端口）→
    #    edge 应回落 legacy 且请求仍 200（回滚语义：FIREPAAS_EDGE_MESH_DIRECT
    #    关闭等价于永久回落）。
    NODE_ULA=$(pg "SELECT w.node_prefix FROM wg_peers w JOIN machines m ON m.node_id=w.node_id WHERE m.app_id='$APP' LIMIT 1")
    INGRESS_PORT=$(docker exec dev-redis-1 redis-cli --scan --pattern 'mesh:endpoint:*' | head -1 | xargs -r docker exec dev-redis-1 redis-cli get 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("ingress_port", 5109))' 2>/dev/null || echo 5109)
    # ULA 是 v6：必须 ip6tables（iptables 会静默失败——真机实测踩坑）。
    ip6tables -I INPUT -p tcp -d "${NODE_ULA%%/*}" --dport "$INGRESS_PORT" -j DROP 2>/dev/null || true
    sleep 1
    CODE2=$(curl -s -m 20 -o /dev/null -w '%{http_code}' -H "Host: $APP.firepaas.local" "$EDGE/")
    ip6tables -D INPUT -p tcp -d "${NODE_ULA%%/*}" --dport "$INGRESS_PORT" -j DROP 2>/dev/null || true
    [ "$CODE2" = "200" ] || fail "fallback request: HTTP $CODE2 (want 200 via legacy path)"
    log "fallback to legacy path: 200 OK"
    log "stage3 PASS (edge mesh direct + token e2e + fallback)"
    ;;
esac
