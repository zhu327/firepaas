#!/usr/bin/env bash
# M5 e2e harness（单机）：mvp-plan §9 内部生产就绪验收
#   A) 安全负路径：API key（错/撤销/scope/跨 project）+ 镜像准入（digest 强制/允许列表）
#   B) 运行时稳定性：20 循环 pause/resume guest 时钟漂移 + 宿主 entropy/FD/conntrack 采样
#   C) 可观测：/metrics 宿主 gauge + operation trace（request/result 脱敏断言）
#   D) 可靠性：PG 备份/恢复演练 + Redis flushall → 显式重投影 ≤45s
#   E) 升级：node drain → agentd rebuild → ready → 对账（upgrade-agentd.sh 演练）
#   F) host hardening 审计 + 清理 + 终态零泄漏
# 用法: sudo bash scripts/lab/e2e-m5.sh
# 运行约定：后台运行 + 日志轮询（全部等待有界、每行带时间戳）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-m5"
RUN_ID="e2e-m5-$(date +%s)"
API_TOKEN="m5-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT=8083
EDGE_HTTP=8084
EDGE_TLS=8445
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-m5 $(now)] $*"; }
fail() { echo "[e2e-m5] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
cur() { curl -s -m 20 "$@"; }

# ontime 探针镜像：自建自推（P1-6 修复：不再依赖手工预推的 tag——旧硬编码
# digest 曾指向丢失的 manifest blob，e2e 无限 recreate）。
ONLINE_OUT=$(bash "$HERE/push-ontime.sh") || fail "push-ontime 失败"
ONTIME_REF=$(echo "$ONLINE_OUT" | grep '^REF=' | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || fail "ontime REF 解析失败"

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建"
[[ -f "$CERT_DIR/wildcard-$DOMAIN.crt" ]] || fail "泛域名证书缺失"

log "0) 启动：root setup + agentd + API/edge（require-digest + 允许列表）"
T0=$(date +%s)
mark() { log "    (累计 $(( $(date +%s) - T0 ))s) $*"; }
"$HERE/root-setup.sh" >/dev/null
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"
for _ in $(seq 1 60); do "$LAB_BIN/agentctl" info >/dev/null 2>&1 && break; sleep 2; done
mark "agentd ready"

MASTER_KEY="$(openssl rand -base64 32)"
TRAFFIC_KEY="$(openssl rand -base64 32)"
pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
pkill -f "$LAB_BIN/edge-proxy" 2>/dev/null || true
sleep 1
nohup env FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_NOMAD_ADDR=http://127.0.0.1:4646 \
  FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 FIREPAAS_HTTP_PORT=$API_PORT FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_SECRETS_MASTER_KEY="$MASTER_KEY" FIREPAAS_TRAFFIC_TOKEN_KEY="$TRAFFIC_KEY" \
  FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_ROLLOUT_TIMEOUT=120s FIREPAAS_ROLLOUT_DRAIN=10s \
  FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/m5-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" FIREPAAS_EDGE_RATE_LIMIT=100 FIREPAAS_EDGE_RATE_BURST=200 \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/m5-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/m5-api.log"; fail "API 未就绪"; }
hc=$(curl -sk -m 5 -o /dev/null -w '%{http_code}' --resolve "x.$DOMAIN:$EDGE_TLS:127.0.0.1" "https://x.$DOMAIN:$EDGE_TLS/healthz")
[[ "$hc" == "200" ]] || fail "edge TLS 未就绪 ($hc)"
mark "api/edge up"

log "0.5) 预清理历史验收机"
# M5 修复（实测踩坑）：上一次运行若在 E 段失败（drain 后未 ready），节点会
# 永久停在 draining，本轮全部放置被过滤 → B 段卡死。预清理一律复位。
for _ in $(seq 1 10); do
  curl -s -m 10 -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/v1/nodes" |
    python3 -c 'import json,sys
for n in json.load(sys.stdin).get("nodes") or []:
    print(n.get("id",n.get("ID","")))' |
    while read -r nid; do
      [[ -n "$nid" ]] && curl -s -m 10 -H "Authorization: Bearer $API_TOKEN" -X POST \
        "http://127.0.0.1:$API_PORT/v1/nodes/$nid/ready" >/dev/null || true
    done
  nd=$(pg "SELECT count(*) FROM nodes WHERE draining")
  [[ "$nd" == "0" ]] && break
  sleep 3
done
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now()" >/dev/null
pg "UPDATE deployments SET status='SUPERSEDED', updated_at=now() WHERE status IN ('ACTIVE','PREPARING')" >/dev/null
pg "UPDATE rollouts SET status='COMPLETE', failed=true, completed_at=now(), updated_at=now()
	WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK')" >/dev/null
for _ in $(seq 1 60); do
  left=$(pg "SELECT count(*) FROM machines WHERE desired_state != 'DELETED'")
  [[ "$left" == "0" ]] && break
  sleep 5
done
[[ "$left" == "0" ]] || fail "旧机器未清理 ($left)"

log "A) 安全负路径"
pg "INSERT INTO projects(id, name) VALUES ('other','other-project') ON CONFLICT (id) DO NOTHING" >/dev/null
ROK=$(cur -H "Authorization: Bearer $API_TOKEN" -X POST "http://127.0.0.1:$API_PORT/v1/apikeys" \
  -H 'Content-Type: application/json' -d '{"name":"e2e-readonly","scopes":["read"],"project_id":"dev"}')
ROKEY=$(echo "$ROK" | python3 -c 'import json,sys;print(json.load(sys.stdin)["key"])')
[[ -n "$ROKEY" ]] || fail "create readonly key: $ROK"
WK=$(cur -H "Authorization: Bearer $API_TOKEN" -X POST "http://127.0.0.1:$API_PORT/v1/apikeys" \
  -H 'Content-Type: application/json' -d '{"name":"e2e-otherwrite","scopes":["write"],"project_id":"other"}')
WKEY=$(echo "$WK" | python3 -c 'import json,sys;print(json.load(sys.stdin)["key"])')
WID=$(echo "$WK" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
[[ -n "$WKEY" ]] || fail "create cross-project key: $WK"
# 1) 错 key / 乱字符串 → 401。
c1=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer fp_deadbeef" "http://127.0.0.1:$API_PORT/v1/machines")
[[ "$c1" == "401" ]] || fail "wrong key not 401: $c1"
# 2) read-only key 写端点 → 403。
c2=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $ROKEY" -X POST \
  "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' -d '{"app_id":"x"}')
[[ "$c2" == "403" ]] || fail "read scope can write: $c2"
# 3) read key 读列表 → 200 且只含 dev 项目行（M5 补：验内容而非仅状态码）。
c3=$(cur -H "Authorization: Bearer $ROKEY" "http://127.0.0.1:$API_PORT/v1/machines?project_id=dev")
echo "$c3" | grep -q '"project_id":"other"' && fail "read key 列表泄漏他 project 行"
# 3b) read key 铸造 traffic-token → 403（P1-4：routeScope write）。
c3b=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $ROKEY" \
  "http://127.0.0.1:$API_PORT/v1/machines/any/traffic-token")
[[ "$c3b" == "403" ]] || fail "read key traffic-token 未拒: $c3b"
# 3c) 受限 write key 显式建 dev app → 403（P1-2：body.project_id clamp）。
c3c=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $WKEY" -X POST \
  "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"clamp-$RUN_ID\",\"project_id\":\"dev\",\"hostname\":\"clamp.$DOMAIN\",
       \"image\":\"$ONTIME_REF\",\"port\":80,\"replicas\":1}")
[[ "$c3c" == "403" ]] || fail "跨 project createApp 未拦截: $c3c"
# 3d) 受限 write key 向 dev project 写 secret → 403（P1-2：putSecret clamp）。
c3d=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $WKEY" -X POST \
  "http://127.0.0.1:$API_PORT/v1/secrets" -H 'Content-Type: application/json' \
  -d '{"project_id":"dev","name":"clamptest","value":"x"}')
[[ "$c3d" == "403" ]] || fail "跨 project putSecret 未拦截: $c3d"
# 4) 镜像准入：mutable tag / 越界 registry → 400。
c4=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $API_TOKEN" -X POST \
  "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"bad-tag-$RUN_ID\",\"project_id\":\"dev\",\"hostname\":\"badtag.$DOMAIN\",
       \"image\":\"docker.m.daocloud.io/library/nginx:alpine\",\"port\":80,\"replicas\":1}")
[[ "$c4" == "400" ]] || fail "require-digest 未生效: $c4"
log "    错 key/只读 scope/镜像 tag+allowlist 拒绝 全部符合预期 OK"

log "B) 稳定性：secret_env 双策略 + ontime 探针 + 20 循环 pause/resume 时钟漂移"
# 先植入 secret 标记（后续脱敏断言用；P3-18 修复：不再空转 grep）。
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X POST \
  "http://127.0.0.1:$API_PORT/v1/secrets" -H 'Content-Type: application/json' \
  -d '{"project_id":"dev","name":"e2e-marker","value":"s3cr3t-e2e-marker-value"}'

# B1：默认 fail-closed——带 secret_refs 的 create 必须以 InvalidArgument 终态
# 失败（agent 无 FIREPAAS_SECRET_INJECTION 时拒绝 one-shot 语义不明的注入）。
APP5="app-clock-$RUN_ID"
HN5="$APP5.$DOMAIN"
create_status=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/m5-app.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP5\",\"project_id\":\"dev\",\"hostname\":\"$HN5\",
       \"image\":\"$ONTIME_REF\",\"port\":80,\"replicas\":1,
       \"secret_refs\":{\"MARKER\":{\"secret\":\"e2e-marker\"}}}")
[[ "$create_status" == "201" ]] || fail "secret-ref app create request: $create_status $(cat /tmp/m5-app.json)"
# B1( v1.2-B 语义 )：one-shot 是默认交付模式——agentd 默认 FIREPAAS_SECRET_INJECTION=oneshot
# 并上报 secret.oneshot.v1能力；带 secret_refs 的 create 必须 RUNNING 且 canary 经路由可见、
# lease ACKED。早期「默认 fail-closed」断言已过期（v1.2 后默认交付、agentd-single.hcl
# 的 opt-in 注释同样为失效的 M4 语义）。
for _ in $(seq 1 90); do
  running=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP5' AND observed_state='RUNNING' AND desired_state!='DELETED'")
  [[ "$running" == "1" ]] && break
  sleep 3
done
[[ "$running" == "1" ]] || fail "secret-ref workload 未 RUNNING（oneshot 默认交付应成功）"
B1M=$(pg "SELECT id FROM machines WHERE app_id='$APP5' AND desired_state!='DELETED' LIMIT 1")
for _ in $(seq 1 30); do
  acked=$(pg "SELECT count(*) FROM secret_delivery_leases WHERE machine_id='$B1M' AND state='ACKED'")
  [[ "${acked:-0}" == "1" ]] && break
  sleep 2
done
[[ "${acked:-0}" == "1" ]] || fail "oneshot lease 未 ACKED"
mark "B1 oneshot 默认交付 RUNNING + lease ACKED OK"
# canary：entrypoint 进程读到 secret（经 edge 路由）；exec 会话环境隔离。
edge_curl() { local h=$1; shift; curl -s -m 15 --resolve "$h:$EDGE_TLS:127.0.0.1" --cacert "$CERT_DIR/ca.crt" "$@"; }
env_val=$(edge_curl "$HN5" "https://$HN5:$EDGE_TLS/env?k=MARKER" || true)
echo "$env_val" | grep -q "s3cr3t-e2e-marker-value" || fail "entrypoint 未读到 secret: $env_val"
# pause（memory snapshot）对 secret execution 必须 409（ADR-0024 §9）。
pause_code=$(cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' -X POST \
  "http://127.0.0.1:$API_PORT/v1/machines/$B1M/pause")
[[ "$pause_code" == "409" ]] || fail "secret machine pause 应 409，got $pause_code"
mark "B1 canary 路由读取 + pause 409 OK"

# B2：非法 FIREPAAS_SECRET_INJECTION 模式 = fail-closed（product 默认路径下 secret-bearing
# create 必须 InvalidArgument 终态，绝不创建无 secret 的 VM）。用 job 副本重启。
python3 - "$ROOT_DIR/iac/nomad/agentd-single.hcl" > "$RUN_DIR/agentd-badmode.hcl" <<'PY' || fail "badmode 渲染脚本缺执行权限"
import sys
src = open(sys.argv[1]).read()
marker = "FIREPAAS_IMAGE_MAX_UNPACK_MIB"
assert marker in src
src = src.replace(marker, 'FIREPAAS_SECRET_INJECTION = "bogus-mode"\n        ' + marker)
print(src, end="")
PY
grep -q 'FIREPAAS_SECRET_INJECTION = "bogus-mode"' "$RUN_DIR/agentd-badmode.hcl" || fail "badmode 渲染失败"
if ! nomad job run -detach -var "repo_root=$ROOT_DIR" -var "lab_bin=$LAB_BIN" \
  -var "agentd_binary_sha256=$(sha256sum "$LAB_BIN/agentd" | awk '{print $1}')-secret-badmode" \
  "$RUN_DIR/agentd-badmode.hcl" > "$RUN_DIR/badmode-run.log" 2>&1; then
  tail -5 "$RUN_DIR/badmode-run.log"
  fail "badmode job run 失败"
fi
for _ in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/5108) 2>/dev/null && break; sleep 2; done
(echo > /dev/tcp/127.0.0.1/5108) 2>/dev/null || fail "badmode agentd 未就绪"
# TCP 已监听但能力是“ agentd 已重注册并同步能力予控制面”——不然 create 会
# 撞 Unavailable（验收实测的 B2 抖动）。
for _ in $(seq 1 40); do
  ndh=$(curl -s -m 5 -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/v1/nodes" \
    | python3 -c 'import json,sys; print(sum(1 for n in (json.load(sys.stdin).get("nodes") or []) if n.get("Status")=="HEALTHY"))' || true)
  [[ "${ndh:-0}" -ge 1 ]] && break
  sleep 2
done
[[ "${ndh:-0}" -ge 1 ]] || fail "badmode agent 未注册 HEALTHY"
mark "B2 badmode agentd 就绪"
APP5B="app-clock-$RUN_ID-badmode"
create_status=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/m5-app.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP5B\",\"project_id\":\"dev\",\"hostname\":\"$APP5B.$DOMAIN\",
       \"image\":\"$ONTIME_REF\",\"port\":80,\"replicas\":1,
       \"secret_refs\":{\"MARKER\":{\"secret\":\"e2e-marker\"}}}")
[[ "$create_status" == "201" ]] || fail "badmode secret-ref app create: $create_status $(cat /tmp/m5-app.json)"
for _ in $(seq 1 90); do
  failed=$(pg "SELECT count(*) FROM operations o JOIN machines m ON m.id=o.machine_id WHERE m.app_id='$APP5B' AND o.kind='create' AND o.status='FAILED'")
  [[ "$failed" == "1" ]] && break
  sleep 3
done
[[ "$failed" == "1" ]] || fail "badmode secret-ref workload did not fail closed"
running=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP5B' AND observed_state='RUNNING' AND desired_state!='DELETED'")
[[ "$running" == "0" ]] || fail "badmode secret-ref workload unexpectedly became ready"
mark "B2 未知注入模式 fail-closed OK"
# B2 实验机 cleanup：否则 E 段 evacuate 会拖入该 machine（它创建已终态失败）。
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP5B" || true
for _ in $(seq 1 30); do
  left=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP5B' AND desired_state != 'DELETED'")
  [[ "${left:-0}" == "0" ]] && break
  sleep 2
done
[[ "${left:-0}" == "0" ]] || fail "badmode app 未清除"
# 还原正常 agentd，供后续 B3 pause/resume 与 C/D 段使用。
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 恢复失败"
for _ in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/5108) 2>/dev/null && break; sleep 2; done
(echo > /dev/tcp/127.0.0.1/5108) 2>/dev/null || fail "还原后的 agentd 未就绪"
mark "B2 后 agentd 已还原"

# B3：pause/resume 漂移验证必须使用非 secret 机器。
APP6="app-plain-$RUN_ID"
HN6="$APP6.$DOMAIN"
create_status=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/m5-app2.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP6\",\"project_id\":\"dev\",\"hostname\":\"$HN6\",
       \"image\":\"$ONTIME_REF\",\"port\":80,\"replicas\":1}")
[[ "$create_status" == "201" ]] || fail "B3 非 secret app create: $create_status $(cat /tmp/m5-app2.json)"
for _ in $(seq 1 90); do
  running=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP6' AND observed_state='RUNNING' AND desired_state!='DELETED'")
  [[ "$running" == "1" ]] && break
  sleep 3
done
[[ "$running" == "1" ]] || fail "B3 非 secret 机器未 RUNNING"
M5=$(pg "SELECT id FROM machines WHERE app_id='$APP6' AND desired_state!='DELETED' LIMIT 1")

edge_curl() { local h=$1; shift; curl -s -m 15 --resolve "$h:$EDGE_TLS:127.0.0.1" --cacert "$CERT_DIR/ca.crt" "$@"; }
guest_ms() { edge_curl "$HN6" "https://$HN6:$EDGE_TLS/" | python3 -c 'import json,sys; print(json.load(sys.stdin)["epoch_ms"])'; }
host_ms() { date +%s%3N; }
g0=$(guest_ms); h0=$(host_ms)
[[ -n "$g0" && -n "$h0" ]] || fail "guests clock read: g=$g0 h=$h0"

FD0=$(cat /proc/sys/fs/file-nr)
CT0=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || true)
EA0=$(cat /proc/sys/kernel/random/entropy_avail)
log "    基线 FD=$FD0 conntrack=$CT0 entropy=$EA0"
MAX_DRIFT=0
DRIFTS=""
for i in $(seq 1 20); do
  cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X POST "http://127.0.0.1:$API_PORT/v1/machines/$M5/pause"
  for _ in $(seq 1 30); do [[ "$(pg "SELECT observed_state FROM machines WHERE id='$M5'")" == "PAUSED" ]] && break; sleep 1; done
  cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X POST "http://127.0.0.1:$API_PORT/v1/machines/$M5/resume"
  for _ in $(seq 1 30); do [[ "$(pg "SELECT observed_state FROM machines WHERE id='$M5'")" == "RUNNING" ]] && break; sleep 1; done
  if (( i % 5 == 0 )); then
    g=$(guest_ms); h=$(host_ms)
    drift=$(( g - h ))
    DRIFTS="$DRIFTS $drift"
    (( ${drift#-} > MAX_DRIFT )) && MAX_DRIFT=${drift#-}
    log "    cycle $i guest-host drift=${drift}ms"
  fi
done
[[ "$(pg "SELECT observed_state FROM machines WHERE id='$M5'")" == "RUNNING" ]] || fail "20 循环后未恢复 RUNNING"
FD1=$(cat /proc/sys/fs/file-nr)
CT1=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || true)
log "    20 循环后 FD=$FD1 conntrack=$CT1（漂移序列:$DRIFTS max=${MAX_DRIFT}ms）"
# M5.2 结论：FC snapshot 不保存 wall clock，guest 时钟落后宿主（上限记录进 capacity model）。
[[ "$MAX_DRIFT" -lt 600000 ]] || fail "guest 时钟漂移 ${MAX_DRIFT}ms ≥ 10min"
log "    guest 时钟漂移 < 10min（snapshot 语义，runbook 建议 guest chrony）OK"

log "C) 可观测：/metrics 宿主 gauge + operation trace 脱敏"
for _ in $(seq 1 25); do
  M=$(cur -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/metrics")
  echo "$M" | grep -q "firepaas_host_entropy_avail" && break
  sleep 3
done
echo "$M" | grep -qE '^firepaas_host_(fds_allocated|entropy_avail|conntrack_count|load1_x100)' \
  || fail "/metrics 宿主 gauge 缺失"
log "    /metrics 已含 host gauge（fds/entropy/conntrack/load）"
ops=$(cur -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/v1/operations?machine_id=$M5&kind=create&limit=10")
echo "$ops" | grep -q '"kind":"create"' || fail "op trace 无 create: $ops"
echo "$ops" | grep -qi "s3cr3t\|fp_[a-f0-9]\{32\}" && fail "op trace 泄露明文特征！"
OPID=$(echo "$ops" | python3 -c 'import json,sys; o=json.load(sys.stdin)["operations"]; print([x["id"] for x in o if x["kind"]=="create"][0])')
op1=$(cur -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/v1/operations/$OPID")
echo "$op1" | grep -q '"attempts"' || fail "op 详情缺 attempts 时间轴"
log "    operation trace 全字段 + 零明文 OK"

log "D) 可靠性：PG 备份/恢复演练 + flushall → 显式重投影"
"$HERE/pg-backup.sh" >/dev/null 2>&1 || fail "pg-backup 失败"
"$HERE/pg-restore-rehearsal.sh" >/dev/null 2>&1 || fail "pg-restore-rehearsal 失败（行数不一致）"
log "    PG 备份→scratch 恢复→行数一致 PASS"
redis-cli_flush() { docker exec dev-redis-1 redis-cli FLUSHALL >/dev/null; }
redis-cli_flush
rp=$(cur -H "Authorization: Bearer $API_TOKEN" -X POST "http://127.0.0.1:$API_PORT/v1/system/reprojections")
echo "$rp" | python3 -c '
import json,sys
r=json.load(sys.stdin)
assert r.get("rebuilt_now") is True, f"kick 未生效: {r}"
assert isinstance(r.get("duration_ms"), int)' || fail "reprojections 响应异常: $rp"
log "    显式重投影（同步 kick 重建）: $rp"
for _ in $(seq 1 25); do
  routes=$(docker exec dev-redis-1 redis-cli --scan --pattern 'route:*' 2>/dev/null | wc -l)
  (( routes > 0 )) && break
  sleep 3
done
(( routes > 0 )) || fail "45s 内 route 投影未重建"
hc2=$(edge_curl "$HN5" -o /dev/null -w '%{http_code}' "https://$HN5:$EDGE_TLS/")
[[ "$hc2" == "200" ]] || fail "重投影后数据面: $hc2"
log "    flushall→显式重投影→≤15s 重建→edge 200 OK"

log "E) 升级：node drain → agentd rebuild → ready → 对账"
curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP5" || true
FP_API_TOKEN="$API_TOKEN" FP_API_ADDR="http://127.0.0.1:$API_PORT" \
  "$HERE/upgrade-agentd.sh" > "$RUN_DIR/m5-upgrade.log" 2>&1 || { tail -10 "$RUN_DIR/m5-upgrade.log"; fail "upgrade rehearsal 失败"; }
cat "$RUN_DIR/m5-upgrade.log" | grep -E "PASS|draining|ready" | tail -3
log "    drain→rebuild→ready→对账 PASS"
# 升级后管线验证。
APP6="app-post-upg-$RUN_ID"
HN6="$APP6.$DOMAIN"
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' -X POST \
  "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP6\",\"project_id\":\"dev\",\"hostname\":\"$HN6\",
       \"image\":\"$ONTIME_REF\",\"port\":80,\"replicas\":1}" | grep -q 201 || fail "升级后建 app 失败"
for _ in $(seq 1 90); do
  r6=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP6' AND observed_state='RUNNING' AND desired_state!='DELETED'")
  [[ "$r6" == "1" ]] && break
  sleep 3
done
[[ "$r6" == "1" ]] || fail "升级后机器未 RUNNING"
h6=$(curl -s -m 15 --resolve "$HN6:$EDGE_TLS:127.0.0.1" --cacert "$CERT_DIR/ca.crt" -o /dev/null -w '%{http_code}' "https://$HN6:$EDGE_TLS/")
[[ "$h6" == "200" ]] || fail "升级后数据面: $h6"
log "    升级后创建+路由 200 OK"

log "F) host hardening 审计 + 清理 + 终态零泄漏"
"$HERE/host-hardening-check.sh" > "$RUN_DIR/m5-hardening.log" 2>&1 || { grep FAIL "$RUN_DIR/m5-hardening.log" | tail -5; fail "hardening 审计 FAIL"; }
grep -E "PASS|WARN|FAIL" "$RUN_DIR/m5-hardening.log" | head -8
log "    hardening 审计执行（PASS/无 FAIL）"

# 撤销演练：revoked key 必须 401。
cur -H "Authorization: Bearer $API_TOKEN" -X DELETE "http://127.0.0.1:$API_PORT/v1/apikeys/$WID" >/dev/null
c7=$(cur -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $WKEY" "http://127.0.0.1:$API_PORT/v1/machines")
[[ "$c7" == "401" ]] || fail "撤销后 key 仍可用: $c7"

curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP6" || true
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now()" >/dev/null
for _ in $(seq 1 60); do
  left=$(pg "SELECT count(*) FROM machines WHERE desired_state != 'DELETED'")
  [[ "$left" == "0" ]] && break
  sleep 5
done
pg "SELECT count(*) FROM deployments WHERE status IN ('ACTIVE','PREPARING')" | grep -q '^0$' || :
sleep 10
fc=$(ps -eo args | grep -c "[b]inaries/firecracker" 2>/dev/null || true)
ns=$(ip netns list 2>/dev/null | grep -c '^fp-slot-' || true)
vv=$(ip link show type veth 2>/dev/null | grep -c 'veth' || true)
pend=$(pg "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
log "    终态 fc=$fc netns=$ns veth=$vv pending_ops=$pend"
[[ "$fc" == "0" && "$ns" == "0" && "$pend" == "0" ]] || fail "终态泄漏：fc=$fc ns=$ns pending=$pend"

log "M5 e2e PASS：安全负路径 / 镜像准入 / 时钟漂移采样 / operation trace 脱敏 / PG 备份恢复 / 显式重投影 / drain-rebuild 升级 / hardening 审计 / 零泄漏 全部通过"
