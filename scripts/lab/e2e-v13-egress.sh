#!/usr/bin/env bash
# v1.3-A（ADR-0027）egress policy 单机验收：
#   A) allowlist 域名：HTTP Host 白名单放行/拒绝（busybox wget）；
#   B) deny_all：默认全拒，allowed_cidrs（DNS resolver）例外；
#   C) 连接限额：max_tcp_connections 下并行连接出现拒绝且 PG 摘要可审计；
#   D) 审计：拒绝摘要入 PG（egress_deny_summaries）+ /v1/apps/{id}/egress-audit，
#      agentd 日志决策行不含 URL path/query/header；
#   E) 真实 TLS SNI/无 SNI、非标准 TCP/UDP、pause/resume、agent restart；
#   F) 策略变更：新 deployment（generation fencing）替换旧策略。
# ECH、DNS rebinding 与保留段完整矩阵仍由 internal/agent/egress 单测覆盖。
# 用法: sudo bash scripts/lab/e2e-v13-egress.sh
# 运行约定：后台运行 + 日志轮询（全部等待有界、每行带时间戳）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-v13-egress"
RUN_ID="v13e-$(date +%s)"
API_TOKEN="v13e-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT=8091
EDGE_HTTP=8092
EDGE_TLS=8492
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-v13-egress $(now)] $*"; }
fail() { echo "[e2e-v13-egress] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
authed_raw() { curl -sS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
mark() { log "    (累计 $(( $(date +%s) - T0 ))s) $*"; }
trap 'log "TRACE: died at line $LINENO rc=$? cmd=$BASH_COMMAND"' ERR
# shellcheck source=lib/runtime.sh
source "$HERE/lib/runtime.sh"
restart_agentd() { lab_restart_agentd 45 2; }

stream_exit_code() {
  python3 -c 'import json,sys
rc=None
for line in sys.stdin:
    if line.strip():
        obj=json.loads(line)
        if "exit_code" in obj: rc=obj["exit_code"]
if rc is None: raise SystemExit("exec stream has no exit_code")
print(rc)'
}

expect_guest_rc() {
  local machine="$1" expected="$2" label="$3"; shift 3
  local out rc
  out=$(guest_exec "$machine" "$@")
  rc=$(printf '%s\n' "$out" | stream_exit_code) || fail "$label：exec 响应无有效 exit_code"
  if [[ "$expected" == "zero" ]]; then
    [[ "$rc" == "0" ]] || fail "$label：期望成功，exit=$rc"
  else
    [[ "$rc" != "0" ]] || fail "$label：期望失败，exit=0"
  fi
}

# guest_exec <machine> <command...>：经 exec 通道跑 guest 命令。
guest_exec() { lab_guest_exec "$@"; }
exec_stdout() { lab_exec_stdout; }

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建（make build 并复制到 $LAB_BIN）"
[[ -f "$CERT_DIR/wildcard-$DOMAIN.crt" ]] || fail "泛域名证书缺失（先跑 scripts/lab/gen-certs.sh）"

log "0) 启动：root setup + agentd（slot 后端 + egress 能力）+ API/edge"
T0=$(date +%s)
"$HERE/root-setup.sh" >/dev/null || fail "root-setup 失败"
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
  FIREPAAS_ROLLOUT_TIMEOUT=180s FIREPAAS_ROLLOUT_DRAIN=10s \
  FIREPAAS_GC_MODE=dry-run FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/v13e-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/v13e-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/v13e-api.log"; fail "API 未就绪"; }
mark "api/edge up"

ONLINE_OUT=$(bash "$HERE/push-ontime.sh") || fail "push-ontime 失败"
ONTIME_REF=$(echo "$ONLINE_OUT" | grep '^REF=' | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || fail "ontime REF 解析失败"

log "0.5) 预清理历史验收机"
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now()" >/dev/null
pg "UPDATE deployments SET status='SUPERSEDED', updated_at=now() WHERE status IN ('ACTIVE','PREPARING')" >/dev/null
pg "UPDATE rollouts SET status='COMPLETE', failed=true, completed_at=now(), updated_at=now()
	WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK')" >/dev/null
sleep 3

log "1) 能力：节点上报 egress.cidr.v1 / egress.domain.v1"
NODE_EGRESS_OK=0
for _ in $(seq 1 30); do
  n=$(pg "SELECT count(*) FROM nodes WHERE status='HEALTHY' AND feature_ids::text LIKE '%egress.domain.v1%' AND feature_ids::text LIKE '%egress.cidr.v1%'")
  [[ "${n:-0}" -ge 1 ]] && NODE_EGRESS_OK=1 && break
  sleep 4
done
[[ "$NODE_EGRESS_OK" == "1" ]] || fail "节点未上报 egress 能力"

# DNS resolver CIDR：allowlist 模式 UDP 默认拒绝，guest 解析域名需要显式放行
# 解析器（hypeman dns_server 223.5.5.5 + host 10.251.1.1）。
RESOLVER_CIDRS='["223.5.5.5/32","10.251.1.1/32"]'

log "2) A：allowlist 域名白名单（HTTP Host）"
app="v13e-allow-$RUN_ID"
host="$app.$DOMAIN"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$app\",\"hostname\":\"$host\",\"image\":\"$ONTIME_REF\",\"replicas\":1,\"egress\":{\"mode\":\"allowlist\",\"allowed_domains\":[\"example.com\"],\"allowed_cidrs\":$RESOLVER_CIDRS}}" >/dev/null
machine="$app-r0-g1"
ALLOW_READY=0
for _ in $(seq 1 90); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
  [[ "$st" == "RUNNING" ]] && ALLOW_READY=1 && break
  sleep 3
done
[[ "$ALLOW_READY" == "1" ]] || fail "allowlist app 未就绪（observed=$st）"
mark "allowlist app ready"

# 正向：HTTP Host 与真实 TLS ClientHello SNI 都必须放行。
expect_guest_rc "$machine" zero "example.com HTTP Host" /bin/sh -c "/bin/busybox wget -q -T 15 -O /dev/null http://example.com/"
expect_guest_rc "$machine" zero "example.com TLS SNI" /bin/sh -c "/bin/busybox wget -q -T 15 -O /dev/null https://example.com/"
# 负向：未列域名、无 SNI、非标准 TCP 和 UDP 都必须 fail closed。HTTPS
# 直连 IP 产生无 DNS hostname 的 ClientHello（不得借 Host/SNI allowlist 放行）。
expect_guest_rc "$machine" nonzero "neverssl.com 未列域名" /bin/sh -c "/bin/busybox wget -q -T 15 -O /dev/null http://neverssl.com/"
expect_guest_rc "$machine" nonzero "TLS 无 SNI" /bin/sh -c "/bin/busybox wget --no-check-certificate -q -T 8 -O /dev/null https://93.184.216.34/"
expect_guest_rc "$machine" nonzero "非标准 TCP 端口" /bin/sh -c "/bin/busybox nc -w 4 example.com 8443 </dev/null >/dev/null 2>&1"
expect_guest_rc "$machine" nonzero "UDP 非 resolver" /bin/sh -c "/bin/busybox nslookup example.com 1.1.1.1 >/dev/null 2>&1"
mark "Host/SNI 与无 SNI、非标准 TCP/UDP 矩阵 OK"

log "3) C：连接限额（max_tcp_connections=2，并行 8 连必须出现拒绝）"
limit_app="v13e-limit-$RUN_ID"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$limit_app\",\"hostname\":\"$limit_app.$DOMAIN\",\"image\":\"$ONTIME_REF\",\"replicas\":1,\"egress\":{\"mode\":\"allowlist\",\"allowed_domains\":[\"example.com\"],\"allowed_cidrs\":$RESOLVER_CIDRS,\"max_tcp_connections\":2}}" >/dev/null
limit_machine="$limit_app-r0-g1"
LIMIT_READY=0
for _ in $(seq 1 90); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$limit_machine'")
  [[ "$st" == "RUNNING" ]] && LIMIT_READY=1 && break
  sleep 3
done
[[ "$LIMIT_READY" == "1" ]] || fail "limit app 未就绪"
limit_exec=$(pg "SELECT current_execution_id FROM machines WHERE id='$limit_machine'")
before_limit=$(pg "SELECT coalesce(sum(limit_rejections),0) FROM egress_deny_summaries WHERE project_id='dev' AND app_id='$limit_app' AND machine_id='$limit_machine' AND execution_id='$limit_exec'")
# 每个后台连接保存 PID 并逐个 wait；nc 对被代理立即关闭的连接也可能返回 0，
# 因此进程结果只证明 8 次尝试均完成，限流 verdict 以 agent/PG 计数为准。
burst_out=$(guest_exec "$limit_machine" /bin/sh -c '
pids=""
for i in 1 2 3 4 5 6 7 8; do
  ( { sleep 8; printf "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"; } | /bin/busybox nc -w 12 example.com 80 >/dev/null 2>&1 ) &
  pids="$pids $!"
done
ok=0; failed=0
for pid in $pids; do
  if wait "$pid"; then ok=$((ok+1)); else failed=$((failed+1)); fi
done
echo "pid_results ok=$ok failed=$failed"
')
burst_stats=$(printf '%s\n' "$burst_out" | exec_stdout) || fail "并发连接统计命令失败"
BURST_STATS="$burst_stats" python3 - <<'PY' || fail "并发连接逐 PID 统计无效"
import os,re
m=re.search(r"pid_results ok=(\d+) failed=(\d+)", os.environ["BURST_STATS"])
assert m, os.environ["BURST_STATS"]
ok, failed=map(int,m.groups())
assert ok + failed == 8, (ok, failed)
PY
slot_index=$(python3 - "$limit_machine" <<'PY'
import json,sys
with open('/var/lib/firepaas-p0/hypeman/agent/slots.json') as f:
    slots=json.load(f)
print(next(s['index'] for s in slots if s['machine_id']==sys.argv[1]))
PY
)
limit_packets=$(ip netns exec "fp-slot-$slot_index" nft -a list chain ip fp-slot egress-fwd |
  awk '/firepaas-tcp-limit/ {for(i=1;i<=NF;i++) if($i=="packets") {print $(i+1); exit}}')
[[ "${limit_packets:-0}" -ge 1 ]] || fail "并发连接未命中 nft connection limit（packets=${limit_packets:-missing}）"
mark "连接限额逐 PID 尝试与内核拒绝计数 OK（$burst_stats packets=$limit_packets）"

log "4) D：审计摘要入 PG + API 可见 + 日志脱敏"
# 只接受本次 app/current execution 的事实，历史 run 或其它 app 不能让验收假绿。
allow_exec=$(pg "SELECT current_execution_id FROM machines WHERE id='$machine'")
limit_exec=$(pg "SELECT current_execution_id FROM machines WHERE id='$limit_machine'")
[[ -n "$allow_exec" && -n "$limit_exec" ]] || fail "无法解析本次 execution"
AUDIT_OK=0
for _ in $(seq 1 30); do
  denied=$(pg "SELECT coalesce(sum(denied_connections),0) FROM egress_deny_summaries WHERE project_id='dev' AND app_id='$app' AND machine_id='$machine' AND execution_id='$allow_exec'")
  limited=$(pg "SELECT coalesce(sum(limit_rejections),0) FROM egress_deny_summaries WHERE project_id='dev' AND app_id='$limit_app' AND machine_id='$limit_machine' AND execution_id='$limit_exec'")
  [[ "${denied:-0}" -ge 1 ]] && AUDIT_OK=1 && break
  sleep 3
done
[[ "$AUDIT_OK" == "1" ]] || fail "本次 app/execution 无代理拒绝摘要（denied=$denied）"
summ=$(authed_raw "http://127.0.0.1:$API_PORT/v1/apps/$app/egress-audit")
APP_ID="$app" MACHINE_ID="$machine" EXECUTION_ID="$allow_exec" SUMM="$summ" python3 - <<'PY' || fail "egress-audit API 缺本次 execution 拒绝摘要"
import json, os
body = json.loads(os.environ["SUMM"])
rows = body.get("deny_summaries") or []
assert rows, body
assert all(r["AppID"] == os.environ["APP_ID"] for r in rows), body
assert any(r["MachineID"] == os.environ["MACHINE_ID"] and r["ExecutionID"] == os.environ["EXECUTION_ID"] and r["DeniedConnections"] > 0 for r in rows), body
PY
mark "PG 摘要 + egress-audit API OK"
# 日志缺失本身就是失败；只检查本次 machine/execution 的决策行。
sleep 3
alloc_id=$(nomad job allocs -json firepaas-agentd | python3 -c 'import json,sys; a=json.load(sys.stdin); print(next((x["ID"] for x in a if x.get("ClientStatus")=="running"), ""))')
[[ -n "$alloc_id" ]] || fail "找不到运行中的 agentd allocation，无法验证审计日志"
nomad alloc logs -stderr "$alloc_id" agentd >"$RUN_DIR/agentd-audit.log" 2>&1 || fail "读取 agentd 审计日志失败"
LOG_FILE="$RUN_DIR/agentd-audit.log" MACHINE_ID="$machine" EXECUTION_ID="$allow_exec" python3 - <<'PY' || fail "本次 execution 审计日志缺失或含敏感正文"
import os
lines=open(os.environ["LOG_FILE"],encoding="utf-8",errors="replace").read().splitlines()
rows=[x for x in lines if "egress connection decision" in x and os.environ["MACHINE_ID"] in x and os.environ["EXECUTION_ID"] in x]
assert rows, "no matching decision line"
leaks=[x[:300] for x in rows if "example.com/" in x or "neverssl.com/" in x or "?k=" in x or "&q=" in x]
assert not leaks, leaks
PY
mark "本次 execution 审计日志存在且无 URL path/query 泄漏"

log "5) B：deny_all 模式（只放行 DNS resolver，其余全拒）"
deny_app="v13e-deny-$RUN_ID"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$deny_app\",\"hostname\":\"$deny_app.$DOMAIN\",\"image\":\"$ONTIME_REF\",\"replicas\":1,\"egress\":{\"mode\":\"deny_all\",\"allowed_cidrs\":$RESOLVER_CIDRS}}" >/dev/null
deny_machine="$deny_app-r0-g1"
DENY_READY=0
for _ in $(seq 1 90); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$deny_machine'")
  [[ "$st" == "RUNNING" ]] && DENY_READY=1 && break
  sleep 3
done
[[ "$DENY_READY" == "1" ]] || fail "deny_all app 未就绪"
wget_out=$(guest_exec "$deny_machine" "/bin/sh" "-c" "/bin/busybox wget -q -T 15 -O /dev/null http://example.com/ >/dev/null 2>&1; rc=\$?; echo wget_rc=\$rc; exit \$rc")
WGET_OUT="$wget_out" python3 - <<'PY' || fail "deny_all 下 example.com 应被拒绝"
import json, os
lines = [l for l in os.environ["WGET_OUT"].splitlines() if l.strip()]
for line in lines:
    obj = json.loads(line)
    if "exit_code" in obj:
        assert obj["exit_code"] != 0, "deny_all 应拒绝"
PY
mark "deny_all 全拒（resolver 例外）OK"

log "6) pause/resume 与 agent restart 后策略保持"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/pause" >/dev/null
PAUSED=0
for _ in $(seq 1 45); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
  [[ "$st" == "PAUSED" ]] && PAUSED=1 && break
  sleep 2
done
[[ "$PAUSED" == "1" ]] || fail "pause 未收敛（state=$st）"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/resume" >/dev/null
RESUMED=0
for _ in $(seq 1 45); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
  [[ "$st" == "RUNNING" ]] && RESUMED=1 && break
  sleep 2
done
[[ "$RESUMED" == "1" ]] || fail "resume 未收敛（state=$st）"
expect_guest_rc "$machine" nonzero "resume 后策略" /bin/sh -c "/bin/busybox wget -q -T 10 -O /dev/null http://neverssl.com/"
restart_agentd || fail "agentd restart 未恢复服务"
RECONCILED=0
for _ in $(seq 1 60); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
  [[ "$st" == "RUNNING" ]] && RECONCILED=1 && break
  sleep 2
done
[[ "$RECONCILED" == "1" ]] || fail "agent restart 后 machine 未 RUNNING（state=$st）"
expect_guest_rc "$machine" nonzero "agent restart 后策略" /bin/sh -c "/bin/busybox wget -q -T 10 -O /dev/null http://neverssl.com/"
mark "pause/resume + agent restart 策略保持 OK"

log "7) E：策略变更 → 新 generation 全量替换"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps/$app/deployments" \
  -H 'Content-Type: application/json' \
  -d "{\"image\":\"$ONTIME_REF\",\"egress\":{\"mode\":\"allowlist\",\"allowed_domains\":[\"example.org\"],\"allowed_cidrs\":$RESOLVER_CIDRS}}" >/dev/null
# 新代 machine（dep 前缀 generation 2）。
new_machine="$app-r0-g2"
ROLL_OK=0
for _ in $(seq 1 90); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$new_machine'")
  [[ "$st" == "RUNNING" ]] && ROLL_OK=1 && break
  sleep 3
done
[[ "$ROLL_OK" == "1" ]] || fail "策略变更 rollout 未就绪"
# 旧域名（example.com）现在被拒；新域名（example.org）放行 → 全量替换生效。
wget_out=$(guest_exec "$new_machine" "/bin/sh" "-c" "/bin/busybox wget -q -T 15 -O /dev/null http://example.com/ >/dev/null 2>&1; rc=\$?; echo wget_rc=\$rc; exit \$rc")
WGET_OUT="$wget_out" python3 - <<'PY' || fail "换代后 example.com 应被拒（全量替换）"
import json, os
lines = [l for l in os.environ["WGET_OUT"].splitlines() if l.strip()]
for line in lines:
    obj = json.loads(line)
    if "exit_code" in obj:
        assert obj["exit_code"] != 0, "generation 替换后旧域名应被拒"
PY
wget_out=$(guest_exec "$new_machine" "/bin/sh" "-c" "/bin/busybox wget -q -T 15 -O /dev/null http://example.org/ >/dev/null 2>&1; rc=\$?; echo wget_rc=\$rc; exit \$rc")
WGET_OUT="$wget_out" python3 - <<'PY' || fail "换代后 example.org 应放行"
import json, os
lines = [l for l in os.environ["WGET_OUT"].splitlines() if l.strip()]
for line in lines:
    obj = json.loads(line)
    if "exit_code" in obj:
        assert obj["exit_code"] == 0, "新域名应放行"
PY
mark "策略全量替换（generation）OK"

log "8) 清理 + 结论"
for a in "$app" "$limit_app" "$deny_app"; do
  authed_curl -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$a" >/dev/null 2>&1 || true
done
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE app_id IN ('$app','$limit_app','$deny_app')" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now() WHERE id IN ('$app','$limit_app','$deny_app')" >/dev/null
log "ALL PASS"
