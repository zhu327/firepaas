#!/usr/bin/env bash
# LAYER: l2-regression  PREREQ: nomad,agentd,root  DESTRUCTIVE: no  FROZEN: no
# ADR-0041 e2e harness（单机）：基于并发的自动弹性验收
#   1) 阶跃 2x：并发负载阶跃后 60s 内 desired 跟上
#   2) 归零与冷起：min=0 时稳定窗后 desired=0；再打流量 unserved>0 且 30s 内 desired>=1
#   3) 信号丢失不缩容：删 key / 全字段过期 / 过期非零 field 时 desired 不变
#   4) 发布冻结：PREPARING 期间 desired 不变（含高负载/panic 信号）；rollback 后恢复跟随
#   5) 配额冻结：quota 拒绝后不再加副本；缩容仍可用；超时自动解冻（事件可见）
#   6) 手动接管：POST /scale 后 enabled=false 且 desired 不再被改写
#   7) panic 快扩：hard_rejected 信号触发 eff+2 上限放大（事件 reason=panic）
# 用法: sudo bash scripts/lab/e2e-autoscale.sh
# 说明：本脚本自起专属 API（8091，自带 token；退出时清理），信号经 Redis
# autoscale:{host} 键直接注入（与 reporter 同线格式；reporter 本体由单测覆盖）。
# B 段（发布冻结/恢复）需要真实 VM（等待 READY，最长约 12min）；其余段只断言
# desired 与事件，不依赖 VM。缩容稳定窗用 delay=30s、冻结超时 30s 控制总时长。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-autoscale"
RUN_ID="e2e-autoscale-$(date +%s)"
API_TOKEN="as-token-$RUN_ID"
API_PORT=8091
API="http://127.0.0.1:$API_PORT"
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"
REDIS="docker exec dev-redis-1 redis-cli"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

log() { echo "[e2e-autoscale $(date +%H:%M:%S)] $*"; }
fail() { echo "[e2e-autoscale] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }

[[ -f "$LAB_BIN/firepaas-api" ]] || fail "二进制未构建（make build）"
[[ -f "$CERT_DIR/ca.crt" ]] || fail "证书未生成：bash scripts/lab/gen-certs.sh"

cleanup() {
  stop_keeper 2>/dev/null || true
  for a in $APPS_CREATED; do
    curl -s -m 10 -H "Authorization: Bearer $API_TOKEN" -X DELETE "$API/v1/apps/$a" >/dev/null || true
  done
  [[ -n "${QUOTA_SAVED:-}" ]] && restore_quota || true
  [[ -n "${API_PID:-}" ]] && kill "$API_PID" 2>/dev/null || true
}
APPS_CREATED=""
QUOTA_SAVED=""
trap cleanup EXIT

# ---- 0) 镜像与 API 启动（照 e2e-m5：digest 强制 + 允许列表，自带 token）----
ONLINE_OUT=$(bash "$HERE/push-ontime.sh") || fail "push-ontime 失败"
ONTIME_REF=$(echo "$ONLINE_OUT" | grep '^REF=' | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || fail "ontime REF 解析失败"

MASTER_KEY="$(openssl rand -base64 32)"
TRAFFIC_KEY="$(openssl rand -base64 32)"
pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
sleep 1
nohup env FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_NOMAD_ADDR=http://127.0.0.1:4646 \
  FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 FIREPAAS_HTTP_PORT=$API_PORT FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_SECRETS_MASTER_KEY="$MASTER_KEY" FIREPAAS_TRAFFIC_TOKEN_KEY="$TRAFFIC_KEY" \
  FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  FIREPAAS_AUTOSCALE_QUOTA_FREEZE=30s FIREPAAS_ROLLOUT_DRAIN=90s \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/api.log" 2>&1 &
API_PID=$!
for _ in $(seq 1 60); do
  authed_curl "$API/v1/health" >/dev/null 2>&1 && break
  kill -0 "$API_PID" 2>/dev/null || { tail -20 "$RUN_DIR/api.log"; fail "API 未就绪"; }
  sleep 1
done
authed_curl "$API/v1/health" >/dev/null || fail "API 未就绪"
log "0) API 就绪（:$API_PORT，freeze=30s）"

# ---- helpers ----
inject() { # $1 host $2 ewma $3 rps $4 hard $5 unserved
  local ts
  ts=$(($(date +%s%3N)))
  $REDIS HSET "autoscale:$1" e2e \
    "{\"ewma\":$2,\"rps\":$3,\"hard_rejected\":$4,\"unserved\":$5,\"ts_ms\":$ts,\"win_ms\":10000}" >/dev/null
  $REDIS EXPIRE "autoscale:$1" 20 >/dev/null
}
# 信号 keeper：真 edge 每 5s 刷新信号；单次 inject 的 key 20s 后过期，
# 长等待必须持续刷新，否则“信号丢失 hold”会误杀断言（A5 实测教训）。
start_keeper() { # $1 host, $2.. = inject 参数；KEEPER_STALE=1 时同带过期非零 field
  local host="$1"; shift
  ( while true; do
      inject "$host" "$@"
      [[ "${KEEPER_STALE:-0}" == "1" ]] && inject_stale "$host"
      sleep 8
    done ) &
  KEEPER_PID=$!
}
stop_keeper() {
  if [[ -n "${KEEPER_PID:-}" ]]; then kill "$KEEPER_PID" 2>/dev/null || true; KEEPER_PID=""; fi
  KEEPER_STALE=0
}
inject_stale() { # $1 host：过期非零 field（60s 前）
  local ts
  ts=$(($(date +%s%3N) - 60000))
  $REDIS HSET "autoscale:$1" edge-dead \
    "{\"ewma\":9,\"rps\":0,\"hard_rejected\":0,\"unserved\":0,\"ts_ms\":$ts,\"win_ms\":10000}" >/dev/null
  $REDIS EXPIRE "autoscale:$1" 20 >/dev/null
}
app_json() { authed_curl "$API/v1/apps/$1"; }
# hold_rollout 累计计数：冻结若被别的原因空过（如无 fresh 信号），此处为 0。
hold_rollout_count() {
  curl -fsS -m 10 "$API/metrics" | python3 -c "
import sys
for line in sys.stdin:
    if line.startswith('firepaas_autoscale_decisions_total') and 'result=\"hold_rollout\"' in line:
        print(line.split()[-1]); break
else: print(0)"
}
desired() { app_json "$1" | python3 -c "import json,sys; print(json.load(sys.stdin)['app']['DesiredReplicas'])"; }
policy3() { authed_curl "$API/v1/apps/$1/autoscale" | python3 -c "import json,sys; d=json.load(sys.stdin)['autoscale']; print(d['enabled'], d['min_replicas'], d['max_replicas'])"; }
rollout_status() { app_json "$1" | python3 -c "
import json,sys
r=json.load(sys.stdin).get('active_rollout')
print(r['Status'] if r else 'NONE')"; }
ready_count() { app_json "$1" | python3 -c "
import json,sys
ms=json.load(sys.stdin).get('machines') or []
print(sum(1 for m in ms if m.get('ObservedState') in ('RUNNING','PAUSED') and m.get('ObservedReadiness') in ('READY','UNCONFIGURED')))"; }
wait_desired() { # $1 app $2 want $3 timeout_sec
  for _ in $(seq 1 "$3"); do
    [[ "$(desired "$1")" == "$2" ]] && return 0
    sleep 5
  done
  return 1
}
wait_ready() { # $1 app $2 want $3 timeout_sec
  for _ in $(seq 1 "$3"); do
    [[ "$(ready_count "$1")" == "$2" ]] && return 0
    sleep 5
  done
  return 1
}
wait_no_rollout() { # $1 app $2 timeout_sec
  for _ in $(seq 1 "$2"); do
    [[ "$(rollout_status "$1")" == "NONE" ]] && return 0
    sleep 5
  done
  return 1
}
# root 身份读事件流必须显式带 project_id（listEvents 强制）；本脚本 app 全在 dev。
EVPROJ=dev
has_event() { # $1 app $2 action：autoscale.decision 事件断言（details 紧凑 JSON）
  authed_curl "$API/v1/events?project_id=$EVPROJ&app_id=$1" | grep -q "\"action\":\"$2\""
}
has_etype() { # $1 app $2 type：任意用户事件类型断言
  authed_curl "$API/v1/events?project_id=$EVPROJ&app_id=$1" | grep -q "\"type\":\"$2\""
}
mkapp() { # $1 app $2 host $3 replicas [$4 min $5 max]
  local min="${4:-1}" max="${5:-5}"
  authed_curl -X POST "$API/v1/apps" \
    -d "{\"app_id\":\"$1\",\"hostname\":\"$2\",\"image\":\"$ONTIME_REF\",\"replicas\":$3}" >/dev/null
  authed_curl -X PUT "$API/v1/apps/$1/autoscale" \
    -d "{\"enabled\":true,\"min_replicas\":$min,\"max_replicas\":$max,\"target_concurrency\":20,\"scale_down_delay_sec\":30,\"panic_threshold\":2.0}" >/dev/null
  APPS_CREATED="$APPS_CREATED $1"
  $REDIS DEL "autoscale:$2" >/dev/null || true
}

# ---- A) 阶跃 / 信号丢失 / 慢缩 ----
AAPP="as-e2e-a-$RANDOM"; AHOST="$AAPP.local"
log "A1) 建 app 并开策略"
mkapp "$AAPP" "$AHOST" 2
[[ "$(policy3 "$AAPP")" == "True 1 5" ]] || fail "策略回读不对"

log "A2) 阶跃：served=45 → want=3，60s 内跟上"
start_keeper "$AHOST" 45 90 0 0
wait_desired "$AAPP" 3 12 || fail "阶跃未扩容（desired=$(desired "$AAPP")）"
stop_keeper
has_event "$AAPP" scale_up || fail "缺 scale_up 事件"

log "A3) 信号丢失：删 key → hold 不动"
$REDIS DEL "autoscale:$AHOST" >/dev/null
sleep 15
[[ "$(desired "$AAPP")" == "3" ]] || fail "信号丢失时被改写"

log "A4) 过期非零 field → 禁缩"
KEEPER_STALE=1
start_keeper "$AHOST" 0 0 0 0
sleep 45 # 稳定窗（30s）走完也必须不动（keeper 让过期 field 持续在场）
[[ "$(desired "$AAPP")" == "3" ]] || fail "过期非零信号未冻结缩容"
stop_keeper

log "A5) 慢缩：fresh 零信号 + 30s 稳定窗 → 1"
$REDIS DEL "autoscale:$AHOST" >/dev/null
start_keeper "$AHOST" 0 0 0 0
wait_desired "$AAPP" 1 14 || fail "稳定窗后未缩容"
stop_keeper
has_event "$AAPP" scale_down || fail "缺 scale_down 事件"

# ---- B) 归零与冷起（min=0）----
BAPP="as-e2e-b-$RANDOM"; BHOST="$BAPP.local"
log "B1) min=0 建 app（replicas=1）"
mkapp "$BAPP" "$BHOST" 1 0 5
log "B2) 零流量 30s 窗后 desired=0（rps==0 才缩零）"
start_keeper "$BHOST" 0 0 0 0
wait_desired "$BAPP" 0 14 || fail "未缩零（desired=$(desired "$BAPP")）"
stop_keeper
log "B3) 冷起：unserved 到达 → 30s 内 desired>=1"
start_keeper "$BHOST" 0 3 0 401
for _ in $(seq 1 6); do
  [[ "$(desired "$BAPP")" != "0" ]] && break
  sleep 5
done
[[ "$(desired "$BAPP")" != "0" ]] || fail "冷起失败（desired=0）"
stop_keeper
$REDIS HGETALL "autoscale:$BHOST" | grep -q unserved || fail "信号键无 unserved"

# ---- C) 发布冻结与恢复（需真实 VM）----
CAPP="as-e2e-c-$RANDOM"; CHOST="$CAPP.local"
log "C1) 建 app 等 1 VM READY（最长 10min）"
mkapp "$CAPP" "$CHOST" 1
wait_ready "$CAPP" 1 120 || fail "VM 未 READY"
log "C2) 发新代 → PREPARING 期间高负载不动"
authed_curl -X POST "$API/v1/apps/$CAPP/deployments" -d "{\"image\":\"$ONTIME_REF\"}" >/dev/null
[[ "$(rollout_status "$CAPP")" == "PREPARING" ]] || fail "无 PREPARING rollout"
start_keeper "$CHOST" 80 160 0 0
sleep 25
[[ "$(rollout_status "$CAPP")" != "NONE" ]] || fail \
  "断言时 rollout 已结束（drain 未覆盖断言窗口，时序竞态）"
[[ "$(desired "$CAPP")" == "1" ]] || fail "发布中 desired 被改写（$(desired "$CAPP")）"
[[ "$(hold_rollout_count)" -gt 0 ]] || fail "发布中无 hold_rollout（冻结判断空过）"
stop_keeper
log "C3) panic 信号在发布中同样冻结"
start_keeper "$CHOST" 5 10 1 0
sleep 15
[[ "$(rollout_status "$CAPP")" != "NONE" ]] || fail \
  "断言时 rollout 已结束（drain 未覆盖断言窗口，时序竞态）"
[[ "$(desired "$CAPP")" == "1" ]] || fail "发布中 panic 扩容未冻结"
stop_keeper
log "C4) rollback → rollout 结束 → 恢复跟随（hold_rollout 此后不再增长）"
authed_curl -X POST "$API/v1/apps/$CAPP/rollback" >/dev/null
wait_no_rollout "$CAPP" 144 || fail "rollback 未结束"
start_keeper "$CHOST" 45 90 0 0
wait_desired "$CAPP" 3 12 || fail "恢复后未扩容"
stop_keeper

# ---- D) 配额冻结（placement 前即拒绝，无需 VM）----
DAPP="as-e2e-d-$RANDOM"; DHOST="$DAPP.local"
log "D1) 建 app（replicas=1），记录项目配额"
mkapp "$DAPP" "$DHOST" 1
PROJ=$(app_json "$DAPP" | python3 -c "import json,sys; print(json.load(sys.stdin)['app']['ProjectID'])")
QUOTA_SAVED=$(authed_curl "$API/v1/projects/$PROJ/quota")
qput() { # $1 machine_concurrency：其余沿用现值，需 revision
  local cur rev vcpu mem disk sess
  cur=$(authed_curl "$API/v1/projects/$PROJ/quota")
  rev=$(echo "$cur" | python3 -c "import json,sys; print(json.load(sys.stdin)['revision'])")
  vcpu=$(echo "$cur" | python3 -c "import json,sys; print(json.load(sys.stdin)['vcpu_quota'])")
  mem=$(echo "$cur" | python3 -c "import json,sys; print(json.load(sys.stdin)['mem_mib_quota'])")
  disk=$(echo "$cur" | python3 -c "import json,sys; print(json.load(sys.stdin)['disk_mib_quota'])")
  sess=$(echo "$cur" | python3 -c "import json,sys; print(json.load(sys.stdin)['runtime_session_concurrency'])")
  authed_curl -X PUT "$API/v1/projects/$PROJ/quota" \
    -d "{\"vcpu_quota\":$vcpu,\"mem_mib_quota\":$mem,\"disk_mib_quota\":$disk,\"machine_concurrency\":$1,\"runtime_session_concurrency\":$sess,\"revision\":$rev}" >/dev/null
}
restore_quota() {
  local cur rev
  cur=$(authed_curl "$API/v1/projects/$PROJ/quota")
  rev=$(echo "$cur" | python3 -c "import json,sys; print(json.load(sys.stdin)['revision'])")
  echo "$QUOTA_SAVED" | python3 -c "import json,sys; d=json.load(sys.stdin); d['revision']=$rev; print(json.dumps(d))" \
    | { authed_curl -X PUT "$API/v1/projects/$PROJ/quota" -d @- >/dev/null || true; }
  QUOTA_SAVED=""
}
log "D2) 收紧 machine_concurrency 至存量 → 高负载不再加副本"
USAGE=$(pg "SELECT count(*) FROM machines m JOIN apps a ON a.id=m.app_id WHERE a.project_id='$PROJ' AND m.desired_state!='DELETED';" | tr -d ' ')
qput "$USAGE"
start_keeper "$DHOST" 80 160 0 0
sleep 30 # 足够一次扩容 CAS + create 派发撞配额
D1="$(desired "$DAPP")"
stop_keeper
start_keeper "$DHOST" 95 190 0 0 # 更强信号
sleep 20
[[ "$(desired "$DAPP")" == "$D1" ]] || fail "配额冻结后仍在加副本"
stop_keeper
has_event "$DAPP" freeze || fail "缺 freeze 事件"
log "D3) 缩容通道开放：零信号 30s 窗 → 照缩"
$REDIS DEL "autoscale:$DHOST" >/dev/null
start_keeper "$DHOST" 0 0 0 0
[[ "$D1" != "1" ]] && wait_desired "$DAPP" 1 14 || true
stop_keeper
[[ "$(desired "$DAPP")" == "1" ]] || fail "冻结期缩容被阻断"
log "D4) 配额恢复 → 拒绝停止 30s 后自动解冻：unfreeze 事件可见"
# 冻结由每次配额拒绝续期：配额不恢复则拒绝不停、unfreeze 永不到（符合设计：
# 配额一直爆就不该解冻）。先恢复配额让拒绝停止，再等冻结超时。
restore_quota
start_keeper "$DHOST" 0 0 0 0
sleep 60
stop_keeper
has_event "$DAPP" unfreeze || fail "缺 unfreeze 事件"
log "D5) 配额恢复（D4 已恢复，此处确认）"
[[ -z "${QUOTA_SAVED:-}" ]] || fail "配额未恢复"

# ---- E) 手动接管 ----
EAPP="as-e2e-e-$RANDOM"; EHOST="$EAPP.local"
log "E1) 建 app 开策略，高负载下 POST /scale 接管"
mkapp "$EAPP" "$EHOST" 2
start_keeper "$EHOST" 80 160 0 0
authed_curl -X POST "$API/v1/apps/$EAPP/scale" -d '{"replicas":2}' | grep -q '"desired_replicas":2' || fail "接管 scale 失败"
[[ "$(policy3 "$EAPP")" == "False 1 5" ]] || fail "接管后未关闭 enabled"
has_etype "$EAPP" autoscale.takeover || fail "缺 takeover 事件"
sleep 15
[[ "$(desired "$EAPP")" == "2" ]] || fail "接管后 desired 被改写"
stop_keeper

log "清理"
for a in $APPS_CREATED; do
  authed_curl -X DELETE "$API/v1/apps/$a" >/dev/null || true
done
APPS_CREATED=""
kill "$API_PID" 2>/dev/null || true
API_PID=""
trap - EXIT
log "PASS：autoscale e2e 全绿"
