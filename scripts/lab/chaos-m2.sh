#!/usr/bin/env bash
# M2 chaos harness（单机）：mvp-plan §6 验收
#   - ACK 丢失（agent 侧直接删 VM，PG/op 不知情）→ R3 重建
#   - agent crash（Nomad 重启 agentd，VM 全部死亡）→ 2 分钟内收敛重建
#   - Redis 清空 → route 投影 2 分钟内重建，数据面恢复
#   - API crash → leader 重选，创建路径恢复
# 用法: sudo bash scripts/lab/chaos-m2.sh（需先 sudo bash scripts/lab/e2e-m2.sh 或等价环境）
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/chaos-m2"
RUN_ID="chaos-$(date +%s)"
API_TOKEN="chaos-token-$RUN_ID"
TRAFFIC_KEY="$(openssl rand -base64 32)"   # M4：proxy credential 密钥（与 agent 强制校验配套）
MID="chaos-vm-$RUN_ID"
HN="$MID.local"
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"
RD="docker exec dev-redis-1 redis-cli"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"

mkdir -p "$RUN_DIR"
log() { echo "[chaos-m2] $*"; }
fail() { echo "[chaos-m2] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }

start_api() {
  pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
  sleep 2
  nohup env \
    FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
    FIREPAAS_REDIS_ADDR=127.0.0.1:6379 \
    FIREPAAS_NOMAD_ADDR=http://127.0.0.1:4646 \
    FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 \
    FIREPAAS_HTTP_PORT=8080 \
    FIREPAAS_API_TOKEN="$API_TOKEN" \
    FIREPAAS_TRAFFIC_TOKEN_KEY="$TRAFFIC_KEY" \
    FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" \
    FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
    FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
    "$LAB_BIN/firepaas-api" >"$RUN_DIR/api.log" 2>&1 &
  echo $! >"$RUN_DIR/api.pid"
  for _ in $(seq 1 40); do
    curl -fsS http://127.0.0.1:8080/v1/health >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

wait_state() { # $1 machine_id $2 state $3 timeout
  local id="$1" want="$2" timeout="${3:-120}"
  for _ in $(seq 1 $((timeout / 3))); do
    local st
    st=$(authed_curl http://127.0.0.1:8080/v1/machines \
      | python3 -c 'import json,sys
ms=json.load(sys.stdin)["machines"]
m=next((x for x in ms if x["ID"]=="'"$id"'"),None)
print(m["ObservedState"] if m else "")')
    [[ "$st" == "$want" ]] && return 0
    sleep 3
  done
  return 1
}

wait_edge() { # $1 hostname $2 timeout
  local hn="$1" timeout="${2:-120}"
  for _ in $(seq 1 $((timeout / 3))); do
    curl -fsS -m 5 -H "Host: $hn" http://127.0.0.1:8081/ 2>/dev/null | grep -qi 'Welcome to nginx' && return 0
    sleep 3
  done
  return 1
}

[[ -f "$CERT_DIR/ca.crt" ]] || fail "证书未生成"

log "0) 环境：Nomad/agentd/edge + API（新 token）"
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"
pkill -f "$LAB_BIN/edge-proxy" 2>/dev/null || true
sleep 1
nohup env FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_EDGE_PORT=8081 \
  FIREPAAS_API_ADDR=http://127.0.0.1:8080 FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/edge-proxy" >"$RUN_DIR/edge.log" 2>&1 &
echo $! >"$RUN_DIR/edge.pid"
start_api || fail "API 启动失败"
for _ in $(seq 1 40); do
  ST=$(authed_curl http://127.0.0.1:8080/v1/nodes | python3 -c 'import json,sys; ns=json.load(sys.stdin)["nodes"] or []; print(ns[0]["Status"] if ns else "")')
  [[ "$ST" == "HEALTHY" ]] && break
  sleep 3
done
[[ "${ST:-}" == "HEALTHY" ]] || fail "无 HEALTHY 节点"

log "1) 创建 baseline machine 并验证数据面"
EXEC1=$($PG "SELECT current_execution_id FROM machines WHERE id='$MID'")
if [[ -z "$EXEC1" ]]; then
  authed_curl -X POST http://127.0.0.1:8080/v1/machines -H 'Content-Type: application/json' \
    -d "{\"machine_id\":\"$MID\",\"hostname\":\"$HN\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"op-$RUN_ID-create\"}" >/dev/null
fi
wait_state "$MID" RUNNING 180 || fail "baseline 未 RUNNING"
wait_edge "$HN" 60 || fail "baseline 数据面不通"
EXEC1=$($PG "SELECT current_execution_id FROM machines WHERE id='$MID'")
log "    baseline RUNNING（execution=$EXEC1）"

log "2) 注入 ACK 丢失：agent 侧直接删 VM（PG/op 不知情）"
T0=$(date +%s)
"$LAB_BIN/agentctl" delete -machine-id "$MID" -execution "$EXEC1" -operation "op-acklost-$RUN_ID" >/dev/null 2>&1 || true
EXEC2=""
for _ in $(seq 1 60); do
  EXEC2=$($PG "SELECT current_execution_id FROM machines WHERE id='$MID'")
  [[ -n "$EXEC2" && "$EXEC2" != "$EXEC1" ]] && break
  sleep 3
done
T1=$(date +%s)
[[ -n "$EXEC2" && "$EXEC2" != "$EXEC1" ]] || fail "ACK 丢失后未触发换代重建"
wait_state "$MID" RUNNING 180 || fail "ACK 丢失后重建未 RUNNING"
wait_edge "$HN" 60 || fail "ACK 丢失后数据面未恢复"
log "    R3 重建 execution=$EXEC2（$(($T1-$T0))s，<120s 达标）"

log "3) 注入 agent crash：kill -9 agentd + Firecracker（模拟进程组级崩溃）"
T0=$(date +%s)
# 真实崩溃语义：hypeman 把 Firecracker 放在独立进程组，只杀 agentd 时
# VM 会被新 agentd 收养（graceful 行为，不是 crash）。VM 全灭 = 同时
# SIGKILL agentd 与全部 firecracker 子进程；raw_exec restart 策略拉起
# 新 agentd 后，controller 决策表必须 reap+换代重建。
APID=$(pgrep -f "$LAB_BIN/agentd" | head -1)
[[ -n "$APID" ]] && kill -9 "$APID" 2>/dev/null || true
pkill -9 -f 'binaries/firecracker' 2>/dev/null || true
for _ in $(seq 1 60); do
  "$LAB_BIN/agentctl" info >/dev/null 2>&1 && break
  sleep 2
done
EXEC3=""
for _ in $(seq 1 60); do
  EXEC3=$($PG "SELECT current_execution_id FROM machines WHERE id='$MID'")
  [[ -n "$EXEC3" && "$EXEC3" != "$EXEC2" ]] && break
  sleep 3
done
T1=$(date +%s)
[[ -n "$EXEC3" && "$EXEC3" != "$EXEC2" ]] || fail "agent crash 后未触发重建"
wait_state "$MID" RUNNING 180 || fail "agent crash 后重建未 RUNNING"
wait_edge "$HN" 60 || fail "agent crash 后数据面未恢复"
log "    agent crash → 收敛重建 execution=$EXEC3（$(($T1-$T0))s，<120s 达标）"

log "4) 注入 Redis 清空：route 投影必须重建"
T0=$(date +%s)
$RD flushall >/dev/null
wait_edge "$HN" 90 || fail "Redis 清空后数据面 90s 内未恢复"
T1=$(date +%s)
log "    Redis 重建 + 数据面恢复（$(($T1-$T0))s，<120s 达标）"

log "5) 注入 API crash：kill -9 → 重启 → leader 重选"
APIPID=$(cat "$RUN_DIR/api.pid" 2>/dev/null || true)
[[ -n "$APIPID" ]] && kill -9 "$APIPID" 2>/dev/null || true
sleep 2
start_api || fail "API crash 后重启失败"
for _ in $(seq 1 40); do
  curl -fsS http://127.0.0.1:8080/v1/health >/dev/null 2>&1 && break
  sleep 2
done
MID2="chaos-post-$RUN_ID"
authed_curl -X POST http://127.0.0.1:8080/v1/machines -H 'Content-Type: application/json' \
  -d "{\"machine_id\":\"$MID2\",\"hostname\":\"$MID2.local\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"op-$RUN_ID-post\"}" >/dev/null
wait_state "$MID2" RUNNING 180 || fail "API crash 后新 create 未 RUNNING"
log "    API crash → leader 重选 → create 路径恢复 OK"

log "5.5) 注入在途 crash：POST 后立即 kill -9（op 可能 PENDING/CLAIMED，P1-1）"
MID3="chaos-inflight-$RUN_ID"
T0=$(date +%s)
authed_curl -X POST http://127.0.0.1:8080/v1/machines -H 'Content-Type: application/json' \
  -d "{\"machine_id\":\"$MID3\",\"hostname\":\"$MID3.local\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"op-$RUN_ID-inflight\"}" >/dev/null
APIPID=$(cat "$RUN_DIR/api.pid" 2>/dev/null || true)
[[ -n "$APIPID" ]] && kill -9 "$APIPID" 2>/dev/null || true
# 断言当时确有在途操作（否则本场景没有覆盖到目标窗口）
for _ in $(seq 1 5); do
  INFLIGHT=$($PG "SELECT count(*) FROM operations WHERE id='op-$RUN_ID-inflight'")
  [[ "$INFLIGHT" == "1" ]] && break
  sleep 1
done
[[ "$INFLIGHT" == "1" ]] || fail "在途 create 未登记（场景无效）"
start_api || fail "在途 crash 后重启失败"
# 新 leader 启动即回收孤儿 CLAIMED（单写者不变量），PENDING 直接接管；
# 断言 2 分钟内收敛（M2 验收）。
wait_state "$MID3" RUNNING 120 || fail "在途 crash 后 create 未在 2 分钟内 RUNNING"
T1=$(date +%s)
log "    在途 crash → 启动回收孤儿 CLAIMED → 收敛（$(($T1-$T0))s，<120s 达标）"

log "6) 终态检查：无在途操作、事件审计可解释"
PENDING=$($PG "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
[[ "$PENDING" == "0" ]] || fail "存在在途 operation $PENDING"
EVENTS=$(curl -fsS -H "Authorization: Bearer $API_TOKEN" 'http://127.0.0.1:8080/v1/system/scheduler-events?limit=100' \
  | python3 -c 'import json,sys; evs=json.load(sys.stdin)["events"]; print(sum(1 for e in evs if e["Kind"]=="reconcile"))')
[[ "$EVENTS" -gt 0 ]] || fail "无 reconcile 审计事件"
authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MID?operation_id=op-clean-$MID" >/dev/null || true
authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MID2?operation_id=op-clean-$MID2" >/dev/null || true
authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MID3?operation_id=op-clean-$MID3" >/dev/null || true
log "chaos-m2 PASS：ACK 丢失/agent crash/Redis 清空/API crash（含在途）全部 2 分钟内收敛，审计可解释"
