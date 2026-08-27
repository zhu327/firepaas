#!/usr/bin/env bash
# M2 e2e harness（单机）：mvp-plan §6 验收
#   1) 同一 replica ordinal 1000 次并发重试 → 1 machine / 1 execution
#   2) 同一 deployment 不同 ordinal 并发创建 → 各 1 个 machine
#   3) 20 轮创建/删除 → 无 VM/TAP/bridge/Redis lease/PG 在途残留
#   4) hostname → edge → agent proxy(TLS) → VM → HTTP 200
#   5) /v1/nodes 节点投影 + /metrics 计数存在
# 用法: sudo bash scripts/lab/e2e-m2.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="/home/zty/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-m2"
RUN_ID="e2e-m2-$(date +%s)"
API_TOKEN="e2e-m2-token-$RUN_ID"
TRAFFIC_KEY="$(openssl rand -base64 32)"   # M4：proxy credential 密钥（与 agent 强制校验配套）
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"
RD="docker exec dev-redis-1 redis-cli"

export PATH="$LAB_BIN:/home/zty/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"

mkdir -p "$RUN_DIR"
log() { echo "[e2e-m2] $*"; }
fail() { echo "[e2e-m2] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }

wait_machine_state() { # $1 machine_id  $2 state  $3 timeout_sec
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

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建"
[[ -f "$CERT_DIR/ca.crt" ]] || fail "证书未生成：bash scripts/lab/gen-certs.sh"

log "0) root setup + Nomad/agentd（最新二进制）"
"$HERE/root-setup.sh" >/dev/null
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"
nomad job restart -on-error fail firepaas-agentd >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
  "$LAB_BIN/agentctl" info >/dev/null 2>&1 && break
  sleep 2
done

log "1) 启动 control-plane API + edge-proxy"
pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
pkill -f "$LAB_BIN/edge-proxy" 2>/dev/null || true
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
nohup env \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 \
  FIREPAAS_EDGE_PORT=8081 \
  FIREPAAS_API_ADDR=http://127.0.0.1:8080 \
  FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" \
  FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/edge-proxy" >"$RUN_DIR/edge.log" 2>&1 &
echo $! >"$RUN_DIR/edge.pid"

for _ in $(seq 1 40); do
  curl -fsS http://127.0.0.1:8080/v1/health >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS http://127.0.0.1:8080/v1/health >/dev/null || fail "API 未就绪"

log "2) 等待节点 HEALTHY（Nomad discovery + ServiceInfo）"
for _ in $(seq 1 40); do
  ST=$(authed_curl http://127.0.0.1:8080/v1/nodes \
    | python3 -c 'import json,sys; ns=json.load(sys.stdin)["nodes"] or []; print(ns[0]["Status"] if ns else "")')
  [[ "$ST" == "HEALTHY" ]] && break
  sleep 3
done
[[ "${ST:-}" == "HEALTHY" ]] || fail "无 HEALTHY 节点（当前 $ST）"

log "2.5) 预清理：删除历史验收机（保证残留检查与重跑幂等）"
PRE=$(authed_curl http://127.0.0.1:8080/v1/machines \
  | python3 -c 'import json,sys
ms=json.load(sys.stdin)["machines"]
print(" ".join(m["ID"] for m in ms if m["DesiredState"] not in ("DELETED",)))')
if [[ -n "$PRE" ]]; then
  for MID in $PRE; do
    authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MID?operation_id=op-prec-$MID-$RUN_ID" >/dev/null || true
  done
  for MID in $PRE; do
    for _ in $(seq 1 60); do
      ST=$($PG "SELECT desired_state FROM machines WHERE id='$MID'")
      [[ "$ST" == "DELETED" || -z "$ST" ]] && break
      sleep 3
    done
  done
  sleep 10
fi
log "    预清理完成: $PRE"

log "3) 验收 1：同一 ordinal 1000 次并发重试 → 1 machine/execution"
APP1="app-m2-retry"; DEP1="dep-m2-retry"; HOST1="m2-retry.local"; OP1="op-m2-retry-$RUN_ID"
BODY1="{\"app_id\":\"$APP1\",\"deployment_id\":\"$DEP1\",\"hostname\":\"$HOST1\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"replica_ordinal\":0,\"operation_id\":\"$OP1\"}"
export API_TOKEN BODY1
seq 1 1000 | xargs -P 32 -I{} bash -c 'curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $API_TOKEN" -X POST http://127.0.0.1:8080/v1/machines -H "Content-Type: application/json" -d "$BODY1"' | sort | uniq -c >"$RUN_DIR/retry-codes.txt"
log "    HTTP 分布: $(cat "$RUN_DIR/retry-codes.txt" | tr '\n' ' ')"
grep -q "202" "$RUN_DIR/retry-codes.txt" || fail "并发重试没有 202 响应"
[[ $(grep -vc 202 "$RUN_DIR/retry-codes.txt" || true) -eq 0 ]] || fail "并发重试出现非 202 响应"
COUNT=$($PG "SELECT count(*) FROM machines WHERE app_id='$APP1'")
[[ "$COUNT" == "1" ]] || fail "同 ordinal 并发后 machine 数=$COUNT，期望 1"
EXECS=$($PG "SELECT count(DISTINCT execution_id) FROM operations WHERE id='$OP1'")
[[ "$EXECS" == "1" ]] || fail "同 ordinal 并发后 execution 数=$EXECS，期望 1"
wait_machine_state "${APP1}-r0" RUNNING 180 || fail "retry machine 未 RUNNING"
log "    1000 并发重试 → 1 machine / 1 execution OK"

log "4) 验收 2：同一 deployment 不同 ordinal 并发创建"
DEP2="dep-m2-ord"; APP2="app-m2-ord"
for i in 1 2 3; do
  BODY="{\"app_id\":\"$APP2\",\"deployment_id\":\"$DEP2\",\"hostname\":\"m2-ord$i.local\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"replica_ordinal\":$i,\"operation_id\":\"op-m2-ord$i-$RUN_ID\"}"
  echo "$BODY" >"$RUN_DIR/ord$i.json"
done
PIDS=""
for i in 1 2 3; do
  (authed_curl -X POST http://127.0.0.1:8080/v1/machines -H 'Content-Type: application/json' -d "@$RUN_DIR/ord$i.json" >"$RUN_DIR/ord$i.resp") &
  PIDS="$PIDS $!"
done
# 只等这三个 create，绝不能裸 wait（会等到 API/edge 这类长期后台任务）。
wait $PIDS
COUNT=$($PG "SELECT count(*) FROM machines WHERE app_id='$APP2'")
[[ "$COUNT" == "3" ]] || fail "3 个 ordinal 并发后 machine 数=$COUNT，期望 3"
for i in 1 2 3; do wait_machine_state "${APP2}-r$i" RUNNING 240 || fail "ordinal $i 未 RUNNING"; done
log "    3 个 ordinal 并发 → 3 machines OK"

log "5) 验收 3：hostname → edge → VM HTTP 200（轮询至 VM 启动）"
for _ in $(seq 1 30); do
  if curl -fsS -m 5 -H "Host: $HOST1" http://127.0.0.1:8081/ 2>/dev/null | grep -qi 'Welcome to nginx'; then
    log "    HTTP 200 OK"
    break
  fi
  sleep 3
  [[ $_ -eq 30 ]] && fail "edge 在 90s 内未返回 nginx 页面"
done

log "6) 验收 4：20 轮创建/删除，无残留"
for i in $(seq 1 20); do
  MID="m2-round-$RUN_ID-$i"; HN="$MID.local"; OPID="op-round-$i-$RUN_ID"
  authed_curl -X POST http://127.0.0.1:8080/v1/machines -H 'Content-Type: application/json' \
    -d "{\"machine_id\":\"$MID\",\"hostname\":\"$HN\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"$OPID\"}" >/dev/null
  wait_machine_state "$MID" RUNNING 240 || fail "round $i create 未 RUNNING"
  curl -fsS -m 10 -H "Host: $HN" http://127.0.0.1:8081/ | grep -qi 'Welcome to nginx' || fail "round $i 数据面不通"
  authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MID?operation_id=op-del-$i-$RUN_ID" >/dev/null
  for _ in $(seq 1 40); do
    ST=$($PG "SELECT desired_state FROM machines WHERE id='$MID'")
    [[ "$ST" == "DELETED" ]] && break
    sleep 3
  done
  [[ "$ST" == "DELETED" ]] || fail "round $i delete 未收敛"
  log "    round $i OK"
done
sleep 10
log "    round 1-20 完成（残留检查在清理验收机之后）"

log "7) 清理验收 1/2 的 machine"
for MID in "${APP1}-r0" "${APP2}-r1" "${APP2}-r2" "${APP2}-r3"; do
  authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MID?operation_id=op-clean-$MID-$RUN_ID" >/dev/null || true
done
for MID in "${APP1}-r0" "${APP2}-r1" "${APP2}-r2" "${APP2}-r3"; do
  for _ in $(seq 1 40); do
    ST=$($PG "SELECT desired_state FROM machines WHERE id='$MID'")
    [[ "$ST" == "DELETED" ]] && break
    sleep 3
  done
done
sleep 10

log "7.5) 泄漏检查：轮询至无 VM/TAP/route/lease/在途残留（收敛而非瞬时）"
FC=1; TAPS=1; ROUTES=1; RESV_OP=1; PENDING=1
for _ in $(seq 1 30); do
  FC=$(ps -eo args | grep -c '[f]irecracker' || true)
  TAPS=$(ip link 2>/dev/null | grep -c ' tap' || true)
  ROUTES=$($RD keys 'route:*' 2>/dev/null | sed '/^$/d' | wc -l)
  RESV_OP=$($RD keys 'resv:op:*' 2>/dev/null | sed '/^$/d' | wc -l)
  PENDING=$($PG "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
  [[ "$FC" == "0" && "$TAPS" == "0" && "$ROUTES" == "0" && "$RESV_OP" == "0" && "$PENDING" == "0" ]] && break
  sleep 3
done
log "    残留: firecracker=$FC tap=$TAPS route_keys=$ROUTES resv_op_keys=$RESV_OP 在途op=$PENDING"
[[ "$FC" == "0" ]] || fail "残留 Firecracker 进程 $FC"
[[ "$TAPS" == "0" ]] || fail "残留 TAP 接口 $TAPS"
[[ "$ROUTES" == "0" ]] || fail "残留 route 投影 $ROUTES"
[[ "$RESV_OP" == "0" ]] || fail "残留 Redis 预约记录 $RESV_OP"
[[ "$PENDING" == "0" ]] || fail "残留在途 operation $PENDING"

log "8) /v1/nodes 与 /metrics"
authed_curl http://127.0.0.1:8080/v1/nodes | python3 -c 'import json,sys; ns=json.load(sys.stdin)["nodes"]; assert ns and ns[0]["Status"]=="HEALTHY", "nodes 投影异常"' || fail "nodes 投影异常"
METRICS=$(curl -fsS http://127.0.0.1:8080/metrics)
echo "$METRICS" | grep -q 'firepaas_placements_total' || fail "metrics 缺少 placements 计数"
echo "$METRICS" | grep -q 'firepaas_reconcile_actions_total' || fail "metrics 缺少 reconcile 计数"
log "    nodes/metrics OK"

log "M2 e2e PASS：并发幂等、多 ordinal、数据面、20 轮无泄漏、可观测全部通过"
