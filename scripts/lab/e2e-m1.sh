#!/usr/bin/env bash
# M1 e2e harness（单机）：一键验证
#   authenticated API → PG operations → controller → agent mTLS → observed
#   → Redis route catalog → edge → agent proxy(TLS) → Firecracker VM → HTTP 200
# 覆盖 mvp-plan §5 验收：幂等重试/冲突拒绝、agent 重启重放、身份隔离、
# route 投影清理（M1 评审 P1/P2 修复后扩充）。
# 用法: sudo bash scripts/lab/e2e-m1.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e"
RUN_ID="e2e-$(date +%s)"
HOSTNAME="$RUN_ID.local"
# machine_id 由服务端按 (app_id, replica_ordinal) 生成（P0：fence 字段不再接受客户端提交）。
MACHINE_ID="$RUN_ID-r0"
API_TOKEN="e2e-token-$RUN_ID"
# M4：execution-bound proxy credential（旧脚本与新链路共用同一密钥；
# API 不配 key 则不下发凭证，agent 默认强制校验会拒绝 create）。
TRAFFIC_KEY="$(openssl rand -base64 32)"
OP_CREATE="op-$RUN_ID-create"
OP_DELETE="op-$RUN_ID-delete"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"

mkdir -p "$RUN_DIR"

log() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

authed_curl() { curl -fsS -H "Authorization: Bearer $API_TOKEN" "$@"; }

# restart_agentd 原地重启 agentd 任务（raw_exec 重新执行磁盘上的二进制）。
# 非交互终端必须显式指定 -on-error 策略；重启后用 agentctl 探测就绪。
restart_agentd() {
  nomad job restart -on-error fail firepaas-agentd >/dev/null 2>&1 || return 1
  for _ in $(seq 1 60); do
    "$LAB_BIN/agentctl" info >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

[[ -f "$LAB_BIN/agentd" ]] || fail "agentd 未构建"
[[ -f "$LAB_BIN/firepaas-api" ]] || fail "firepaas-api 未构建"
[[ -f "$LAB_BIN/edge-proxy" ]] || fail "edge-proxy 未构建"
[[ -f "$CERT_DIR/ca.crt" ]] || fail "证书未生成：bash scripts/lab/gen-certs.sh"

log "0) root setup + Nomad/agentd"
"$HERE/root-setup.sh" >/dev/null
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"
# 强制重启 alloc：raw_exec 不感知磁盘上的二进制更新，必须重新拉起
# 才能保证本脚本测试的是最新构建。
restart_agentd || fail "agentd 强制重启失败（二进制更新）"
log "    agentd (re)started OK"

log "1) 启动 control-plane API（root，认证开启）"
pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
nohup env \
  FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 \
  FIREPAAS_AGENT_ADDR=127.0.0.1:5108 \
  FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 \
  FIREPAAS_HTTP_PORT=8080 \
  FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_TRAFFIC_TOKEN_KEY="$TRAFFIC_KEY" \
  FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" \
  FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/firepaas-api" >"$RUN_DIR/api.log" 2>&1 &
echo $! >"$RUN_DIR/api.pid"

log "2) 启动 edge-proxy（root）"
pkill -f "$LAB_BIN/edge-proxy" 2>/dev/null || true
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

for _ in $(seq 1 30); do
  curl -fsS http://127.0.0.1:8080/v1/health >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS http://127.0.0.1:8080/v1/health >/dev/null || fail "API 未就绪"

log "3) API 认证：无 token / 错误 token 必须 401（P1-1）"
CODE=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/v1/machines)
[[ "$CODE" == "401" ]] || fail "无 token 返回 $CODE，期望 401"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer wrong-token" http://127.0.0.1:8080/v1/machines)
[[ "$CODE" == "401" ]] || fail "错误 token 返回 $CODE，期望 401"
log "    401 OK"

log "4) agent 身份隔离（P1-2）"
if env -u FIREPAAS_AGENT_TLS_CERT -u FIREPAAS_AGENT_TLS_KEY -u FIREPAAS_AGENT_TLS_CA \
  "$LAB_BIN/agentctl" info >/dev/null 2>&1; then
  fail "明文 agentctl 居然成功，mTLS 未生效"
fi
log "    plain rejected OK"
# edge 证书（CN=edge-proxy）不得能调 agent gRPC（5108 只接受 control-plane）。
if env FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/edge.crt" \
       FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/edge.key" \
       FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/agentctl" info >/dev/null 2>&1; then
  fail "edge 身份不应能访问 agent gRPC（5108）"
fi
log "    edge identity on gRPC rejected OK"
# control-plane 证书（CN=control-plane）不得能过 agent proxy（5107 只接受 edge）。
CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  --cert "$CERT_DIR/control-plane.crt" --key "$CERT_DIR/control-plane.key" \
  --cacert "$CERT_DIR/ca.crt" https://127.0.0.1:5107/ || true)
[[ "$CODE" == "403" ]] || fail "control-plane 身份过 proxy 返回 $CODE，期望 403"
log "    control-plane identity on proxy rejected(403) OK"

log "5) 创建 machine"
CREATE_BODY="{\"app_id\":\"$RUN_ID\",\"hostname\":\"$HOSTNAME\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"$OP_CREATE\"}"
authed_curl -X POST http://127.0.0.1:8080/v1/machines \
  -H 'Content-Type: application/json' -d "$CREATE_BODY" >"$RUN_DIR/create.json"
log "    $(cat "$RUN_DIR/create.json")"

log "6) 幂等（验收 1）：同 operation_id 重试 100 次只产生一个副本；异 body 拒绝"
for i in $(seq 1 100); do
  authed_curl -X POST http://127.0.0.1:8080/v1/machines \
    -H 'Content-Type: application/json' -d "$CREATE_BODY" >/dev/null \
    || fail "第 $i 次幂等重试失败"
  # 默认 mutation bucket 是 20 req/s、burst 40；验收幂等语义时不得
  # 偶然把 API 限流也打满（限流行为由 e2e-v12 单独覆盖）。
  sleep 0.1
done
COUNT=$(authed_curl http://127.0.0.1:8080/v1/machines \
  | python3 -c 'import json,sys
ms=json.load(sys.stdin)["machines"] or []
print(sum(1 for m in ms if m["ID"]=="'"$MACHINE_ID"'"))')
[[ "$COUNT" == "1" ]] || fail "重复提交后 machine 数 = $COUNT，期望 1"
log "    100 retries → 1 machine OK"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $API_TOKEN" \
  -X POST http://127.0.0.1:8080/v1/machines -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$RUN_ID\",\"hostname\":\"$HOSTNAME\",\"image\":\"docker.io/library/nginx:1.27-alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"$OP_CREATE\"}")
[[ "$CODE" == "409" ]] || fail "同 op 异 body 返回 $CODE，期望 409（P2-8）"
log "    conflicting body rejected(409) OK"

log "7) 等待 observed RUNNING + PG route 权威落库（P2-6）"
ST=""
for _ in $(seq 1 60); do
  ST=$(authed_curl http://127.0.0.1:8080/v1/machines \
    | python3 -c 'import json,sys
ms=json.load(sys.stdin)["machines"] or []
print(next((m["ObservedState"] for m in ms if m["ID"]=="'"$MACHINE_ID"'"),""))')
  [[ "$ST" == "RUNNING" ]] && break
  sleep 3
done
[[ "$ST" == "RUNNING" ]] || fail "machine 未 RUNNING（$ST）"
for _ in $(seq 1 20); do
  PGB=$(docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc \
    "SELECT count(*) FROM route_backends rb JOIN routes r ON r.id=rb.route_id WHERE r.hostname='$HOSTNAME'" 2>/dev/null || echo 0)
  [[ "$PGB" == "1" ]] && break
  sleep 2
done
[[ "$PGB" == "1" ]] || fail "PG route_backends 未落库（count=$PGB）"
log "    observed RUNNING + PG route_backends OK"

log "8) hostname 路由返回 HTTP 200"
BODY=$(curl -fsS -H "Host: $HOSTNAME" http://127.0.0.1:8081/)
echo "$BODY" | grep -qi 'Welcome to nginx' || fail "edge 返回的不是 nginx 页面"
log "    HTTP 200 + nginx page OK"

log "9) agent 重启后 ledger 重放（验收 1：agent 重启后仍返回原结果）"
restart_agentd || fail "agentd 重启失败"
log "    agentd restarted OK"
# API 与 agent 共用该 HMAC 规则派生 execution-bound credential。调试客户端
# 必须重放完整原请求，否则验证的是凭证缺失而不是 ledger 幂等。
PROXY_CREDENTIAL="$(python3 - "$TRAFFIC_KEY" "$MACHINE_ID" <<'PY'
import hashlib, hmac, sys
print(hmac.new(sys.argv[1].encode(), f"{sys.argv[2]}/exec-1".encode(), hashlib.sha256).hexdigest()[:32])
PY
)"
# 同 operation_id + 完全一致 spec：必须命中 ledger 重放，返回已记录结果。
# 注意：首创建 body 的 app_id 是 $RUN_ID（非默认 app-$HOSTNAME），重放必须
# 与首次请求逐字段一致，否则 request hash 冲突（验收 AgentLedger 的正确拒
# 绝，而不是幂等重放）。
"$LAB_BIN/agentctl" create -machine-id "$MACHINE_ID" -operation "$OP_CREATE" \
  -image docker.io/library/nginx:alpine -vcpus 1 -mem-mib 512 \
  -project dev -app "$RUN_ID" -deployment "dep-$HOSTNAME" -execution exec-1 \
  -generation 1 -hostname "$HOSTNAME" -port 80 -proxy-credential "$PROXY_CREDENTIAL" >"$RUN_DIR/replay.json" \
  || fail "agent 重启后 ledger 重放失败"
grep -Eq "\"machineId\": *\"$MACHINE_ID\"" "$RUN_DIR/replay.json" \
  || fail "重放结果不含 machine：$(cat "$RUN_DIR/replay.json")"
log "    ledger replay after restart OK"
# 同 operation_id + 不同 spec（request hash 冲突）：必须被拒绝。
if "$LAB_BIN/agentctl" create -machine-id "$MACHINE_ID" -operation "$OP_CREATE" \
  -image docker.io/library/nginx:alpine -vcpus 2 -mem-mib 512 \
  -project dev -app "app-$HOSTNAME" -deployment "dep-$HOSTNAME" -execution exec-1 \
  -generation 1 -hostname "$HOSTNAME" -port 80 -proxy-credential "$PROXY_CREDENTIAL" >/dev/null 2>&1; then
  fail "同 op 异 hash 应被 agent ledger 拒绝"
fi
log "    hash conflict rejected OK"

log "10) generation fencing + secret 不泄漏（P0-2 / P0-1）"
FENCE_ID="fence-$RUN_ID"
FENCE_SECRET="fence-secret-$RUN_ID"
# gen=2 创建成功（携带 secret）；响应/列表/持久化 ledger 三处均不得出现 secret 值。
"$LAB_BIN/agentctl" create -machine-id "$FENCE_ID" -operation "op-$RUN_ID-fence-create" \
  -image docker.io/library/nginx:alpine -vcpus 1 -mem-mib 512 \
  -project dev -app "app-$FENCE_ID" -deployment "dep-$FENCE_ID" -execution fence-exec-1 \
  -generation 2 -secret-lease-id "lease-$RUN_ID-fence" -proxy-credential "$(python3 - "$TRAFFIC_KEY" "$FENCE_ID" <<'PY'
import hashlib, hmac, sys
print(hmac.new(sys.argv[1].encode(), f"{sys.argv[2]}/fence-exec-1".encode(), hashlib.sha256).hexdigest()[:32])
PY
)" -secret "API_KEY=$FENCE_SECRET" >"$RUN_DIR/fence-create.json" \
  || fail "fence 前置 create(gen=2) 失败"
if grep -q "$FENCE_SECRET" "$RUN_DIR/fence-create.json"; then
  fail "secret 值泄漏到 CreateMachineResponse"
fi
if "$LAB_BIN/agentctl" list | grep -q "$FENCE_SECRET"; then
  fail "secret 值泄漏到 ListMachines"
fi
if grep -q "$FENCE_SECRET" /var/lib/firepaas-p0/hypeman/agent/ledger.json; then
  fail "secret 值泄漏到持久化 ledger"
fi
log "    secret not in response/list/ledger OK"
# 旧 generation delete 被拒绝。
if "$LAB_BIN/agentctl" delete -machine-id "$FENCE_ID" -execution fence-exec-1 \
  -generation 1 -operation "op-$RUN_ID-fence-del-stale" >/dev/null 2>&1; then
  fail "旧 generation delete 应被拒绝"
fi
# 删除后旧 generation re-create 被拒绝。
if "$LAB_BIN/agentctl" create -machine-id "$FENCE_ID" -operation "op-$RUN_ID-fence-recreate-stale" \
  -image docker.io/library/nginx:alpine -vcpus 1 -mem-mib 512 \
  -project dev -app "app-$FENCE_ID" -deployment "dep-$FENCE_ID" -execution fence-exec-1 \
  -generation 1 -proxy-credential "stale-credential" >/dev/null 2>&1; then
  fail "删除后旧 generation re-create 应被拒绝"
fi
# 同代 delete 成功，清理 fence 测试 machine。
"$LAB_BIN/agentctl" delete -machine-id "$FENCE_ID" -execution fence-exec-1 \
  -generation 2 -operation "op-$RUN_ID-fence-del-ok" >/dev/null \
  || fail "fence 清理 delete(gen=2) 失败"
log "    stale delete / stale recreate rejected OK"

log "11) 删除 machine 并确认 route 投影清理（Redis + PG）"
authed_curl -X DELETE "http://127.0.0.1:8080/v1/machines/$MACHINE_ID?operation_id=$OP_DELETE" >/dev/null
for _ in $(seq 1 30); do
  KEYS=$(docker exec dev-redis-1 redis-cli keys "route:$HOSTNAME:*" 2>/dev/null || true)
  HKEYS=$(docker exec dev-redis-1 redis-cli keys "hostidx:$HOSTNAME" 2>/dev/null || true)
  [[ -z "$KEYS" && -z "$HKEYS" ]] && break
  sleep 2
done
[[ -z "${KEYS:-}" ]] || fail "route 投影未清理：$KEYS"
[[ -z "${HKEYS:-}" ]] || fail "hostidx 投影未清理：$HKEYS"
PGR=$(docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc \
  "SELECT count(*) FROM routes WHERE hostname='$HOSTNAME'" 2>/dev/null || echo 1)
[[ "$PGR" == "0" ]] || fail "PG routes 未清理（count=$PGR）"
log "    redis + PG route cleanup OK"

log "M1 e2e PASS：API(auth) → PG → agent(mTLS+身份+fencing) → Redis → edge(TLS) → VM → HTTP 200"
log "    附加覆盖：幂等 100x / 409 冲突 / 403 越权 / agent 重启重放 / PG route 权威"
log "    P0：stale generation 变更拒绝 / secret 不入响应、列表与 ledger"
