#!/usr/bin/env bash
# M1 e2e harness（单机）：一键验证
#   authenticated API → PG operations → controller → agent mTLS → observed
#   → Redis route catalog → edge → agent proxy(TLS) → Firecracker VM → HTTP 200
# 用法: sudo bash scripts/lab/e2e-m1.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
LAB_BIN="/home/zty/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e"
RUN_ID="e2e-$(date +%s)"
HOSTNAME="$RUN_ID.local"
MACHINE_ID="$RUN_ID"

export PATH="$LAB_BIN:/home/zty/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"

mkdir -p "$RUN_DIR"

log() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

[[ -f "$LAB_BIN/agentd" ]] || fail "agentd 未构建"
[[ -f "$LAB_BIN/firepaas-api" ]] || fail "firepaas-api 未构建"
[[ -f "$LAB_BIN/edge-proxy" ]] || fail "edge-proxy 未构建"
[[ -f "$CERT_DIR/ca.crt" ]] || fail "证书未生成：bash scripts/lab/gen-certs.sh"

log "0) root setup + Nomad/agentd"
"$HERE/root-setup.sh" >/dev/null
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"

log "1) 启动 control-plane API（root）"
pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
nohup env \
  FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 \
  FIREPAAS_AGENT_ADDR=127.0.0.1:5108 \
  FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 \
  FIREPAAS_HTTP_PORT=8080 \
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

log "3) 无 mTLS 身份访问 agent gRPC 必须被拒绝"
if env -u FIREPAAS_AGENT_TLS_CERT -u FIREPAAS_AGENT_TLS_KEY -u FIREPAAS_AGENT_TLS_CA \
  "$LAB_BIN/agentctl" info >/dev/null 2>&1; then
  fail "明文 agentctl 居然成功，mTLS 未生效"
fi
log "    plain rejected OK"

log "4) 创建 machine"
OP_CREATE="op-$RUN_ID-create"
curl -fsS -X POST http://127.0.0.1:8080/v1/machines \
  -H 'Content-Type: application/json' \
  -d "{\"machine_id\":\"$MACHINE_ID\",\"hostname\":\"$HOSTNAME\",\"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"operation_id\":\"$OP_CREATE\"}" \
  >"$RUN_DIR/create.json"
log "    $(cat "$RUN_DIR/create.json")"

log "5) 等待 observed RUNNING"
for _ in $(seq 1 60); do
  ST=$(curl -fsS http://127.0.0.1:8080/v1/machines \
    | python3 -c 'import json,sys
ms=json.load(sys.stdin)["machines"]
print(next((m["ObservedState"] for m in ms if m["ID"]=="'"$MACHINE_ID"'"),""))')
  [[ "$ST" == "RUNNING" ]] && break
  sleep 3
done
[[ "${ST:-}" == "RUNNING" ]] || fail "machine 未 RUNNING（$ST）"

log "6) hostname 路由返回 HTTP 200"
BODY=$(curl -fsS -H "Host: $HOSTNAME" http://127.0.0.1:8081/)
echo "$BODY" | grep -qi 'Welcome to nginx' || fail "edge 返回的不是 nginx 页面"
log "    HTTP 200 + nginx page OK"

log "7) 删除 machine 并确认 route 投影清理"
OP_DELETE="op-$RUN_ID-delete"
curl -fsS -X DELETE "http://127.0.0.1:8080/v1/machines/$MACHINE_ID?operation_id=$OP_DELETE" >/dev/null
for _ in $(seq 1 30); do
  KEYS=$(docker exec dev-redis-1 redis-cli keys "route:$HOSTNAME:*" 2>/dev/null || true)
  [[ -z "$KEYS" ]] && break
  sleep 2
done
[[ -z "${KEYS:-}" ]] || fail "route 投影未清理：$KEYS"

log "M1 e2e PASS：API → PG → agent(mTLS) → Redis → edge(TLS) → VM → HTTP 200"
