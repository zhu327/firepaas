#!/usr/bin/env bash
# 启动 soak 专用 API/edge（M5 60min 排练用；与 e2e-m5 同拓扑，token 固定）。
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="/home/zty/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-m5"
mkdir -p "$RUN_DIR"

pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
pkill -f "$LAB_BIN/edge-proxy" 2>/dev/null || true
sleep 1

MASTER_KEY="$(openssl rand -base64 32)"
TRAFFIC_KEY="$(openssl rand -base64 32)"
export TOKEN="${FP_API_TOKEN:-soak-m5-token}"

nohup env \
  FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 \
  FIREPAAS_NOMAD_ADDR=http://127.0.0.1:4646 \
  FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 \
  FIREPAAS_HTTP_PORT=8083 \
  FIREPAAS_API_TOKEN="$TOKEN" \
  FIREPAAS_SECRETS_MASTER_KEY="$MASTER_KEY" \
  FIREPAAS_TRAFFIC_TOKEN_KEY="$TRAFFIC_KEY" \
  FIREPAAS_ROLLOUT_TIMEOUT=120s FIREPAAS_ROLLOUT_DRAIN=10s \
  FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" \
  FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/soak-api.log" 2>&1 &

nohup env \
  FIREPAAS_EDGE_PORT=8084 FIREPAAS_EDGE_TLS_LISTEN=":8445" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-firepaas.local.crt" \
  FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-firepaas.local.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" \
  FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 \
  FIREPAAS_API_ADDR="http://127.0.0.1:8083" \
  FIREPAAS_API_TOKEN="$TOKEN" \
  FIREPAAS_EDGE_RATE_LIMIT=100 FIREPAAS_EDGE_RATE_BURST=200 \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/soak-edge.log" 2>&1 &

for _ in $(seq 1 40); do
  curl -fsS -m 3 -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8083/v1/health >/dev/null 2>&1 && \
    echo "soak api/edge up (token=$TOKEN)" && exit 0
  sleep 1
done
tail -5 "$RUN_DIR/soak-api.log" >&2
exit 1
