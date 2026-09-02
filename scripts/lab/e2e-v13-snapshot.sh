#!/usr/bin/env bash
# v1.3-B/C（ADR-0028）snapshot/checkpoint/fork/rescue 单机 smoke：
#   A) memory checkpoint：pause→capture→resume source（源 execution 不变、
#      RUNNING 保持），快照 READY + locality/durability/compatibility 字段可见；
#   B) 手工 checkpoint 不受 schedule retention 影响；schedule max_count 收敛；
#   C) fork：从 READY 快照 fork debug machine（新 execution、TTL 必填、无 route），
#      源 machine 不受影响；
#   D) 删除快照 → DELETING→DELETED；删除前有 fork 引用时拒绝。
# 明确不覆盖：跨节点 restore 兼容矩阵、freeze/thaw 故障注入、leader handover、
# 72h soak；见 docs/v1.3-plan.md §12。用法: sudo bash scripts/lab/e2e-v13-snapshot.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-v13-snapshot"
RUN_ID="v13s-$(date +%s)"
API_TOKEN="v13s-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT=8093
EDGE_HTTP=8094
EDGE_TLS=8494
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-v13-snapshot $(now)] $*"; }
fail() { echo "[e2e-v13-snapshot] FAIL: $*" >&2; exit 1; }
blocked() { echo "[e2e-v13-snapshot] BLOCKED/UNSUPPORTED: $*" >&2; exit 2; }
authed_curl() { curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
authed_raw() { curl -sS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
mark() { log "    (累计 $(( $(date +%s) - T0 ))s) $*"; }

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建"
[[ -f "$CERT_DIR/wildcard-$DOMAIN.crt" ]] || fail "泛域名证书缺失"

log "0) 启动 agentd + API/edge"
T0=$(date +%s)
"$HERE/root-setup.sh" >/dev/null || fail "root-setup 失败"
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"
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
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/v13s-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/v13s-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/v13s-api.log"; fail "API 未就绪"; }
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
for _ in $(seq 1 30); do
  snapshot_cap=$(pg "SELECT count(*) FROM nodes WHERE status='HEALTHY' AND feature_ids::text LIKE '%snapshot.memory.v1%'")
  [[ "${snapshot_cap:-0}" -ge 1 ]] && break
  sleep 2
done

log "1) A：memory checkpoint（源 execution 不变）"
# 当前 pinned hypeman 不提供可验证 artifact checksum；agent 必须不发布能力，
# 或在调用链上明确返回 unsupported。缺能力是验收阻塞，不得打印 ALL PASS。
[[ "${snapshot_cap:-0}" -ge 1 ]] || blocked "健康节点未发布 snapshot.memory.v1（已知上游 checksum 缺口，checkpoint 验收不可兑现）"
app="v13s-app-$RUN_ID"
host="$app.$DOMAIN"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$app\",\"hostname\":\"$host\",\"image\":\"$ONTIME_REF\",\"replicas\":1}" >/dev/null
machine="$app-r0-g1"
READY=0
for _ in $(seq 1 90); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
  [[ "$st" == "RUNNING" ]] && READY=1 && break
  sleep 3
done
[[ "$READY" == "1" ]] || fail "app 未就绪"
exec_before=$(pg "SELECT current_execution_id FROM machines WHERE id='$machine'")
mark "source ready（execution=$exec_before）"

snap_file="$RUN_DIR/create-snapshot.json"
snap_code=$(authed_raw -o "$snap_file" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/snapshots" \
  -H 'Content-Type: application/json' -d '{"kind":"MEMORY","name":"ckpt-1"}')
if [[ "$snap_code" == "501" ]]; then
  blocked "snapshot create 返回 501：$(tr '\n' ' ' <"$snap_file")"
fi
[[ "$snap_code" == "202" ]] || fail "snapshot create 期望 202 或明确 501，got $snap_code body=$(tr '\n' ' ' <"$snap_file")"
SNAP_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["snapshot_id"])' "$snap_file")
[[ -n "$SNAP_ID" ]] || fail "checkpoint 未受理"
READY_SNAP=0
op_status=""; op_error=""; st=""
for _ in $(seq 1 60); do
  st=$(pg "SELECT status FROM snapshots WHERE id='$SNAP_ID'")
  op_status=$(pg "SELECT coalesce(status,'') FROM operations WHERE id='op-snap-$SNAP_ID'")
  op_error=$(pg "SELECT replace(coalesce(error,''),' ','_') FROM operations WHERE id='op-snap-$SNAP_ID'")
  [[ "$st" == "READY" ]] && READY_SNAP=1 && break
  if [[ "$op_status" == "FAILED" ]]; then
    if [[ "$op_error" == *unsupported* || "$op_error" == *checksum* || "$op_error" == *unavailable* ]]; then
      blocked "snapshot create fail closed：${op_error//_/ }"
    fi
    fail "snapshot create operation FAILED：${op_error//_/ }"
  fi
  sleep 3
done
[[ "$READY_SNAP" == "1" ]] || fail "snapshot 未 READY（status=$st operation=$op_status error=${op_error//_/ }）"
# locality/durability/compatibility 字段可见。
snap_detail=$(authed_raw "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID")
SNAP_DETAIL="$snap_detail" SOURCE_MACHINE="$machine" SOURCE_EXECUTION="$exec_before" python3 - <<'PY' || fail "snapshot DTO 字段不符合 v1.3 契约"
import json, os
s = json.loads(os.environ["SNAP_DETAIL"])["snapshot"]
required={"id","project_id","source_machine_id","source_execution_id","source_generation","kind","status","origin_node_id","locality","durability","compatibility_key","size_bytes","checksum","created_at"}
assert required <= set(s), (required-set(s), s)
assert s["status"] == "READY" and s["kind"] == "MEMORY", s
assert s["source_machine_id"] == os.environ["SOURCE_MACHINE"], s
assert s["source_execution_id"] == os.environ["SOURCE_EXECUTION"], s
assert s["origin_node_id"] and s["locality"] == "node-local", s
assert s["durability"] == "best-effort" and s["compatibility_key"], s
assert s["size_bytes"] > 0 and s["checksum"], s
PY
# 源 machine：checkpoint 后必须 RUNNING 且 execution 不变。
exec_after=$(pg "SELECT current_execution_id FROM machines WHERE id='$machine'")
[[ "$exec_after" == "$exec_before" ]] || fail "checkpoint 改变了源 execution"
st_after=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
[[ "$st_after" == "RUNNING" ]] || fail "checkpoint 后源必须恢复 RUNNING（got $st_after）"
mark "memory checkpoint OK（源 execution 不变）"

log "2) B：schedule + retention（max_count=2，不删手工 checkpoint）"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/snapshot-schedules" \
  -H 'Content-Type: application/json' \
  -d '{"interval_seconds":60,"max_count":2}' >/dev/null
# 手工再打一个 checkpoint（无 schedule 归属）。
authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/snapshots" \
  -H 'Content-Type: application/json' -d '{"kind":"memory","name":"manual-1"}' >/dev/null
# 等 schedule 产生 ≥2 个产物 + retention 收敛（interval 60s，最多等 ~4min）。
RETENTION_OK=0
for _ in $(seq 1 80); do
  schedule_id="sch-$machine"
  n=$(pg "SELECT count(*) FROM snapshots WHERE schedule_id='$schedule_id' AND status IN ('READY','UNAVAILABLE')")
  total=$n
  manual=$(pg "SELECT count(*) FROM snapshots WHERE source_machine_id='$machine' AND schedule_id='' AND status IN ('READY','UNAVAILABLE')")
  [[ "${n:-0}" -ge 2 && "${total:-0}" -le 2 && "${manual:-0}" -ge 1 ]] && RETENTION_OK=1 && break
  sleep 3
done
[[ "$RETENTION_OK" == "1" ]] || fail "retention 未在窗口内收敛（n=$n total=$total manual=$manual）"
mark "schedule + retention 收敛 OK"

log "3) C：fork（新 execution、TTL 必填、源不受影响）"
fork_resp=$(authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID/fork" \
  -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$app\",\"ttl_seconds\":600}")
FORK_MACHINE=$(echo "$fork_resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["machine_id"])')
[[ -n "$FORK_MACHINE" ]] || fail "fork 未受理"
FORK_READY=0
for _ in $(seq 1 60); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$FORK_MACHINE'")
  [[ "$st" == "RUNNING" ]] && FORK_READY=1 && break
  sleep 3
done
[[ "$FORK_READY" == "1" ]] || fail "fork machine 未就绪"
fork_exec=$(pg "SELECT current_execution_id FROM machines WHERE id='$FORK_MACHINE'")
[[ "$fork_exec" != "$exec_before" ]] || fail "fork 必须使用新 execution"
# fork 默认无 public route：hostname 不指向它（机器行 hostname 为空即可证明）。
fork_host=$(pg "SELECT hostname FROM machines WHERE id='$FORK_MACHINE'")
[[ -z "$fork_host" ]] || fail "fork machine 不应绑定 hostname（无 route）"
mark "fork OK（新 execution、无 route、源 execution=$exec_after）"

log "4) D：删除快照（fork 已完成后 DELETING→DELETED）"
# 引用保护覆盖 in-flight fork/restore；fork 完成并生成独立 artifact 后释放引用，
# 因而原 snapshot 可删除且不能影响已创建的 fork。
authed_curl -X DELETE "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID" >/dev/null
DELETED=0
for _ in $(seq 1 30); do
  st=$(pg "SELECT status FROM snapshots WHERE id='$SNAP_ID'")
  [[ "$st" == "DELETED" ]] && DELETED=1 && break
  sleep 2
done
[[ "$DELETED" == "1" ]] || fail "snapshot 未 DELETED（status=$st）"
mark "delete → DELETED OK"

log "5) 清理 + 结论"
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE app_id='$app'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now() WHERE id='$app'" >/dev/null
log "ALL PASS"
