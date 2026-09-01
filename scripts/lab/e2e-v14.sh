#!/usr/bin/env bash
# v1.4 单机 smoke（docs/v1.4-plan.md §5/§7/§8）：
#   A) capability 真值：inventory.local.v1 已发布、volume.dataset_overlay.v1
#      未发布（未验收的 CoW 不得广告）；
#   B) prewarm/coverage/pin：digest-pinned prewarm 派发收敛、coverage 反映
#      cached、pin 创建/过期保护/删除、未观测大小的 digest 拒绝 pin；
#   C) restore preflight：只读结论与 memory/auto 语义一致（memory 不兼容
#      明确拒绝、auto 只报告降级）；
#   D) snapshot delete 幂等：DELETING 中重试 202 而非 409，最终收敛 DELETED；
#   E) DATASET_RO URL 加固：userinfo/query/fragment 拒绝（400）；
#   F) integrity observation：READY 快照经 inventory 对账变 METADATA_VERIFIED，
#      DTO 暴露 integrity 字段。
# 明确不覆盖：双节点 leader handover、磁盘水位、quarantine/finalize、scrub——
# 见 docs/v1.4-plan.md §10 版本级验收（需多节点/破坏性注入）。本 smoke 还验证
# ADR-0036 inventory epoch 持久化与默认 local GC=off，以及 ADR-0037 operator-only 门禁。
# 用法: sudo bash scripts/lab/e2e-v14.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-v14"
RUN_ID="v14-$(date +%s)"
API_TOKEN="v14-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT=8095
EDGE_HTTP=8096
EDGE_TLS=8496
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-v14 $(now)] $*"; }
fail() { echo "[e2e-v14] FAIL: $*" >&2; exit 1; }
blocked() { echo "[e2e-v14] BLOCKED/UNSUPPORTED: $*" >&2; exit 2; }
authed_curl() { curl -fsS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }
authed_raw() { curl -sS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }
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
  FIREPAAS_LOCAL_GC_MODE=off \
  FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/v14-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/v14-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/v14-api.log"; fail "API 未就绪"; }
mark "api/edge up"

ONLINE_OUT=$(bash "$HERE/push-ontime.sh") || fail "push-ontime 失败"
ONTIME_REF=$(echo "$ONLINE_OUT" | grep '^REF=' | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || fail "ontime REF 解析失败"
DIGEST=$(echo "$ONTIME_REF" | sed 's/.*@//')

log "0.5) 预清理历史验收机"
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now()" >/dev/null
pg "UPDATE deployments SET status='SUPERSEDED', updated_at=now() WHERE status IN ('ACTIVE','PREPARING')" >/dev/null
pg "UPDATE rollouts SET status='COMPLETE', failed=true, completed_at=now(), updated_at=now()
	WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK')" >/dev/null
sleep 3

log "1) A：capability 真值（inventory 已发布；overlay 未发布）"
INV_CAP=0
for _ in $(seq 1 20); do
  INV_CAP=$(pg "SELECT count(*) FROM nodes WHERE status='HEALTHY' AND feature_ids::text LIKE '%inventory.local.v1%'")
  [[ "${INV_CAP:-0}" -ge 1 ]] && break
  sleep 2
done
[[ "${INV_CAP:-0}" -ge 1 ]] || fail "健康节点未发布 inventory.local.v1"
OVERLAY_CAP=$(pg "SELECT count(*) FROM nodes WHERE feature_ids::text LIKE '%volume.dataset_overlay.v1%'")
[[ "${OVERLAY_CAP:-0}" -eq 0 ]] || fail "未验收的 dataset overlay 能力被发布（fail-closed 违约）"
# ADR-0036：accepted observation 必须带 epoch/generation。启动命令故意不设置
# FIREPAAS_LOCAL_GC_MODE，以检查默认 off 不产生 claim。
OBS_OK=0
for _ in $(seq 1 20); do
  obs=$(pg "SELECT count(*) FROM local_inventory_observations WHERE epoch<>'' AND generation>0")
  [[ "${obs:-0}" -ge 2 ]] && OBS_OK=1 && break
  sleep 2
done
[[ "$OBS_OK" == "1" ]] || fail "未持久化带 epoch/generation 的 authoritative inventory observation"
claims_before=$(pg "SELECT count(*) FROM local_gc_claims")
sleep 2
claims_after=$(pg "SELECT count(*) FROM local_gc_claims")
[[ "$claims_after" == "$claims_before" ]] || fail "FIREPAAS_LOCAL_GC_MODE=off 时产生了 local GC claim"
mark "capability + inventory epoch + local GC off OK"

log "2) B：operator-only prewarm → coverage → pin"
# ADR-0037：普通 project key 不得操作 prewarm/coverage/pin 或读取节点拓扑。
PROJECT_KEY=$(authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/apikeys" \
  -H 'Content-Type: application/json' \
  -d '{"name":"v14-project-'$RUN_ID'","scopes":["read","write"],"project_id":"dev"}' | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["key"])')
[[ -n "$PROJECT_KEY" ]] || fail "project API key 创建失败"
for request in \
  "GET|/v1/images/coverage?image_ref=$ONTIME_REF|" \
  "GET|/v1/images/pins|" \
  "POST|/v1/images/prewarm|{\"project_id\":\"dev\",\"image_ref\":\"$ONTIME_REF\",\"node_pool\":\"compute\"}"; do
  IFS='|' read -r method path body <<<"$request"
  args=(-o /dev/null -w '%{http_code}' -X "$method" -H "Authorization: Bearer $PROJECT_KEY")
  [[ -z "$body" ]] || args+=(-H 'Content-Type: application/json' -d "$body")
  code=$(curl -sS -m 30 "${args[@]}" "http://127.0.0.1:$API_PORT$path")
  [[ "$code" == "403" ]] || fail "project key 访问 $method $path 应 403，got $code"
done
mark "operator-only 门禁 OK"

# root/admin mutation 的 Idempotency-Key：同 key/同请求返回同一 operation。
PREWARM_IDEM="v14-prewarm-$RUN_ID"
log "2.1) root/admin prewarm → coverage → pin"
prewarm_file="$RUN_DIR/prewarm.json"
prewarm_code=$(authed_raw -o "$prewarm_file" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/images/prewarm" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $PREWARM_IDEM" \
  -d "{\"project_id\":\"dev\",\"image_ref\":\"$ONTIME_REF\",\"node_pool\":\"compute\"}")
[[ "$prewarm_code" == "202" ]] || fail "prewarm 期望 202，got $prewarm_code body=$(tr '\n' ' ' <"$prewarm_file")"
PREWARM_OP=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operation_id"])' "$prewarm_file")
[[ -n "$PREWARM_OP" ]] || fail "prewarm 未返回 operation_id"
replay_file="$RUN_DIR/prewarm-replay.json"
replay_code=$(authed_raw -o "$replay_file" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/images/prewarm" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $PREWARM_IDEM" \
  -d "{\"project_id\":\"dev\",\"image_ref\":\"$ONTIME_REF\",\"node_pool\":\"compute\"}")
[[ "$replay_code" == "202" ]] || fail "prewarm 幂等重放期望 202，got $replay_code"
REPLAY_OP=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["operation_id"])' "$replay_file")
[[ "$REPLAY_OP" == "$PREWARM_OP" ]] || fail "相同 Idempotency-Key 未返回原 operation"
PREWARM_DONE=0
for _ in $(seq 1 60); do
  op=$(pg "SELECT status FROM operations WHERE id='$PREWARM_OP'")
  [[ "$op" == "SUCCEEDED" ]] && PREWARM_DONE=1 && break
  [[ "$op" == "FAILED" ]] && fail "prewarm operation FAILED: $(pg "SELECT coalesce(error,'') FROM operations WHERE id='$PREWARM_OP'")"
  sleep 3
done
[[ "$PREWARM_DONE" == "1" ]] || fail "prewarm 未收敛（op status=$op）"
targets=$(pg "SELECT count(*) FROM image_prewarm_targets WHERE operation_id='$PREWARM_OP' AND status='SUCCEEDED'")
[[ "${targets:-0}" -ge 1 ]] || fail "prewarm 逐节点结果缺失"
size_seen=$(pg "SELECT count(*) FROM image_sizes WHERE digest='$DIGEST'")
[[ "${size_seen:-0}" -eq 1 ]] || fail "prewarm 未记录观测大小（image_sizes）"
mark "prewarm 收敛 OK（op=$PREWARM_OP targets=$targets）"

# coverage：cached ≥ 1，且节点观测时间存在（image_cache 投影 20s 同步节奏，轮询等待）。
cov_file="$RUN_DIR/coverage.json"
COV_OK=0
for _ in $(seq 1 20); do
  cov_code=$(authed_raw -o "$cov_file" -w '%{http_code}' "http://127.0.0.1:$API_PORT/v1/images/coverage?image_ref=$ONTIME_REF")
  [[ "$cov_code" == "200" ]] || fail "coverage 期望 200，got $cov_code"
  if COV_FILE="$cov_file" python3 - <<'PY' 2>/dev/null
import json, os
cov = json.load(open(os.environ["COV_FILE"]))
summary = cov["summary"]
assert summary["eligible"] >= 1, cov
assert summary["cached"] >= 1, cov
assert summary["uncached"] == 0, cov
nodes = cov["nodes"]
assert any(n["cached"] and n["last_observed"] for n in nodes), cov
PY
  then COV_OK=1; break; fi
  sleep 3
done
[[ "$COV_OK" == "1" ]] || { cat "$cov_file"; fail "coverage 结构/缓存状态不符合 v1.4 契约"; }
mark "coverage OK（cached=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["summary"]["cached"])' "$cov_file")）"

# 未观测大小的 digest → pin 拒绝（fail closed）。
fake_digest="sha256:$(openssl rand -hex 32)"
pin_code=$(authed_raw -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/images/pins" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: pin-unknown-$RUN_ID" \
  -d "{\"project_id\":\"dev\",\"image_digest\":\"$fake_digest\",\"node_pool\":\"compute\",\"ttl_seconds\":600}")
[[ "$pin_code" == "409" ]] || fail "未观测大小的 digest pin 应 409，got $pin_code"

# 正常 pin：创建 → 列表 → 删除。
pin_file="$RUN_DIR/pin.json"
PIN_IDEM="pin-$RUN_ID"
pin_code=$(authed_raw -o "$pin_file" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/images/pins" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $PIN_IDEM" \
  -d "{\"project_id\":\"dev\",\"image_ref\":\"$ONTIME_REF\",\"node_pool\":\"compute\",\"ttl_seconds\":600,\"reason\":\"e2e\"}")
[[ "$pin_code" == "201" ]] || fail "pin 创建期望 201，got $pin_code body=$(tr '\n' ' ' <"$pin_file")"
PIN_ID=$(python3 -c 'import json,sys; p=json.load(open(sys.argv[1]))["pins"][0]; print(p.get("id", p.get("ID", "")))' "$pin_file")
pins_list=$(authed_raw "http://127.0.0.1:$API_PORT/v1/images/pins")
echo "$pins_list" | grep -q "$PIN_ID" || fail "pin 列表缺新建 pin"
# TTL 上限拒绝。
ttl_code=$(authed_raw -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/images/pins" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: pin-ttl-$RUN_ID" \
  -d "{\"project_id\":\"dev\",\"image_ref\":\"$ONTIME_REF\",\"node_pool\":\"compute\",\"ttl_seconds\":9999999}")
[[ "$ttl_code" == "400" ]] || fail "超 TTL pin 应 400，got $ttl_code"
# pin 是节点作用域 GC root（dry-run GC 不删除）。
gc_pin_root=$(pg "SELECT count(*) FROM image_pins WHERE id='$PIN_ID' AND expires_at > now()")
[[ "${gc_pin_root:-0}" -eq 1 ]] || fail "pin 行缺失"
del_code=$(authed_raw -o /dev/null -w '%{http_code}' -X DELETE \
  -H "Idempotency-Key: unpin-$RUN_ID" "http://127.0.0.1:$API_PORT/v1/images/pins/$PIN_ID")
[[ "$del_code" == "200" ]] || fail "pin 删除期望 200，got $del_code"
mark "pin 生命周期 OK（未观测大小拒绝、TTL 上限、创建/删除）"

log "3) C+F：checkpoint → preflight + integrity"
app="v14-app-$RUN_ID"
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

snap_file="$RUN_DIR/create-snapshot.json"
snap_code=$(authed_raw -o "$snap_file" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/snapshots" \
  -H 'Content-Type: application/json' -d '{"kind":"MEMORY","name":"ckpt-v14"}')
if [[ "$snap_code" == "501" ]]; then
  blocked "snapshot create 返回 501：$(tr '\n' ' ' <"$snap_file")"
fi
[[ "$snap_code" == "202" ]] || fail "snapshot create 期望 202，got $snap_code"
SNAP_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["snapshot_id"])' "$snap_file")
SNAP_READY=0
for _ in $(seq 1 60); do
  st=$(pg "SELECT status FROM snapshots WHERE id='$SNAP_ID'")
  [[ "$st" == "READY" ]] && SNAP_READY=1 && break
  op_status=$(pg "SELECT coalesce(status,'') FROM operations WHERE id='op-snap-$SNAP_ID'")
  [[ "$op_status" == "FAILED" ]] && fail "snapshot create FAILED: $(pg "SELECT coalesce(error,'') FROM operations WHERE id='op-snap-$SNAP_ID'")"
  sleep 3
done
[[ "$SNAP_READY" == "1" ]] || fail "snapshot 未 READY（status=$st）"
mark "checkpoint READY（$SNAP_ID）"

# F：inventory 对账后 integrity → METADATA_VERIFIED（节点 HEALTHY + complete 列表）。
INTEG_OK=0
for _ in $(seq 1 30); do
  integ=$(pg "SELECT integrity FROM snapshots WHERE id='$SNAP_ID'")
  [[ "$integ" == "METADATA_VERIFIED" || "$integ" == "CONTENT_VERIFIED" ]] && INTEG_OK=1 && break
  sleep 3
done
[[ "$INTEG_OK" == "1" ]] || fail "snapshot integrity 未收敛（integrity=$integ）"
snap_detail=$(authed_raw "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID")
SNAP_DETAIL="$snap_detail" python3 - <<'PY' || fail "snapshot DTO 缺 integrity 字段"
import json, os
s = json.loads(os.environ["SNAP_DETAIL"])["snapshot"]
assert s.get("integrity") in ("METADATA_VERIFIED", "CONTENT_VERIFIED"), s
PY
mark "integrity 对账 OK（METADATA_VERIFIED）"

# C：preflight（memory 兼容 → resolved memory；无 blocking）。
pf_file="$RUN_DIR/preflight.json"
pf_code=$(authed_raw -o "$pf_file" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID/preflight" \
  -H 'Content-Type: application/json' -d '{"restore_mode":"memory"}')
[[ "$pf_code" == "200" ]] || fail "preflight 期望 200，got $pf_code body=$(tr '\n' ' ' <"$pf_file")"
PF_FILE="$pf_file" python3 - <<'PY' || fail "preflight memory 结论不正确"
import json, os
pf = json.load(open(os.environ["PF_FILE"]))
assert pf["memory_compatible"] is True, pf
assert pf["resolved_mode"] == "memory", pf
assert pf["origin_node_available"] is True, pf
assert pf["blocking_issues"] == [], pf
assert pf["locality"] == "node-local" and pf["durability"] == "best-effort", pf
assert "source_key" in pf["compatibility"] and pf["compatibility"]["structured"]["keys_match"], pf
assert "memory" in pf["available_modes"], pf
PY
# auto：同源兼容 → resolved memory；不执行任何隐式操作（snapshot 状态不变）。
authed_raw -o "$pf_file" -X POST "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID/preflight" \
  -H 'Content-Type: application/json' -d '{"restore_mode":"auto"}' >/dev/null
PF_FILE="$pf_file" python3 - <<'PY' || fail "preflight auto 结论不正确"
import json, os
pf = json.load(open(os.environ["PF_FILE"]))
assert pf["resolved_mode"] == "memory" and pf["degradable_to_filesystem"] is False, pf
PY
st_after_pf=$(pg "SELECT status FROM snapshots WHERE id='$SNAP_ID'")
[[ "$st_after_pf" == "READY" ]] || fail "preflight 必须只读（status=$st_after_pf）"
mark "preflight OK（memory/auto 与实际决策一致，只读）"

log "4) D：snapshot delete 幂等收敛"
# 阻止删除快速完成：先持有一个引用（fork），验证 409/重试语义后再删。
first_code=$(authed_raw -o "$RUN_DIR/del1.json" -w '%{http_code}' -X DELETE "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID")
[[ "$first_code" == "202" ]] || fail "第一次删除期望 202，got $first_code"
# 立即重试：DELETING 中必须幂等 202（v1.4-A），不得 409。
second_code=$(authed_raw -o "$RUN_DIR/del2.json" -w '%{http_code}' -X DELETE "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID")
[[ "$second_code" == "202" ]] || fail "DELETING 中重试删除期望 202（幂等），got $second_code body=$(tr '\n' ' ' <"$RUN_DIR/del2.json")"
DELETED=0
for _ in $(seq 1 30); do
  st=$(pg "SELECT status FROM snapshots WHERE id='$SNAP_ID'")
  [[ "$st" == "DELETED" ]] && DELETED=1 && break
  sleep 2
done
[[ "$DELETED" == "1" ]] || fail "snapshot 未 DELETED（status=$st）"
third_code=$(authed_raw -o /dev/null -w '%{http_code}' -X DELETE "http://127.0.0.1:$API_PORT/v1/snapshots/$SNAP_ID")
[[ "$third_code" == "202" ]] || fail "已删除重试期望 202 already_deleted，got $third_code"
mark "delete 幂等收敛 OK（202/202/DELETED/202）"

log "5) E：DATASET_RO 来源 URL 加固"
for url in "https://user:pass@objects.example/d.tar.gz" "https://objects.example/d.tar.gz?X-Amz-Signature=abc" "https://objects.example/d.tar.gz#frag"; do
  code=$(authed_raw -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/volumes" \
    -H 'Content-Type: application/json' \
    -d "{\"project_id\":\"dev\",\"name\":\"v14-bad\",\"mode\":\"DATASET_RO\",\"size_gib\":1,\"source_url\":\"$url\",\"content_digest\":\"$fake_digest\"}")
  [[ "$code" == "400" ]] || fail "带凭证/查询/片段的来源 URL 应 400，got $code（$url）"
done
# URL 原文不进 operations（规范化请求只保存规范化 URL + 摘要；source_url 出入参走 redact）。
mark "DATASET_RO URL 加固 OK（userinfo/query/fragment 拒绝）"

log "6) 清理 + 结论"
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE app_id='$app'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now() WHERE id='$app'" >/dev/null
log "ALL PASS"
