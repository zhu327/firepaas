#!/usr/bin/env bash
# v1.3-D/E（ADR-0029/0030）node-local volume 验收：
#   A) 不指定 node 创建 LOCAL_RW，控制面选择健康 origin node；
#   B) guest 真实写读，agent restart 后数据仍在；
#   C) 两台 machine 并发竞争 attach，必须恰有一个成功；
#   D) origin node loss → UNAVAILABLE，不得在别处空建；恢复后原数据仍在；
#   E) DATASET_RO 恶意 archive 必须拒绝；CoW 未发布时明确阻塞而非 ALL PASS。
# 恶意 archive 默认在本机启动临时 fixture；API 仅在显式
# FIREPAAS_E2E_ALLOW_HTTP_LOOPBACK=1 时为 127.0.0.1 签发测试例外。
# 用法: sudo bash scripts/lab/e2e-v13-volume.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-v13-volume"
RUN_ID="v13v-$(date +%s)"
API_TOKEN="v13v-token-$RUN_ID"
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
log() { echo "[e2e-v13-volume $(now)] $*"; }
fail() { echo "[e2e-v13-volume] FAIL: $*" >&2; exit 1; }
blocked() { echo "[e2e-v13-volume] BLOCKED/UNSUPPORTED: $*" >&2; exit 2; }
authed_curl() { curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
authed_raw() { curl -sS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
mark() { log "    (累计 $(( $(date +%s) - T0 ))s) $*"; }

restart_agentd() {
  nomad job restart -on-error fail firepaas-agentd >/dev/null 2>&1 || return 1
  for _ in $(seq 1 45); do
    "$LAB_BIN/agentctl" info >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

guest_exec() {
  local machine="$1"; shift
  local opid="exec-$RANDOM-$RANDOM"
  authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/exec" \
    -H 'Content-Type: application/json' \
    -d "$(python3 -c 'import json,sys; print(json.dumps({"command": list(sys.argv[2:]), "operation_id": sys.argv[1]}))' "$opid" "$@")"
}

exec_stdout() {
  python3 -c 'import base64,json,sys
chunks=[]; rc=None
for line in sys.stdin:
    if not line.strip(): continue
    obj=json.loads(line)
    if "stdout" in obj: chunks.append(base64.b64decode(obj["stdout"]).decode("utf-8","replace"))
    if "exit_code" in obj: rc=obj["exit_code"]
sys.stdout.write("".join(chunks))
raise SystemExit(0 if rc == 0 else (rc if isinstance(rc,int) else 1))'
}

wait_machine_running() {
  local machine="$1" st=""
  for _ in $(seq 1 90); do
    st=$(pg "SELECT observed_state FROM machines WHERE id='$machine'")
    [[ "$st" == "RUNNING" ]] && return 0
    sleep 3
  done
  log "machine $machine did not become RUNNING (state=$st)"
  return 1
}

create_app() {
  local app="$1"
  authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
    -d "{\"project_id\":\"dev\",\"app_id\":\"$app\",\"hostname\":\"$app.$DOMAIN\",\"image\":\"$ONTIME_REF\",\"replicas\":1}" >/dev/null
}

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
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" FIREPAAS_ROLLOUT_TIMEOUT=180s FIREPAAS_ROLLOUT_DRAIN=10s \
  FIREPAAS_GC_MODE=dry-run FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  FIREPAAS_E2E_ALLOW_HTTP_LOOPBACK=1 \
  "$LAB_BIN/firepaas-api" >"$RUN_DIR/v13v-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" FIREPAAS_API_TOKEN="$API_TOKEN" \
  "$LAB_BIN/edge-proxy" >"$RUN_DIR/v13v-edge.log" 2>&1 &
for _ in $(seq 1 40); do authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break; sleep 1; done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || fail "API 未就绪"
ONLINE_OUT=$(bash "$HERE/push-ontime.sh") || fail "push-ontime 失败"
ONTIME_REF=$(printf '%s\n' "$ONLINE_OUT" | grep '^REF=' | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || fail "ontime REF 解析失败"

log "0.5) 预清理"
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now()" >/dev/null
sleep 3

log "1) 默认 node 创建 LOCAL_RW"
cap_count=$(pg "SELECT count(*) FROM nodes WHERE status='HEALTHY' AND feature_ids::text LIKE '%volume.local_rw.v1%'")
[[ "${cap_count:-0}" -ge 1 ]] || blocked "无健康节点发布 volume.local_rw.v1"
vol="v13v-vol-$RUN_ID"
vol_resp=$(authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/volumes" -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"name\":\"$vol\",\"size_gib\":1}")
VOL_ID=$(printf '%s\n' "$vol_resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["volume_id"])')
[[ -n "$VOL_ID" ]] || fail "volume 创建未受理"
VOL_READY=0; st=""
for _ in $(seq 1 45); do st=$(pg "SELECT state FROM volumes WHERE id='$VOL_ID'"); [[ "$st" == "READY" ]] && VOL_READY=1 && break; sleep 2; done
[[ "$VOL_READY" == "1" ]] || fail "volume 未 READY（state=$st）"
vol_node=$(pg "SELECT node_id FROM volumes WHERE id='$VOL_ID'")
node_ok=$(pg "SELECT count(*) FROM nodes WHERE id='$vol_node' AND status='HEALTHY' AND feature_ids::text LIKE '%volume.local_rw.v1%'")
[[ -n "$vol_node" && "$node_ok" -eq 1 ]] || fail "默认 node 未选择健康 capable origin（node=$vol_node）"
mark "默认 node 选择 OK（node=$vol_node）"

log "2) guest 真实写读 + agent restart 持久性"
app="v13v-a-$RUN_ID"; machine="$app-r0-g1"
create_app "$app"; wait_machine_running "$machine" || fail "app A 未就绪"
[[ "$(pg "SELECT node_id FROM machines WHERE id='$machine'")" == "$vol_node" ]] || fail "machine 未落在 volume origin node"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/volume-attach?volume_id=$VOL_ID" \
  -H 'Content-Type: application/json' -d '{"mount_path":"/mnt/data"}' >/dev/null
for _ in $(seq 1 30); do st=$(pg "SELECT status FROM volume_attachments WHERE volume_id='$VOL_ID' AND machine_id='$machine'"); [[ "$st" == "ATTACHED" ]] && break; sleep 2; done
[[ "$st" == "ATTACHED" ]] || fail "attachment 未 ATTACHED（status=$st）"
payload="persist-$RUN_ID"
# attach 是受控冷重启而非 hot attach；PG attachment ACK 可能早于 guest agent
# readiness，故按短间隔重试真实 I/O，而不是依赖陈旧的 RUNNING 投影。
out=""; IO_READY=0
for _ in $(seq 1 30); do
  if out=$(guest_exec "$machine" /bin/busybox sh -c "printf '%s' '$payload' > /mnt/data/probe && /bin/busybox sync && /bin/busybox cat /mnt/data/probe" | exec_stdout 2>/dev/null) && [[ "$out" == "$payload" ]]; then
    IO_READY=1
    break
  fi
  sleep 2
done
[[ "$IO_READY" == "1" ]] || fail "volume guest 写读失败或回读不一致（got=$out）"
restart_agentd || fail "agent restart 未恢复"
wait_machine_running "$machine" || fail "agent restart 后 machine 未恢复"
out=$(guest_exec "$machine" /bin/busybox cat /mnt/data/probe | exec_stdout) || fail "agent restart 后 volume 不可读"
[[ "$out" == "$payload" ]] || fail "agent restart 后数据丢失（got=$out）"
mark "真实写读 + agent restart 持久性 OK"

log "3) 两台 machine 并发竞争第二个 LOCAL_RW attach"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/volume-detach?volume_id=$VOL_ID" >/dev/null
for _ in $(seq 1 30); do st=$(pg "SELECT status FROM volume_attachments WHERE volume_id='$VOL_ID' AND machine_id='$machine'"); [[ "$st" == "DETACHED" ]] && break; sleep 2; done
[[ "$st" == "DETACHED" ]] || fail "竞争测试前 detach 未收敛"
app_b="v13v-b-$RUN_ID"; app_c="v13v-c-$RUN_ID"; machine_b="$app_b-r0-g1"; machine_c="$app_c-r0-g1"
create_app "$app_b"; create_app "$app_c"
wait_machine_running "$machine_b" || fail "app B 未就绪"
wait_machine_running "$machine_c" || fail "app C 未就绪"
for m in "$machine_b" "$machine_c"; do [[ "$(pg "SELECT node_id FROM machines WHERE id='$m'")" == "$vol_node" ]] || fail "$m 不在 origin node"; done
for pair in "b:$machine_b" "c:$machine_c"; do
  key=${pair%%:*}; m=${pair#*:}
  (authed_raw -o "$RUN_DIR/attach-$key.body" -w '%{http_code}' -X POST \
    "http://127.0.0.1:$API_PORT/v1/machines/$m/volume-attach?volume_id=$VOL_ID" \
    -H 'Content-Type: application/json' -d "{\"mount_path\":\"/mnt/$key\"}" >"$RUN_DIR/attach-$key.code") &
  if [[ "$key" == "b" ]]; then attach_pid_b=$!; else attach_pid_c=$!; fi
done
wait "$attach_pid_b" || true
wait "$attach_pid_c" || true
code_b=$(<"$RUN_DIR/attach-b.code"); code_c=$(<"$RUN_DIR/attach-c.code")
[[ ( "$code_b" == "202" && "$code_c" == "409" ) || ( "$code_b" == "409" && "$code_c" == "202" ) ]] || \
  fail "并发双 attach 必须恰一成功（b=$code_b c=$code_c）"
winner="$machine_b"; [[ "$code_c" == "202" ]] && winner="$machine_c"
for _ in $(seq 1 30); do active=$(pg "SELECT count(*) FROM volume_attachments WHERE volume_id='$VOL_ID' AND status IN ('PENDING','ATTACHED')"); attached=$(pg "SELECT count(*) FROM volume_attachments WHERE volume_id='$VOL_ID' AND status='ATTACHED'"); [[ "$active" -eq 1 && "$attached" -eq 1 ]] && break; sleep 2; done
[[ "$active" -eq 1 && "$attached" -eq 1 ]] || fail "并发后 active attachment 非唯一（active=$active attached=$attached）"
mark "并发双 attach 原子单写 OK（winner=$winner）"

log "4) node loss → UNAVAILABLE；恢复后不空建且数据仍在"
nomad job stop firepaas-agentd >/dev/null || fail "停止 agentd job 失败"
UNAVAILABLE=0
for _ in $(seq 1 60); do st=$(pg "SELECT state FROM volumes WHERE id='$VOL_ID'"); [[ "$st" == "UNAVAILABLE" ]] && UNAVAILABLE=1 && break; sleep 2; done
[[ "$UNAVAILABLE" == "1" ]] || fail "node loss 后 volume 未转 UNAVAILABLE（state=$st）"
[[ "$(pg "SELECT node_id FROM volumes WHERE id='$VOL_ID'")" == "$vol_node" ]] || fail "node loss 后 volume locality 被改写"
"$HERE/run-agentd.sh" >/dev/null || fail "恢复 agentd 失败"
for _ in $(seq 1 60); do st=$(pg "SELECT state FROM volumes WHERE id='$VOL_ID'"); [[ "$st" == "READY" ]] && break; sleep 2; done
[[ "$st" == "READY" ]] || fail "origin node 恢复后 volume 未 READY（state=$st）"
# node restart 后带 LOCAL_RW 的 machine 不得在别处自动重建；运行态可保持
# UNKNOWN，恢复由显式 operator action 决定。直接校验 origin 上物理卷仍存在。
post_state=$(pg "SELECT observed_state FROM machines WHERE id='$winner'")
[[ "$post_state" != "RUNNING" || "$(pg "SELECT node_id FROM machines WHERE id='$winner'")" == "$vol_node" ]] || fail "LOCAL_RW machine 被跨节点重建"
volume_present=$(printf '1\n' | sudo -S test -f "/var/lib/firepaas-p0/hypeman/volumes/$VOL_ID/metadata.json" && echo 1 || echo 0)
[[ "$volume_present" == "1" ]] || fail "origin node 恢复后本地 volume artifact 丢失"
mark "node loss/恢复后 locality 与本地 artifact 保持 OK（machine_state=$post_state）"

log "5) DATASET_RO 恶意 archive 拒绝"
dataset_cap=0
for _ in $(seq 1 30); do
  dataset_cap=$(pg "SELECT count(*) FROM nodes WHERE id='$vol_node' AND status='HEALTHY' AND feature_ids::text LIKE '%volume.dataset_ro.v1%'")
  [[ "$dataset_cap" -eq 1 ]] && break
  sleep 2
done
[[ "$dataset_cap" -eq 1 ]] || fail "origin node 恢复后未重新发布 volume.dataset_ro.v1"
FIXTURE_DIR="$RUN_DIR/dataset-fixture"
mkdir -p "$FIXTURE_DIR"
python3 - "$FIXTURE_DIR/traversal.tar.gz" <<'PY'
import io,tarfile,sys
with tarfile.open(sys.argv[1], 'w:gz') as tf:
    data=b'escape'
    info=tarfile.TarInfo('../escape')
    info.size=len(data)
    tf.addfile(info, io.BytesIO(data))
PY
mal_digest="sha256:$(sha256sum "$FIXTURE_DIR/traversal.tar.gz" | awk '{print $1}')"
python3 -m http.server 18097 --bind 127.0.0.1 --directory "$FIXTURE_DIR" >"$RUN_DIR/dataset-fixture.log" 2>&1 &
fixture_pid=$!
trap 'kill "$fixture_pid" 2>/dev/null || true' EXIT
mal_url="http://127.0.0.1:18097/traversal.tar.gz"
mal_resp="$RUN_DIR/malicious-dataset.json"
mal_code=$(authed_raw -o "$mal_resp" -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/volumes" \
  -H 'Content-Type: application/json' -d "{\"project_id\":\"dev\",\"name\":\"mal-$RUN_ID\",\"mode\":\"DATASET_RO\",\"size_gib\":1,\"node_id\":\"$vol_node\",\"source_url\":\"$mal_url\",\"content_digest\":\"$mal_digest\",\"max_download_bytes\":1048576,\"max_files\":10}")
[[ "$mal_code" == "202" ]] || fail "恶意 dataset 请求未受理（code=$mal_code body=$(tr '\n' ' ' <"$mal_resp")）"
MAL_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["volume_id"])' "$mal_resp")
MAL_REJECTED=0; op_status=""; op_error=""
for _ in $(seq 1 45); do
  op_status=$(pg "SELECT coalesce(status,'') FROM operations WHERE id='op-vol-$MAL_ID'")
  op_error=$(pg "SELECT replace(coalesce(error,''),' ','_') FROM operations WHERE id='op-vol-$MAL_ID'")
  [[ "$op_status" == "FAILED" ]] && MAL_REJECTED=1 && break
  [[ "$(pg "SELECT state FROM volumes WHERE id='$MAL_ID'")" == "READY" ]] && fail "恶意 archive 被 seal 为 READY"
  sleep 2
done
[[ "$MAL_REJECTED" == "1" && "$op_error" == *archive* ]] || fail "恶意 archive 未以 archive 校验错误拒绝（status=$op_status error=${op_error//_/ }）"
mark "DATASET 恶意 archive 拒绝 OK"

log "6) CoW capability 与结论（v1.4 语义：未验收不广告、fail closed）"
# v1.4-A：per-execution CoW 尚未通过 hypeman capability、磁盘 admission、
# cleanup 与真机 e2e 验收，agent 必须不发布 volume.dataset_overlay.v1，
# API 对 overlay attach 明确拒绝（fail closed）而非虚假广告。
cow_cap=$(pg "SELECT count(*) FROM nodes WHERE id='$vol_node' AND feature_ids::text LIKE '%volume.dataset_overlay.v1%'")
[[ "$cow_cap" -eq 0 ]] || fail "volume.dataset_overlay.v1 被发布（未验收 CoW 不得广告）"
# overlay attach 必须被 API 明确拒绝（409 能力不足或 400 语义不符；不得隐式
# 接受或降级为非 overlay 挂载）。
if [[ -n "${VOL_ID:-}" && -n "${machine:-}" ]]; then
  overlay_code=$(authed_raw -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/volume-attach?volume_id=$VOL_ID" \
    -H 'Content-Type: application/json' -d '{"mount_path":"/mnt/ds","overlay_size_bytes":1048576}')
  [[ "$overlay_code" == "409" || "$overlay_code" == "400" ]] || fail "overlay attach 应被明确拒绝（got $overlay_code）"
fi
mark "DATASET_RO fail-closed OK（overlay 未广告且明确拒绝）"
log "ALL PASS"
