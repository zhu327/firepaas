#!/usr/bin/env bash
# v1.2 single-node smoke harness：提供局部集成回归，不是发布级 e2e/HA 证据。
#   A) capability discovery（ADR-0023）：
#      - /v1/nodes 携带 feature_ids；/v1/capabilities 汇总 eligible 节点数
#        （含 secret.oneshot.v1，随 v1.2-B one-shot 通道默认上报）
#      - 绑定 secret 的 deployment 落到支持节点（one-shot 通道随 agent 交付）
#   B) one-shot secret（ADR-0024）：canary 端到端——投递放行 gate、lease
#      ACKED、entrypoint 经路由读到 secret、exec 环境隔离、pause 拒绝、
#      host/PG/API 无明文落盘；secret_refs+auto_standby 互斥 400
#   C) logs/exec/cp（ADR-0025）：
#      - logs tail/follow、exec 非交互 stdout/exit code、cp up/down checksum
#      - symlink 与目录被拒绝；跨 execution（换代后）旧 execution 被拒绝
#   D) wait/TTL（ADR-0026 子集）：
#      - wait execution X ready → reached；TTL 到期 → desired DELETED 且
#        route 摘除；wait 旧 execution → superseded
# 后续段还 smoke E/F 的配额、限流、事件与 GC dry-run。明确不覆盖：混合版本
# capability 多节点调度、跨节点 restart/leader handover、secret 投递崩溃矩阵、
# GC delete/回拉、多 edge/标准拓扑、72h soak；见 docs/v1.2-implementation-notes.md。
# 用法: sudo bash scripts/lab/e2e-v12.sh
# 运行约定：后台运行 + 日志轮询（全部等待有界、每行带时间戳）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-v12"
RUN_ID="v12-$(date +%s)"
API_TOKEN="v12-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT=8096
EDGE_HTTP=8097
EDGE_TLS=8497
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-v12 $(now)] $*"; }
fail() { echo "[e2e-v12] FAIL: $*" >&2; exit 1; }
trap 'log "TRACE: died at line $LINENO rc=$? cmd=$BASH_COMMAND"' ERR
authed_curl() { curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
authed_raw() { curl -sS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
mark() { log "    (累计 $(( $(date +%s) - T0 ))s) $*"; }

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建（make build 并复制到 $LAB_BIN）"
[[ -f "$CERT_DIR/wildcard-$DOMAIN.crt" ]] || fail "泛域名证书缺失（先跑 scripts/lab/gen-certs.sh）"

log "0) 启动 agentd（root）+ API/edge"
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
  FIREPAAS_GC_MODE=dry-run FIREPAAS_GC_INTERVAL=20s FIREPAAS_GC_MIN_AGE=1s \
  FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/v12-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/v12-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/v12-api.log"; fail "API 未就绪"; }
mark "api/edge up"

# ontime 探针镜像（含 /healthz 与 /slow 端点），供 C/D 与 secret fail-closed 用。
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

log "1) A：capability discovery"
# 节点投影已含 feature_ids（agentd 每 20s sync；等一个周期）。
NODE_CAP_OK=0
for _ in $(seq 1 30); do
  feats=$(pg "SELECT count(*) FROM nodes WHERE status='HEALTHY' AND jsonb_array_length(feature_ids) >= 3")
  [[ "${feats:-0}" -ge 1 ]] && NODE_CAP_OK=1 && break
  sleep 4
done
[[ "$NODE_CAP_OK" == "1" ]] || fail "节点 capability 投影未落库（feature_ids < 3）"
authed_curl "http://127.0.0.1:$API_PORT/v1/capabilities" | tee "$RUN_DIR/caps.json"
python3 - "$RUN_DIR/caps.json" <<'PY' || fail "capabilities 汇总缺预期能力"
import json, sys
caps = json.load(open(sys.argv[1]))["capabilities"]
by = {c["feature_id"]: c for c in caps}
for f in ("guest.exec.v1", "guest.copy.v1", "guest.logs.v1", "secret.oneshot.v1"):
    assert by.get(f, {}).get("eligible_nodes", 0) >= 1, f"missing {f}"
PY
mark "capability discovery OK"

log "2) B：one-shot secret delivery（canary 端到端，ADR-0024）"
secret_name="v12-secret-$RUN_ID"
CANARY="canary-$RUN_ID"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/secrets" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"name\":\"$secret_name\",\"value\":\"$CANARY\"}" >/dev/null
secret_app="v12-secret-app-$RUN_ID"
secret_host="$secret_app.$DOMAIN"
# 负路径：secret_refs + 启用的 auto_standby 互斥（ADR-0024 §9）。
sb_code=$(authed_raw -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$secret_app-sb\",\"hostname\":\"$secret_app-sb.$DOMAIN\",\"image\":\"$ONTIME_REF\",\"replicas\":1,\"secret_refs\":{\"TOKEN\":{\"secret\":\"$secret_name\"}},\"auto_standby\":{\"enabled\":true,\"idle_timeout_seconds\":60}}")
[[ "$sb_code" == "400" ]] || fail "secret+auto_standby 应 400，got $sb_code"
# 正路径：secret app 调度、投递、就绪。
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$secret_app\",\"hostname\":\"$secret_host\",\"image\":\"$ONTIME_REF\",\"replicas\":1,\"secret_refs\":{\"TOKEN\":{\"secret\":\"$secret_name\"}},\"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:8080/env?k=PORT\",\"interval_seconds\":2,\"timeout_seconds\":2,\"unhealthy_threshold\":3}}" >/dev/null
secret_machine="$secret_app-r0-g1"
for _ in $(seq 1 30); do
  secret_exec=$(pg "SELECT current_execution_id FROM machines WHERE id='$secret_machine'")
  [[ -n "${secret_exec:-}" ]] && break
  sleep 2
done
[[ -n "${secret_exec:-}" ]] || fail "secret machine 行缺失或 execution 为空"
SECRET_READY=0
for _ in $(seq 1 90); do
  w=$(authed_raw "http://127.0.0.1:$API_PORT/v1/machines/$secret_machine/wait?execution_id=$secret_exec&timeout_ms=3000" || true)
  echo "$w" | grep -q '"status":"reached"' && SECRET_READY=1 && break
  sleep 3
done
[[ "$SECRET_READY" == "1" ]] || fail "secret app 未就绪（投递链路失败？查看 $RUN_DIR/v12-api.log）"
mark "secret app ready（delivery gate 已放行）"
# lease 状态机：ACKED（entrypoint 启动 = guest 已消费）。
LEASE_STATE=0
for _ in $(seq 1 30); do
  acked=$(pg "SELECT count(*) FROM secret_delivery_leases WHERE machine_id='$secret_machine' AND state='ACKED'")
  [[ "${acked:-0}" == "1" ]] && LEASE_STATE=1 && break
  sleep 2
done
[[ "$LEASE_STATE" == "1" ]] || fail "secret delivery lease 未到 ACKED"
mark "lease ACKED"
# canary 正向：entrypoint 进程读到 secret（经 edge TLS 路由）。
env_val=$(curl -fsS -m 10 -sk --resolve "$secret_host:$EDGE_TLS:127.0.0.1" "https://$secret_host:$EDGE_TLS/env?k=TOKEN" || env_val="")
echo "$env_val" | grep -q "$CANARY" || fail "entrypoint 未读到 secret（canary 未注入）: $env_val"
mark "canary 经路由可见"
# canary 隔离：exec 会话（guest agent 进程 env）不含 secret。
# 镜像无 env/grep，用 shell 内建参数展开：无 TOKEN 时输出 unset。
exec_env=$(authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/machines/$secret_machine/exec" \
  -H 'Content-Type: application/json' \
  -d '{"command":["/bin/sh","-c","echo exec_env_token=${TOKEN-unset}"]}')
EXEC_ENV="$exec_env" python3 - <<'PY' || fail "exec 环境不应含 secret"
import json, os, base64
lines = [json.loads(l) for l in os.environ["EXEC_ENV"].splitlines() if l.strip()]
stdout = b"".join(base64.b64decode(l["stdout"]) for l in lines if "stdout" in l)
exit_codes = [l["exit_code"] for l in lines if "exit_code" in l]
assert exit_codes == [0], exit_codes
assert b"exec_env_token=unset" in stdout, stdout  # secret 不得进入 exec 会话环境
PY
mark "exec env 隔离 OK"
# 负路径：pause（memory snapshot）对 secret execution 禁止。
pause_code=$(authed_raw -o /dev/null -w '%{http_code}' -X POST \
  "http://127.0.0.1:$API_PORT/v1/machines/$secret_machine/pause")
[[ "$pause_code" == "409" ]] || fail "secret machine pause 应 409，got $pause_code"
mark "pause 拒绝 OK"
# canary 扫描：明文不得出现在节点落盘（agent ledger、hypeman metadata、
# config 盘、overlay 盘、快照）与 PG 持久化请求/结果中。
leak_host=$(grep -rl "$CANARY" /var/lib/firepaas-p0 2>/dev/null | head -3 || true)
[[ -z "$leak_host" ]] || fail "canary 泄漏到节点磁盘: $leak_host"
leak_pg=$(pg "SELECT count(*) FROM operations WHERE request::text LIKE '%$CANARY%' OR result::text LIKE '%$CANARY%'")
[[ "$leak_pg" == "0" ]] || fail "canary 泄漏到 operations 持久化"
leak_api=$(authed_raw "http://127.0.0.1:$API_PORT/v1/machines/$secret_machine" | grep -c "$CANARY" || true)
[[ "$leak_api" == "0" ]] || fail "canary 出现在 machine API 回显"
mark "canary 无落盘泄漏（host/PG/API）"

log "3) C：创建 ontime 验收 app"
app="v12-ontime-$RUN_ID"
host="$app.$DOMAIN"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/apps" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$app\",\"hostname\":\"$host\",\"image\":\"$ONTIME_REF\",\"replicas\":1,\"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:8080/\",\"interval_seconds\":2,\"timeout_seconds\":2,\"unhealthy_threshold\":3}}" >/dev/null
machine_id="$app-r0-g1"

log "4) D：wait execution X ready → reached"
for _ in $(seq 1 30); do
  cur_exec=$(pg "SELECT current_execution_id FROM machines WHERE id='$machine_id'")
  [[ -n "$cur_exec" && "$cur_exec" != "" ]] && break
  sleep 2
done
[[ -n "$cur_exec" ]] || fail "machine 行缺失或 execution 为空"
for _ in $(seq 1 90); do
  w=$(authed_raw "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/wait?execution_id=$cur_exec&timeout_ms=4000" || true)
  echo "$w" | grep -q '"status":"reached"' && break
  sleep 4
done
echo "$w" | grep -q '"status":"reached"' || fail "wait machine ready 未 reached: $w"
mark "wait reached OK"

log "5) C：logs tail + exec"
authed_curl "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/logs?follow=false&tail=true" > "$RUN_DIR/logs.txt" || true
grep -qiE "ontime|healthz|ready|listen" "$RUN_DIR/logs.txt" || log "    (warn) logs 无标志行；继续（serial 输出可能为空）"
exec_out=$(authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/exec" \
  -H 'Content-Type: application/json' \
  -d '{"command":["/bin/sh","-c","echo hello-v12"]}')
EXEC_OUT="$exec_out" python3 - <<'PY' || fail "exec 输出/退出码错误"
import json, os, base64
lines = [json.loads(l) for l in os.environ["EXEC_OUT"].splitlines() if l.strip()]
stdout = b"".join(base64.b64decode(l["stdout"]) for l in lines if "stdout" in l)
exit_codes = [l["exit_code"] for l in lines if "exit_code" in l]
assert exit_codes == [0], exit_codes
assert b"hello-v12" in stdout, stdout
PY
mark "logs/exec OK"

log "6) C：cp up/down checksum + symlink/目录拒绝"
src="$RUN_DIR/v12-src.bin"
head -c 1048576 /dev/urandom > "$src"   # 1 MiB 随机文件
authed_curl -X PUT "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/files?path=%2Ftmp%2Fv12-file" \
  -H 'Content-Type: application/octet-stream' --data-binary "@$src" | grep -q '"bytes_written":1048576' || fail "cp up 失败"
authed_curl "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/files?path=%2Ftmp%2Fv12-file" \
  -o "$RUN_DIR/v12-dst.bin"
cmp "$src" "$RUN_DIR/v12-dst.bin" || fail "cp down checksum 不一致"
# 目录 cp 拒绝（v1.2 只支持单普通文件）
dir_code=$(authed_raw -o /dev/null -w '%{http_code}' \
  "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/files?path=%2Ftmp")
[[ "$dir_code" == "400" ]] || fail "目录 cp 应 400，got $dir_code"
# symlink 拒绝：先 exec 建链，再 GET。无论 guest 返回 header（agent 拒
# 绝→400）还是 copy error（→404），关键不变量是拿不到 symlink 目标内容。
authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/exec" \
  -H 'Content-Type: application/json' \
  -d '{"command":["/bin/sh","-c","ln -sf /etc/passwd /tmp/v12-link; echo ln_rc=$?"]}' >/dev/null
link_code=$(authed_raw -o /dev/null -w '%{http_code}' \
  "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/files?path=%2Ftmp%2Fv12-link")
[[ "$link_code" == "400" || "$link_code" == "404" ]] || fail "symlink cp 应被拒绝（400/404），got $link_code"
# 路径逃逸（.. 段）直接被白名单拒绝（400），不触达 guest。
esc_code=$(authed_raw -o /dev/null -w '%{http_code}' \
  "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/files?path=%2Ftmp%2F..%2Fetc%2Fpasswd")
[[ "$esc_code" == "400" ]] || fail "路径逃逸应 400，got $esc_code"
mark "cp checksum + 边界拒绝 OK"

log "7) D：TTL 到期 → 摘路由 + desired DELETED"
ttl_machine="v12-ttl-$RUN_ID"
authed_curl -X POST "http://127.0.0.1:$API_PORT/v1/machines" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$ttl_machine\",\"hostname\":\"$ttl_machine.$DOMAIN\",\"image\":\"$ONTIME_REF\",\"machine_id\":\"$ttl_machine\",\"execution_id\":\"exec-1\",\"operation_id\":\"op-ttl-create-$RUN_ID\",\"ttl_seconds\":25,\"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:8080/\",\"interval_seconds\":2,\"timeout_seconds\":2,\"unhealthy_threshold\":3}}" >/dev/null
for _ in $(seq 1 60); do
  st=$(pg "SELECT desired_state FROM machines WHERE id='$ttl_machine'")
  [[ "$st" == "DELETED" ]] && break
  sleep 3
done
[[ "$st" == "DELETED" ]] || fail "TTL 到期后未删除（state=$st）"
rt=$(pg "SELECT count(*) FROM route_backends WHERE machine_id='$ttl_machine'")
[[ "$rt" == "0" ]] || fail "TTL 删除后 route 未摘除"
# 已过期 machine 不得通过更新 TTL 复活。
code=$(authed_raw -o /dev/null -w '%{http_code}' -X PUT \
  "http://127.0.0.1:$API_PORT/v1/machines/$ttl_machine/ttl" \
  -H 'Content-Type: application/json' -d '{"ttl_seconds":600}')
[[ "$code" == "409" ]] || fail "过期 machine 更新 TTL 应 409，got $code"
mark "TTL 到期闭环 OK"

log "8) D：wait 旧 execution → superseded"
w2=$(authed_raw "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/wait?execution_id=exec-nope&timeout_ms=1000" || true)
echo "$w2" | grep -q '"status":"superseded"' || fail "wait 旧 execution 应 superseded: $w2"
mark "wait superseded OK"

log "9) E：项目配额（revision CAS + machine 并发）"
# 配额读（ETag）与冲突写。
qtag=$(authed_raw -D - -o /tmp/v12-quota.json "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" | grep -i '^etag:' | tr -d '\r' | cut -d' ' -f2)
[[ -n "$qtag" ]] || fail "quota GET 无 ETag"
qrev=$(python3 -c 'import json;print(json.load(open("/tmp/v12-quota.json"))["revision"])')
qcode=$(authed_raw -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" \
  -H 'Content-Type: application/json' \
  -d "{\"vcpu_quota\":2,\"mem_mib_quota\":1024,\"disk_mib_quota\":1048576,\"machine_concurrency\":99,\"runtime_session_concurrency\":99,\"revision\":$((qrev+1))}")
[[ "$qcode" == "409" ]] || fail "旧 revision 配额写应 409，got $qcode"
qcode=$(authed_raw -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" \
  -H 'Content-Type: application/json' \
  -d "{\"vcpu_quota\":2,\"mem_mib_quota\":1024,\"disk_mib_quota\":1048576,\"machine_concurrency\":99,\"runtime_session_concurrency\":99,\"revision\":$qrev}")
[[ "$qcode" == "200" ]] || fail "正确 revision 配额写应 200，got $qcode"
mark "quota revision CAS OK"
# machine 并发：dev 目前至少 1 台活跃；把并发限到 1，再建第 2 台必须拒绝。
qrev2=$(authed_raw "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" | python3 -c 'import json,sys;print(json.load(sys.stdin)["revision"])')
authed_raw -o /dev/null -X PUT "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" \
  -H 'Content-Type: application/json' \
  -d "{\"vcpu_quota\":64,\"mem_mib_quota\":32768,\"disk_mib_quota\":1048576,\"machine_concurrency\":1,\"runtime_session_concurrency\":99,\"revision\":$qrev2}" >/dev/null
qm="v12-qmachine-$RUN_ID"
authed_raw -o /dev/null -X POST "http://127.0.0.1:$API_PORT/v1/machines" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"app_id\":\"$qm\",\"hostname\":\"$qm.$DOMAIN\",\"image\":\"$ONTIME_REF\",\"machine_id\":\"$qm\",\"execution_id\":\"exec-q1\",\"operation_id\":\"op-qmachine-$RUN_ID\"}" >/dev/null
QUOTA_BLOCKED=0
for _ in $(seq 1 40); do
  qs=$(pg "SELECT status FROM operations WHERE id='op-qmachine-$RUN_ID'")
  [[ "$qs" == "FAILED" ]] && QUOTA_BLOCKED=1 && break
  sleep 2
done
[[ "$QUOTA_BLOCKED" == "1" ]] || { pg "UPDATE machines SET desired_state='DELETED' WHERE id='$qm'" >/dev/null; fail "machine 并发超限未被拒绝（status=$qs）"; }
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE id='$qm'" >/dev/null
# 恢复配额。
qrev3=$(authed_raw "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" | python3 -c 'import json,sys;print(json.load(sys.stdin)["revision"])')
authed_raw -o /dev/null -X PUT "http://127.0.0.1:$API_PORT/v1/projects/dev/quota" \
  -H 'Content-Type: application/json' \
  -d "{\"vcpu_quota\":64,\"mem_mib_quota\":32768,\"disk_mib_quota\":1048576,\"machine_concurrency\":99,\"runtime_session_concurrency\":99,\"revision\":$qrev3}" >/dev/null
mark "machine 并发配额 OK"

log "10) E：API 限流（project × route_class）"
# 租户视角限流：root 走 __root__ 桶，项目限流要用项目绑定的 API key 验证。
TKEY=$(authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/apikeys" \
  -H 'Content-Type: application/json' \
  -d '{"name":"v12-rl-'$RUN_ID'","scopes":["read"],"project_id":"dev"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["key"])')
[[ -n "$TKEY" ]] || fail "API key 创建失败"
# stream 类限流：低速率 + burst 1，连续两个 stream 请求第二个应 429。
authed_raw -o /dev/null -X PUT "http://127.0.0.1:$API_PORT/v1/projects/dev/rate-limits" \
  -H 'Content-Type: application/json' \
  -d '{"read_rate":1000,"read_burst":2000,"mutation_rate":1000,"mutation_burst":2000,"stream_rate":0.0001,"stream_burst":1}' >/dev/null
sleep 11  # 等配置缓存过期（10s）+ 令牌桶积累 1 个
tcurl() { curl -sS -m 20 -H "Authorization: Bearer $TKEY" "$@"; }
tcurl -o /dev/null "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/wait?execution_id=exec-nope&timeout_ms=500" || true
rl_code=$(tcurl -o /dev/null -w '%{http_code}' "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/wait?execution_id=exec-nope&timeout_ms=500" || true)
restore_rl() { authed_raw -o /dev/null -X PUT "http://127.0.0.1:$API_PORT/v1/projects/dev/rate-limits" \
  -H 'Content-Type: application/json' \
  -d '{"read_rate":100,"read_burst":200,"mutation_rate":20,"mutation_burst":40,"stream_rate":5,"stream_burst":10}' >/dev/null; }
[[ "$rl_code" == "429" ]] || { restore_rl; fail "stream 限流应 429，got $rl_code"; }
# 恢复限流配置并验证立即生效（SetConfig 直写缓存）。
restore_rl
sleep 1  # 桶以新速率（5/s）回填 1 个令牌需 ~0.2s
rl2=$(tcurl -o /dev/null -w '%{http_code}' "http://127.0.0.1:$API_PORT/v1/machines/$machine_id/wait?execution_id=exec-nope&timeout_ms=500" || true)
[[ "$rl2" == "404" || "$rl2" == "200" ]] || fail "恢复限流后 wait 应正常，got $rl2"
mark "rate limit 429 + 恢复 OK（租户 key）"

log "11) F：用户事件 + 引用感知 GC（dry-run）"
# 租户事件：本 run 产生的 machine.created / secret.delivered / quota.rejected
# 对 dev 可见；type 过滤与游标分页可用。
evs=$(authed_raw "http://127.0.0.1:$API_PORT/v1/events?project_id=dev&type=machine.created&limit=50")
EV_OUT="$evs" python3 - <<'PY' || fail "user events 缺 machine.created"
import json, os
evs = json.loads(os.environ["EV_OUT"])["events"]
assert len(evs) >= 1, evs
assert all(e["project_id"] == "dev" for e in evs), evs
PY
evs=$(authed_raw "http://127.0.0.1:$API_PORT/v1/events?project_id=dev&type=secret.delivered&limit=10")
EV_OUT="$evs" python3 - <<'PY' || fail "user events 缺 secret.delivered（B 段 canary 应已产生）"
import json, os
evs = json.loads(os.environ["EV_OUT"])["events"]
assert len(evs) >= 1, evs
PY
evs=$(authed_raw "http://127.0.0.1:$API_PORT/v1/events?project_id=dev&type=quota.rejected&limit=10")
EV_OUT="$evs" python3 - <<'PY' || fail "user events 缺 quota.rejected（E 段应已产生）"
import json, os
evs = json.loads(os.environ["EV_OUT"])["events"]
assert len(evs) >= 1, evs
PY
mark "user_events（created/secret/quota）可见 OK"
# GC（dry-run）：短间隔巡检后 gc_seen_digests 有记账行；active root 镜像
# 仍在 agent 缓存（从未被删）。
GC_SEEN=0
for _ in $(seq 1 20); do
  n=$(pg "SELECT count(*) FROM gc_seen_digests")
  [[ "${n:-0}" -ge 1 ]] && GC_SEEN=1 && break
  sleep 3
done
[[ "$GC_SEEN" == "1" ]] || fail "gc_seen_digests 无记账（GC 巡检未运行？）"
gc_root=$(pg "SELECT count(*) FROM deployments WHERE status IN ('PREPARING','ACTIVE') AND image_ref LIKE '%ontime%'")
[[ "$gc_root" -ge 1 ]] || fail "active deployment root 缺失"
img=$(pg "SELECT count(*) FROM nodes WHERE status='HEALTHY' AND image_cache::text LIKE '%ee15b52a%'")
[[ "$img" -ge 1 ]] || log "    (warn) 节点缓存投影暂无 ontime digest；GC root 断言以 PG 为准"
mark "GC dry-run 记账 OK（active root 保留）"

log "12) 清理 + 结论"
authed_curl -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$app" >/dev/null || true
authed_curl -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$secret_app" >/dev/null || true
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE app_id IN ('$app','$secret_app','$secret_app-sb')" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now() WHERE id IN ('$app','$secret_app','$secret_app-sb')" >/dev/null
log "ALL PASS"
