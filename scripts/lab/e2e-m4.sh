#!/usr/bin/env bash
# M4 e2e harness（单机）：mvp-plan §8 验收（ADR-0006/0010/0011）
#   A) secrets v1：写入/版本/绑定/下发（PG 与本地状态零明文，无 reveal）
#   B) execution-bound credential：traffic-token 认证；无/错/跨 execution 凭证 403
#   C) secret 版本轮转 → 新 deployment rollout COMPLETE → 持续服务
#   D) 每 hostname 限流（429）+ :80 → 308 https
#   E) Redis 宕机注入：serve-stale 窗口内数据面继续服务；恢复后回源
#   F) 清理 + 删除后凭证撤销 + 终态零泄漏
# 用法: sudo bash scripts/lab/e2e-m4.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-m3"
RUN_ID="e2e-m4-$(date +%s)"
API_TOKEN="m4-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
APP="app-m4-$RUN_ID"
HN="$APP.$DOMAIN"
API_PORT=8081        # API 监听（本机 8080 可能被 m3 遗留占用，独立端口）
EDGE_HTTP=8082       # edge 明文（308 跳转）
EDGE_TLS=8443        # edge TLS 客户端入口
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-m4 $(now)] $*"; }
fail() { echo "[e2e-m4] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
# 经 edge TLS 入口的请求（信任内部 CA + hosts 映射）。
edge_curl() { curl -s -m 15 --resolve "$HN:$EDGE_TLS:127.0.0.1" --cacert "$CERT_DIR/ca.crt" "$@"; }

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建（make build）"
[[ -f "$CERT_DIR/wildcard-$DOMAIN.crt" ]] || fail "泛域名证书缺失：bash scripts/lab/gen-certs.sh"

log "0) root setup + agentd 就绪 + API/edge（secrets+traffic key+TLS）"
T0=$(date +%s)
mark() { log "    (耗时 $(( $(date +%s) - T0 ))s) $1"; T0=$(date +%s); }
"$HERE/root-setup.sh" >/dev/null
"$HERE/run-agentd.sh" >/dev/null || fail "agentd 未就绪"
nomad job restart -on-error fail firepaas-agentd >/dev/null 2>&1 || true
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
  FIREPAAS_ROLLOUT_TIMEOUT=120s FIREPAAS_ROLLOUT_DRAIN=10s \
  FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/m4-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" FIREPAAS_EDGE_RATE_LIMIT=100 FIREPAAS_EDGE_RATE_BURST=200 \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/m4-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/m4-api.log"; fail "API 未就绪"; }
mark "api/edge up"
hc=$(curl -sk -m 5 -o /dev/null -w '%{http_code}' --resolve "x.$DOMAIN:$EDGE_TLS:127.0.0.1" "https://x.$DOMAIN:$EDGE_TLS/healthz")
[[ "$hc" == "200" ]] || { tail -5 "$RUN_DIR/m4-edge.log"; fail "edge TLS 未就绪 ($hc)"; }

log "0.5) 预清理历史验收机（幂等重跑）"
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now()" >/dev/null
pg "UPDATE deployments SET status='SUPERSEDED', updated_at=now() WHERE status IN ('ACTIVE','PREPARING')" >/dev/null
pg "UPDATE rollouts SET status='COMPLETE', failed=true, completed_at=now(), updated_at=now()
	WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK')" >/dev/null
for _ in $(seq 1 60); do
  left=$(pg "SELECT count(*) FROM machines WHERE desired_state != 'DELETED'")
  [[ "$left" == "0" ]] && break
  sleep 5
done

log "A) secrets v1：写入/版本/无 reveal/绑定创建 app"
sec_status=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /tmp/m4-secret.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/secrets" -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"name\":\"db-password\",\"value\":\"s3cr3t-PASS-$RUN_ID\",\"created_by\":\"e2e\"}")
[[ "$sec_status" == "201" ]] || fail "put secret HTTP $sec_status: $(cat /tmp/m4-secret.json)"

pg "SELECT value_ciphertext FROM secrets WHERE name='db-password'" | grep -q "s3cr3t" && fail "PG 密文列出现明文特征"
[[ "$(pg "SELECT count(*) FROM secrets WHERE name='db-password' AND version=1")" == "1" ]] || fail "version 行缺失"
meta=$(authed_curl "http://127.0.0.1:$API_PORT/v1/secrets/db-password?project_id=dev")
echo "$meta" | grep -q "s3cr3t" && fail "元数据端点泄露值：$meta"
code_noauth=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$API_PORT/v1/secrets/db-password?project_id=dev")
[[ "$code_noauth" == "401" ]] || fail "secret 端点未认证应 401, got $code_noauth"

create_status=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /tmp/m4-app.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP\",\"project_id\":\"dev\",\"hostname\":\"$HN\",
       \"image\":\"docker.m.daocloud.io/library/nginx:alpine\",\"port\":80,\"replicas\":1,
       \"secret_refs\":{\"DB_PASS\":{\"secret\":\"db-password\"}}}")
[[ "$create_status" == "201" ]] || fail "app create HTTP $create_status: $(cat /tmp/m4-app.json)"

for _ in $(seq 1 90); do
  running=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND observed_state='RUNNING' AND desired_state!='DELETED'")
  [[ "$running" == "1" ]] && break
  sleep 3
done
[[ "$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND observed_state='RUNNING'")" == "1" ]] || fail "机器未 RUNNING"

body_code=$(edge_curl -o /tmp/m4-body.txt -w '%{http_code}' "https://$HN:$EDGE_TLS/")
[[ "$body_code" == "200" ]] || fail "TLS 入口非 200: $body_code"
log "    secret 入库加密、无 reveal、app create 绑定 refs、hostname→TLS→VM 200 OK"

MACHINE_ID=$(pg "SELECT id FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' LIMIT 1")
EXECUTION_ID=$(pg "SELECT current_execution_id FROM machines WHERE id='$MACHINE_ID'")

log "B) 下发链路审计：operations.request / 本地状态文件零明文"
op_req=$(pg "SELECT request FROM operations WHERE machine_id='$MACHINE_ID' AND kind='create' ORDER BY created_at DESC LIMIT 1")
echo "$op_req" | grep -q 'secret_env\|s3cr3t' && fail "operations.request 泄露 secret_env"
leak_hits=$(grep -rl "s3cr3t-PASS" /var/lib/firepaas-p0/agent/*.json 2>/dev/null | head -1 || true)
[[ -z "$leak_hits" ]] || fail "agent 状态文件出现明文: $leak_hits"
if [[ -f "/var/lib/firepaas-p0/hypeman/agent/credentials.json" ]]; then
  grep -q '"digest"' "/var/lib/firepaas-p0/hypeman/agent/credentials.json" || fail "credentials.json 应存摘要"
  # P3-12：路径与内容双断言（agentd creds 落在 data_dir/agent 下；
  # 原先检 /var/lib/firepaas-p0/agent/ 永远不存在，断言被静默跳过）。
else
  fail "credentials.json 缺失（FIREPAAS_AGENT_CREDS_PATH 或 data_dir 配置有误）"
fi
creds_mode=$(stat -c '%a' /var/lib/firepaas-p0/hypeman/agent/credentials.json)
[[ "$creds_mode" == "600" ]] || fail "credentials.json 权限应为 600，got $creds_mode"

log "C) execution-bound credential 正/负路径（ADR-0006 收口）"
probe() { # probe <machine> <execution> <credential|->
  local m=$1 e=$2 cred=$3
  local args=(--cert "$CERT_DIR/edge.crt" --key "$CERT_DIR/edge.key" --cacert "$CERT_DIR/ca.crt"
    -s -m 8 -o /dev/null -w '%{http_code}'
    -H "X-Firepaas-Machine-ID: $m" -H "X-Firepaas-Execution-ID: $e")
  [[ "$cred" != "-" ]] && args+=(-H "X-Firepaas-Credential: $cred")
  curl "${args[@]}" https://127.0.0.1:5107/
}
tt=$(authed_curl "http://127.0.0.1:$API_PORT/v1/machines/$MACHINE_ID/traffic-token")
TOKEN_VALUE=$(echo "$tt" | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
TT_EXEC=$(echo "$tt" | python3 -c 'import json,sys;print(json.load(sys.stdin)["execution_id"])')
[[ "$TT_EXEC" == "$EXECUTION_ID" ]] || fail "traffic-token execution 不一致"
noauth=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$API_PORT/v1/machines/$MACHINE_ID/traffic-token")
[[ "$noauth" == "401" ]] || fail "token 端点未认证应 401 got $noauth"

r1=$(probe "$MACHINE_ID" "$EXECUTION_ID" "-");           [[ "$r1" == "403" ]] || fail "无凭证应 403 got $r1"
r2=$(probe "$MACHINE_ID" "$EXECUTION_ID" "tampered");    [[ "$r2" == "403" ]] || fail "错凭证应 403 got $r2"
r3=$(probe "$MACHINE_ID" "exec-forged" "$TOKEN_VALUE");  [[ "$r3" == "403" ]] || fail "跨 execution 应 403 got $r3"
r4=$(probe "$MACHINE_ID" "$EXECUTION_ID" "$TOKEN_VALUE");[[ "$r4" == "200" ]] || fail "正确凭证应 200 got $r4"
log "    403/403/403 + 200（direct agent proxy, edge mTLS 身份）OK"

log "D) secret 轮转 → 触发新 deployment rollout → 旧代 drain 回收"
curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X POST "http://127.0.0.1:$API_PORT/v1/secrets" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"dev\",\"name\":\"db-password\",\"value\":\"rotated-$RUN_ID\",\"created_by\":\"e2e\"}"
dep=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /tmp/m4-deploy.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps/$APP/deployments" -H 'Content-Type: application/json' -d '{}')
[[ "$dep" == "202" ]] || fail "deploy: $dep $(cat /tmp/m4-deploy.json)"
rollout="PENDING"
log "    rollout 开始"
for _ in $(seq 1 90); do
  rollout=$(pg "SELECT status FROM rollouts WHERE app_id='$APP' ORDER BY started_at DESC LIMIT 1")
  [[ "$rollout" == "COMPLETE" ]] && break
  sleep 2
done
log "    rollout=$rollout"
[[ "$rollout" == "COMPLETE" ]] || { tail -20 "$RUN_DIR/m4-api.log"; fail "rollout=$rollout"; }
# 切流后旧代还有 drain 期限（10s）：轮询收敛而不是立即断言。
conv=""
for _ in $(seq 1 40); do
  code=$(edge_curl -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/" || true)
  new_gen_dep=$(pg "SELECT count(*) FROM deployments WHERE app_id='$APP' AND status='ACTIVE'")
  old_gone=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND desired_state!='DELETED'")
  if [[ "$code" == "200" && "$new_gen_dep" == "1" && "$old_gone" == "1" ]]; then conv=1; break; fi
  sleep 3
done
[[ "$conv" == "1" ]] || fail "发布后未收敛 200/dep=1/machines=1（code=$code dep=$new_gen_dep machines=$old_gone）"
log "    rollout COMPLETE、单 ACTIVE deployment、单副本、200 OK"

log "E) 限流 + :80 跳转"
# 注意：wait 必须精确到 curl 的 PID（裸 wait 会连 nohup 的 API/edge 一起等，
# 永不返回——M3 同款教训）。
flood() {
  local n=${1:-300} pids=""
  for i in $(seq 1 "$n"); do
    edge_curl -o /dev/null -w '%{http_code}\n' "https://$HN:$EDGE_TLS/" &
    pids="$pids $!"
  done
  wait $pids || true
}
flood 260 > /tmp/m4-flood.txt
sleep 1
grep -q "429" /tmp/m4-flood.txt || fail "限流未触发 429"
log "    429 已观测（burst 上限生效）"
log "    429 观测到（burst 上限生效）"
redirect=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$EDGE_HTTP/")
[[ "$redirect" == "308" ]] || fail ":80 应 308 got $redirect"
healthz=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$EDGE_HTTP/healthz")
[[ "$healthz" == "200" ]] || fail ":80 /healthz 应保留 200 got $healthz"
log "    :80 → 308 https（/healthz 明文探针保留）OK"

log "F) Redis 宕机注入：serve-stale 窗口内继续服务，恢复后投影回源"
timeout 30 docker stop dev-redis-1 >/dev/null || fail "docker stop dev-redis-1 卡死（>30s）"
code_fresh=$(edge_curl -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/")
[[ "$code_fresh" == "200" ]] || fail "宕机窗口即失败: $code_fresh"
sleep 7   # 越过 fresh TTL(5s)：此后命中必须是 last-known-good stale
stale_hdr=$(edge_curl -D - -o /dev/null "https://$HN:$EDGE_TLS/" | grep -ci '^x-firepaas-stale:' || true)
[[ "$stale_hdr" == "1" ]] || fail "fresh 窗口外未见 X-Firepaas-Stale 头（未走 stale 或实现失效）"
code_stale=$(edge_curl -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/")
[[ "$code_stale" == "200" ]] || fail "stale 服务非 200: $code_stale"
# P2-10：宕机窗口内发布操作不得悬挂——deploy 是纯 PG 事务（Redis 不在
# 关键路径），应正常 202 受理；rollout 的投影发布随 controller 重试，
# Redis 恢复后自然收敛（不丢操作、不悬挂）。断言三件事：
#   1) deploy 受理非 5xx；
#   2) 不产生悬挂的 PREPARING（超时前有明确的推进路径）；
#   3) Redis 恢复后 rollout 最终 COMPLETE（见 F 段尾部的收敛轮询）。
dep_down=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $API_TOKEN" \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps/$APP/deployments" -H 'Content-Type: application/json' \
  -d '{"env":{"WINDOW":"redis-down"}}' || true)
[[ "$dep_down" != "5"* ]] || fail "Redis 宕机期间 deploy 不应 5xx（PG 事务受理）: $dep_down"
log "    Redis down +7s 数据面持续 200（X-Firepaas-Stale 命中 last-known-good）；deploy 受理（$dep_down）不悬挂"

timeout 30 docker start dev-redis-1 >/dev/null || fail "docker start dev-redis-1 卡死（>30s）"
for _ in $(seq 1 20); do
  [[ "$(docker exec dev-redis-1 redis-cli ping 2>/dev/null || true)" == "PONG" ]] && break
  sleep 1
done
# 宕机窗口受理的发布不得悬挂：rollout 必须在恢复后收敛 COMPLETE。
# （先收敛再 FLUSHALL：切流窗口与新代 readiness 会让数据面短暂非 200，
# 不能与投影重建断言叠加在同一窗口里。）
for _ in $(seq 1 60); do
  r_status=$(pg "SELECT status FROM rollouts WHERE app_id='$APP' ORDER BY started_at DESC LIMIT 1")
  [[ "$r_status" == "COMPLETE" ]] && break
  sleep 3
done
[[ "$r_status" == "COMPLETE" ]] || fail "宕机窗口受理的发布未收敛（rollout=$r_status）"
# 切流后旧代 drain：等待到单副本新代稳定服务。
for _ in $(seq 1 40); do
  code=$(edge_curl -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/" || true)
  alive=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND desired_state!='DELETED'")
  [[ "$code" == "200" && "$alive" == "1" ]] && break
  sleep 3
done
[[ "$code" == "200" ]] || fail "宕机窗口发布的切流未收敛 200（code=$code）"
log "    宕机窗口受理的 rollout 已收敛 COMPLETE + 切流稳定（发布不悬挂）OK"

# P2-10（续）：FLUSHALL 真测投影重建（AOF 下 stop/start 不丢数据，旧实现
# 未真正验证重建路径）。清空后：1) 短暂非 200（权威 miss）；2) ≤75s 重建。
docker exec dev-redis-1 redis-cli FLUSHALL >/dev/null
purged=0
for _ in $(seq 1 10); do
  c=$(edge_curl -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/" || true)
  [[ "$c" != "200" ]] && purged=1 && break
  sleep 1
done
[[ "$purged" == "1" ]] || log "    （警告）FLUSHALL 后数据面未出现瞬时非 200（投影可能在轮询间隙重建）"
T0=$(date +%s)
recovered=0
for _ in $(seq 1 25); do
  code=$(edge_curl -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/" || true)
  if [[ "$code" == "200" ]]; then
    # 确认是重建后的新键而非 stale（stale 响应带 X-Firepaas-Stale 头）。
    hdr=$(edge_curl -D - -o /dev/null "https://$HN:$EDGE_TLS/" | grep -ci '^x-firepaas-stale:' || true)
    [[ "$hdr" == "0" ]] && recovered=1 && break
  fi
  sleep 3
done
T1=$(date +%s)
[[ "$recovered" == "1" ]] || fail "FLUSHALL 后投影未在时限内重建（75s）"
[[ $((T1 - T0)) -le 75 ]] || fail "投影重建耗时 $((T1-T0))s 超过 75s 时限"
log "    FLUSHALL → 投影重建 → 回源 200（$((T1-T0))s，≤75s 时限内）OK"

log "G) 清理 + 删除后凭证撤销 + 终态泄漏检查"
del=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP")
[[ "$del" =~ ^(200|202|204)$ ]] || fail "delete app HTTP $del"
for _ in $(seq 1 60); do
  fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
  [[ "$fc" == "0" ]] && break
  sleep 5
done
revoked=$(probe "$MACHINE_ID" "$EXECUTION_ID" "$TOKEN_VALUE")
[[ "$revoked" == "403" || "$revoked" == "502" ]] || fail "删除后旧凭证未撤销（got $revoked）"
log "    删除后旧 execution 凭证 fail-closed ($revoked) OK"

for _ in $(seq 1 60); do
  fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
  ns=$(ip netns list | grep -c '^fp-slot-' || true)
  veth=$(ip link | grep -c 'fp-vp' || true)
  pending=$(pg "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
  alive=$(pg "SELECT count(*) FROM machines WHERE desired_state!='DELETED'")
  routes=$(ip route show | grep '^10\.100\.' | grep -cv 'dev firepaas0' || true)
  [[ "$fc" == "0" && "$ns" == "0" && "$veth" == "0" && "$routes" == "0" && "$pending" == "0" && "$alive" == "0" ]] && break
  sleep 5
done
[[ "$fc" == "0" && "$ns" == "0" && "$veth" == "0" && "$routes" == "0" ]] || fail "内核对象泄漏 fc=$fc ns=$ns vp=$veth route=$routes"
[[ "$pending" == "0" && "$alive" == "0" ]] || fail "控制面残留 pending=$pending alive=$alive"

log "H) scale-to-zero：pause/resume 50 循环 + autoresume SLO + 无泄漏"
# 重新建一台 app 机器（G 段已清空）。
APP2="app-m4z-$RUN_ID"
HN2="$APP2.$DOMAIN"
cs=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /tmp/m4-app2.json -w '%{http_code}' \
	-X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
	-d "{\"app_id\":\"$APP2\",\"project_id\":\"dev\",\"hostname\":\"$HN2\",
	     \"image\":\"docker.m.daocloud.io/library/nginx:alpine\",\"port\":80,\"replicas\":1}")
[[ "$cs" == "201" ]] || fail "app2 create: $cs $(cat /tmp/m4-app2.json)"
for _ in $(seq 1 75); do
  running=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP2' AND observed_state='RUNNING' AND desired_state!='DELETED'")
  [[ "$running" == "1" ]] && break
  sleep 2
done
M2=$(pg "SELECT id FROM machines WHERE app_id='$APP2' AND desired_state!='DELETED' LIMIT 1")
E2=$(pg "SELECT current_execution_id FROM machines WHERE id='$M2'")

# 取该机器 credential（edge token 端点），构造过 agent proxy 的请求。
tt2=$(authed_curl "http://127.0.0.1:$API_PORT/v1/machines/$M2/traffic-token")
TV2=$(echo "$tt2" | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
probe2() { curl -s --cert "$CERT_DIR/edge.crt" --key "$CERT_DIR/edge.key" \
	--cacert "$CERT_DIR/ca.crt" -m "$1" -o /dev/null -w '%{http_code}' \
	-H "X-Firepaas-Machine-ID: $M2" -H "X-Firepaas-Execution-ID: $E2" \
	-H "X-Firepaas-Credential: $TV2" https://127.0.0.1:5107/; }
[[ "$(probe2 8)" == "200" ]] || fail "pre-pause probe failed"

# 基线对象计数。
base_ns=$(ip netns list | grep -c '^fp-slot-' || true)
base_fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)

for i in $(seq 1 50); do
	ps=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' \
		-X POST "http://127.0.0.1:$API_PORT/v1/machines/$M2/pause")
	[[ "$ps" == "202" ]] || fail "cycle $i pause enqueue: $ps"
	for _ in $(seq 1 30); do
		pst=$(pg "SELECT observed_state FROM machines WHERE id='$M2'")
		[[ "$pst" == "PAUSED" ]] && break
		sleep 1
	done
	[[ "$(pg "SELECT observed_state FROM machines WHERE id='$M2'")" == "PAUSED" ]] || fail "cycle $i not PAUSED"

	rs=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' \
		-X POST "http://127.0.0.1:$API_PORT/v1/machines/$M2/resume")
	[[ "$rs" == "202" ]] || fail "cycle $i resume enqueue: $rs"
	for _ in $(seq 1 30); do
		rst=$(pg "SELECT observed_state FROM machines WHERE id='$M2'")
		[[ "$rst" == "RUNNING" ]] && break
		sleep 1
	done
	[[ "$(pg "SELECT observed_state FROM machines WHERE id='$M2'")" == "RUNNING" ]] || fail "cycle $i not RUNNING back"
done
log "    50 次 pause/resume 状态机循环 OK"

# 内核对象无漂移（standby 删 VMM：fc 允许在 paused 瞬间减一，终态必须回基线）。
end_ns=$(ip netns list | grep -c '^fp-slot-' || true)
end_fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
[[ "$end_ns" == "$base_ns" ]] || fail "netns 漂移: $base_ns → $end_ns"
[[ "$end_fc" == "$base_fc" ]] || fail "firecracker 漂移: $base_fc → $end_fc"
snap_n=$(ls /var/lib/firepaas-p0/hypeman/guests/*/snapshots 2>/dev/null | wc -l || echo 0)
log "    内核对象无漂移（ns=$end_ns fc=$end_fc），快照存在=$snap_n OK"

# autoresume：<8s SLO（含 HTTP 往返；M0 restore p95≈95ms+guest 启动）。
psx=$(curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' \
	-X POST "http://127.0.0.1:$API_PORT/v1/machines/$M2/pause")
[[ "$psx" == "202" ]] || fail "final pause: $psx"
sleep 3
t0=$(date +%s%N)
code_wake=$(probe2 8)
t1=$(date +%s%N)
wake_ms=$(( (t1 - t0) / 1000000 ))
[[ "$code_wake" == "200" ]] || fail "autoresume probe: $code_wake"
[[ "$wake_ms" -lt 8000 ]] || fail "autoresume ${wake_ms}ms ≥ 8s SLO"
log "    proxy 首流量同步唤醒 ${wake_ms}ms（<8s）OK"

# 清理 app2。
curl -s -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP2"

log "M4 e2e PASS：secrets v1 / credential 正负路径与撤销 / TLS 入口 / 限流跳转 / serve-stale / scale-to-zero(50 循环+autoresume SLO) / 零泄漏 全部通过"
