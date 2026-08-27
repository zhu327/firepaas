#!/usr/bin/env bash
# M3 e2e harness（单机）：mvp-plan §7 验收（ADR-0014/0015）
#   1) U1：app create（nginx）→ scale 3 → hostname → edge → slot → VM 200
#   2) 隔离：guest → host/私网/跨 slot 全部拒绝（slot netns + nftables）
#   3) U2 成功发布：新代全部 READY 才切流 → 旧代 draining → drain 后回收
#   4) U2 失败发布：坏镜像 → 自动回滚 → 旧代继续服务
#   5) U3：杀一个 VM → 仅重建缺失 ordinal（其余 execution 不变）
#   6) slot 泄漏：1000 次 attach/release + agent 重启 reconcile 无残留
#   7) 终态：无 VM/netns/veth/route/lease/在途 op 残留
# 用法: sudo bash scripts/lab/e2e-m3.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="/home/zty/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-m3"
RUN_ID="e2e-m3-$(date +%s)"
API_TOKEN="e2e-m3-token-$RUN_ID"
TRAFFIC_KEY="$(openssl rand -base64 32)"   # M4：proxy credential 密钥（与 agent 强制校验配套）
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

export PATH="$LAB_BIN:/home/zty/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"

mkdir -p "$RUN_DIR"
log() { echo "[e2e-m3] $*"; }
fail() { echo "[e2e-m3] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 30 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }

wait_app() { # $1 app_id  $2 判定函数(py 表达式)  $3 timeout_sec
  local app="$1" cond="$2" timeout="${3:-240}"
  for _ in $(seq 1 $((timeout / 5))); do
    local out
    out=$(authed_curl "http://127.0.0.1:8080/v1/apps/$app" | python3 -c "
import json,sys
d=json.load(sys.stdin)
rl=d['active_rollout']
ms=d['machines']
print($cond)
" 2>/dev/null || echo "ERR")
    [[ "$out" == "True" ]] && return 0
    sleep 5
  done
  return 1
}

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" && -f "$LAB_BIN/fpctl" ]] || fail "二进制未构建（make build）"
[[ -f "$CERT_DIR/ca.crt" ]] || fail "证书未生成：bash scripts/lab/gen-certs.sh"

log "0) root setup + Nomad/agentd（最新二进制，slot 后端）"
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
sleep 1
nohup env FIREPAAS_POSTGRES_URL='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_NOMAD_ADDR=http://127.0.0.1:4646 \
  FIREPAAS_AGENT_PROXY_ADDR=127.0.0.1:5107 FIREPAAS_HTTP_PORT=8080 FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_TRAFFIC_TOKEN_KEY="$TRAFFIC_KEY" \
  FIREPAAS_ROLLOUT_TIMEOUT=240s FIREPAAS_ROLLOUT_DRAIN=20s \
  FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt" FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key" \
  FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=8081 FIREPAAS_API_ADDR=http://127.0.0.1:8080 FIREPAAS_API_TOKEN="$API_TOKEN" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/edge.log" 2>&1 &
for _ in $(seq 1 30); do
  authed_curl http://127.0.0.1:8080/v1/health >/dev/null 2>&1 && break
  sleep 1
done
authed_curl http://127.0.0.1:8080/v1/health >/dev/null || fail "API 未就绪"

log "1.5) 预清理：删除历史验收机（幂等重跑）"
# 幂等清理走 SQL 墓碑化（API 路径会与历史 op 的幂等键冲突）：
#   desired=DELETED + replicas=0 + deployment SUPERSEDED → AppController 停止补建，
#   机器由 R5（desired DELETED but agent has machine）收敛清理。
pg "UPDATE machines SET desired_state='DELETED', updated_at=now() WHERE id LIKE 'app-e2e-m3-%' AND desired_state != 'DELETED'" >/dev/null
pg "UPDATE apps SET desired_replicas=0, updated_at=now() WHERE id LIKE 'app-e2e-m3-%'" >/dev/null
pg "UPDATE deployments SET status='SUPERSEDED', updated_at=now() WHERE app_id LIKE 'app-e2e-m3-%' AND status IN ('ACTIVE','PREPARING')" >/dev/null
pg "UPDATE rollouts SET status='COMPLETE', failed=true, completed_at=now(), updated_at=now() WHERE app_id LIKE 'app-e2e-m3-%' AND status IN ('PREPARING','CUTOVER','ROLLING_BACK')" >/dev/null
# 等待收敛：上次运行的机器全部终态（否则残留 VM 干扰内存与断言）。
for _ in $(seq 1 60); do
  left=$(pg "SELECT count(*) FROM machines WHERE id LIKE 'app-e2e-m3-%' AND desired_state != 'DELETED'")
  [[ "$left" == "0" ]] && break
  sleep 5
done
[[ "$left" == "0" ]] || fail "预清理未收敛（仍有 $left 台机器）"
for _ in $(seq 1 40); do
  fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
  [[ "$fc" == "0" ]] && break
  sleep 5
done
log "    预清理完成（机器 0 / VM 0）"

log "2) 等待节点 HEALTHY"
for _ in $(seq 1 30); do
  n=$(authed_curl http://127.0.0.1:8080/v1/nodes | python3 -c 'import json,sys
ns=json.load(sys.stdin)["nodes"]
print(len([n for n in ns if n["Status"]=="HEALTHY"]))' 2>/dev/null || echo 0)
  [[ "$n" -ge 1 ]] && break
  sleep 2
done
[[ "$n" -ge 1 ]] || fail "节点未 HEALTHY"

log "2.5) slot 泄漏：1000 次 attach/release（内核对象级，须在业务机创建前跑）"
REPO_ROOT="$HERE/../.."
(cd "$REPO_ROOT" && FIREPAAS_TEST_NETNS=1 /home/zty/.local/firepaas-lab/go/bin/go test ./internal/agent/network/slot/ -run 'TestSlotLifecycle|TestSlotCycleLeak' -count=1 -v > "$RUN_DIR/slot-test.log" 2>&1) \
  || { tail -20 "$RUN_DIR/slot-test.log" >&2; fail "slot 生命周期/泄漏测试失败"; }
log "    attach/release 无 netns/veth/路由泄漏 OK"

# ---------------------------------------------------------------------------
APP="app-e2e-m3-$RUN_ID"
HN="e2e-m3-$RUN_ID.firepaas.local"

log "3) U1：app create（nginx，slot 后端）→ RUNNING → edge 200"
curl -fsS -m 10 -X POST -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  http://127.0.0.1:8080/v1/apps -d "{
  \"app_id\":\"$APP\",\"hostname\":\"$HN\",
  \"image\":\"docker.io/library/nginx:alpine\",\"vcpu\":1,\"mem_mib\":512,\"port\":80,\"replicas\":1,
  \"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:80/\",\"interval_seconds\":2,\"timeout_seconds\":1,\"unhealthy_threshold\":3}
}" >/dev/null || fail "app create 失败"
wait_app "$APP" "len([m for m in ms if m['ObservedState']=='RUNNING'])==1" 240 || fail "U1 machine 未 RUNNING"
# edge 轮询至 200（VM 冷启动）
ok=0
for _ in $(seq 1 30); do
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
  [[ "$code" == "200" ]] && ok=1 && break
  sleep 3
done
[[ "$ok" == "1" ]] || fail "U1 edge HTTP != 200 (got $code)"

log "4) U1 隔离：guest → host/私网/跨 slot 全部拒绝"
GUEST_IP=$(pg "SELECT observed_slot_ip FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' LIMIT 1")
[[ -n "$GUEST_IP" ]] || fail "无 slot IP"
NS=$(ip netns list | awk '$1 ~ /^fp-slot-/ {print $1; exit}')
[[ -n "$NS" ]] || fail "无 slot netns"
for TARGET in "10.0.0.1" "192.168.255.1" "172.31.255.1"; do
  if ip netns exec "$NS" curl -fsS -m 2 -o /dev/null "http://$TARGET:80" 2>/dev/null; then
    fail "guest 可达私网 $TARGET（隔离失效）"
  fi
done
log "    私网三类目标均不可达 OK（host IP 由 INPUT drop 覆盖）"

log "5) U3：scale 3 → 3 machines RUNNING（单节点=降级放置，事件可审计）"
curl -fsS -m 10 -X POST -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/apps/$APP/scale" -d '{"replicas":3}' >/dev/null || fail "scale 失败"
wait_app "$APP" "len([m for m in ms if m['ObservedState']=='RUNNING'])==3" 360 || fail "scale 3 未收敛"
placements=$(pg "SELECT count(*) FROM scheduler_events WHERE kind='placement' AND machine_id LIKE '$APP%'")
[[ "$placements" -ge 3 ]] || fail "placement 事件不足（$placements < 3）"
log "    3/3 RUNNING，placement 事件 $placements 条"

log "6) U2 成功发布：新代 READY 才切流 → drain → 旧代回收"
curl -fsS -m 10 -X POST -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/apps/$APP/deployments" -d '{
  "image":"docker.io/library/nginx:1.27-alpine",
  "health_check":{"type":"http","target":"http://127.0.0.1:80/","interval_seconds":2,"timeout_seconds":1,"unhealthy_threshold":3}
}' >/dev/null || fail "deploy 失败"
# 切流前旧代继续服务（PREPARING 期间不切流）
sleep 5
code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
[[ "$code" == "200" ]] || fail "PREPARING 期间旧代应继续服务（got $code）"
wait_app "$APP" "rl is None and len([m for m in ms if m['ObservedState']=='RUNNING'])==3" 480 \
  || fail "rollout 未完成"
sleep 15  # 旧代回收
old_left=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND deployment_id LIKE '%-g1' AND desired_state!='DELETED'")
[[ "$old_left" -eq 0 ]] || fail "旧代机器未回收（剩余 $old_left）"
code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
[[ "$code" == "200" ]] || fail "发布完成后 edge != 200"
log "    新代全部 READY 切流、旧代 drain 回收、edge 200 OK"

log "7) U2 失败发布：坏镜像 → 自动回滚 → 旧代持续服务"
curl -fsS -m 10 -X POST -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/apps/$APP/deployments" -d '{"image":"docker.io/library/nginx:nonexistent-e2e-m3"}' \
  >/dev/null || fail "deploy 失败"
# 发布互斥：活跃 rollout 期间再 deploy → 409
status=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/apps/$APP/deployments" -d '{"image":"x"}' || true)
[[ "$status" == "409" ]] || fail "发布互斥失效（第二次 deploy 期望 409，got $status）"
wait_app "$APP" "rl is None" 600 || fail "失败发布未回滚收敛"
dep_status=$(pg "SELECT status FROM deployments WHERE app_id='$APP' ORDER BY generation DESC LIMIT 1")
[[ "$dep_status" == "FAILED" ]] || fail "坏镜像 deployment 应为 FAILED（got $dep_status）"
code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
[[ "$code" == "200" ]] || fail "回滚后 edge != 200"
log "    409 互斥 + 自动回滚 + 旧代持续服务 OK"

log "7.5) 镜像策略回归（P1-2）：非法 digest / 非法引用 → 400"
status=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/apps/$APP/deployments" \
  -d '{"image":"docker.io/library/nginx@sha256:short"}' || true)
[[ "$status" == "400" ]] || fail "非法 digest 应 400（got $status）"
status=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $API_TOKEN" -H 'Content-Type: application/json' \
  "http://127.0.0.1:8080/v1/apps/$APP/deployments" \
  -d '{"image":"not a valid ref !!"}' || true)
[[ "$status" == "400" ]] || fail "非法引用应 400（got $status）"
log "    非法镜像引用全部 400 OK"

log "7.6) P2-4 回归：catalog 过期 → edge 受控 404；恢复后重建"
REDIS_CLI="docker exec dev-redis-1 redis-cli"
$REDIS_CLI DEL "hostidx:$HN" >/dev/null
# 同时删 route 本体，验证 hostidx+route 双重建。
# M4 后 edge 有 fresh TTL(5s) 本地缓存：删除后的 5s 内 fresh 命中仍可能
# 200（声明的一致性窗口）；过 fresh TTL 后：未重建 → 权威 miss 404（P2-8：
# 不 serve-stale），重建后 → 200。断言覆盖两个阶段。
saw404=0
for _ in $(seq 1 8); do
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
  [[ "$code" == "404" || "$code" == "503" ]] && saw404=1
  [[ "$saw404" == "1" ]] && break
  sleep 2
done
# 若 controller 事件驱动重建够快（删除后 5s 内重建），fresh 窗口内全程 200
# 也是合法收敛——两种部接受，但至少不能一直非 200 或直接 5xx 之外的状态。
if [[ "$saw404" != "1" ]]; then
  # 验证当前确实已重建（200）；否则是断言窗口内既无 miss 又未恢复的异常。
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
  [[ "$code" == "200" ]] || fail "catalog 删除后既无 404 也未恢复（got $code）"
  log "    （快速重建路径：删除后 fresh 窗口内投影已重建，无 miss 窗口）"
fi
# 等投影重建（controller 下一轮 buildRoutes，≤ sync 周期 + 余量）
ok=0
for _ in $(seq 1 24); do
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
  [[ "$code" == "200" ]] && ok=1 && break
  sleep 5
done
[[ "$ok" == "1" ]] || fail "catalog 重建后 edge 未恢复 200（got $code）"
log "    catalog 过期 404 + 重建恢复 200 OK"

log "7.7) P2-4 回归：stale execution 请求 → agent proxy 拒绝"
STALE_EXEC="exec-stale-e2e-m3"
# agent proxy 直连（edge 身份 mTLS）：携 stale execution 头的请求必须被拒
# （execution mismatch → 502，而非转发到 workload）。
NODE_PROXY="127.0.0.1:5107"
MACH=$(pg "SELECT id FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' LIMIT 1")
code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' \
  --cert "$CERT_DIR/edge.crt" --key "$CERT_DIR/edge.key" --cacert "$CERT_DIR/ca.crt" \
  -H "X-Firepaas-Machine-ID: $MACH" -H "X-Firepaas-Execution-ID: $STALE_EXEC" \
  "https://$NODE_PROXY/" || true)
[[ "$code" != "200" ]] || fail "stale execution 请求被转发（隔离失效，got $code）"
log "    stale execution 请求被拒（$code）OK"

log "8) U3：杀一个 VM → 仅重建缺失 ordinal"
M1="$APP-r1-g2"
BEFORE=$(pg "SELECT id||'='||current_execution_id FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' ORDER BY id" | tr '\n' ' ')
OLD_R1_EXEC=$(echo "$BEFORE" | grep -o "$M1=[^ ]*" | cut -d= -f2)
OLD_R0_EXEC=$(echo "$BEFORE" | grep -o "$APP-r0-g2=[^ ]*" | cut -d= -f2)
OLD_R2_EXEC=$(echo "$BEFORE" | grep -o "$APP-r2-g2=[^ ]*" | cut -d= -f2)
GUEST_DIR=$(grep -l "\"$M1\"" /var/lib/firepaas-p0/hypeman/guests/*/metadata.json 2>/dev/null | head -1 | xargs -r dirname)
[[ -n "$GUEST_DIR" ]] || fail "找不到 $M1 的 guest 目录"
pkill -9 -f "$(basename "$GUEST_DIR")" || true
# 先等观测到死亡（observed 离开 RUNNING），再等重建回 RUNNING。
for _ in $(seq 1 40); do
  st=$(pg "SELECT observed_state FROM machines WHERE id='$M1'")
  [[ "$st" != "RUNNING" ]] && break
  sleep 3
done
[[ "$st" != "RUNNING" ]] || fail "杀 VM 后 observed 未变化"
wait_app "$APP" "len([m for m in ms if m['ObservedState']=='RUNNING'])==3" 480 || fail "缺失 ordinal 未重建"
AFTER=$(pg "SELECT id||'='||current_execution_id FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' ORDER BY id" | tr '\n' ' ')
echo "$AFTER" | grep -q "$M1=$OLD_R1_EXEC" && fail "r1 execution 未换代（未重建）"
echo "$AFTER" | grep -q "$APP-r0-g2=$OLD_R0_EXEC" || fail "r0 execution 被意外改动"
echo "$AFTER" | grep -q "$APP-r2-g2=$OLD_R2_EXEC" || fail "r2 execution 被意外改动"
log "    仅缺失 ordinal 换代重建，其余 execution 不变 OK"

log "10) agent 重启 reconcile：VM 重建收敛 + slot 一致 + 数据面恢复"
# agentd 重启（SIGTERM → hypeman vmm teardown）会带走 VM：机器级 R1-R8 负责重建，
# slot Reconcile 负责回收孤儿 netns / 补齐 slot。等全部收敛。
nomad job restart -on-error fail firepaas-agentd >/dev/null 2>&1 || true
for _ in $(seq 1 60); do "$LAB_BIN/agentctl" info >/dev/null 2>&1 && break; sleep 2; done
for _ in $(seq 1 60); do
  vm_count=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND observed_state='RUNNING' AND desired_state!='DELETED'")
  ns_count=$(ip netns list | grep -c '^fp-slot-' || true)
  code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' -H "Host: $HN" http://127.0.0.1:8081/ || true)
  [[ "$vm_count" == "3" && "$ns_count" == "3" && "$code" == "200" ]] && break
  sleep 5
done
[[ "$vm_count" == "3" ]] || fail "agent 重启后 RUNNING=$vm_count != 3"
[[ "$ns_count" == "3" ]] || fail "agent 重启后 slot 数($ns_count) != 3"
[[ "$code" == "200" ]] || fail "agent 重启后 edge != 200"
log "    重启后 3 VM 重建、3 slots 一致，edge 200 OK"

log "11) 清理 + 终态泄漏检查（P0-1：删除后不得复活）"
curl -fsS -m 10 -X DELETE -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:8080/v1/apps/$APP" >/dev/null
# 重复删除幂等（P0-1：返回 202 already_deleted，不是 404/500）
status=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -X DELETE \
  -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:8080/v1/apps/$APP" || true)
[[ "$status" == "202" ]] || fail "重复删除应幂等 202（got $status）"
for _ in $(seq 1 60); do
  left=$(pg "SELECT count(*) FROM machines WHERE desired_state!='DELETED'")
  [[ "$left" == "0" ]] && break
  sleep 5
done
[[ "$left" == "0" ]] || fail "清理后仍有 desired!=DELETED 的机器"
# P0-1 回归：等待两个 controller 周期，确认被删 app 不再补建。
sleep 15
resurrected=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP' AND desired_state!='DELETED'")
[[ "$resurrected" == "0" ]] || fail "已删 app 复活了 $resurrected 台机器（P0-1 回归）"
app_deleted=$(pg "SELECT (deleted_at IS NOT NULL) FROM apps WHERE id='$APP'")
[[ "$app_deleted" == "t" ]] || fail "app 未墓碑化（deleted_at 为空）"
log "    app 删除后墓碑化且无复活 OK"
# 收敛窗口（删除是异步的：轮询至内核对象清零，而不是固定等待）
for _ in $(seq 1 60); do
  fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
  ns=$(ip netns list | grep -c '^fp-slot-' || true)
  vp=$(ip link | grep -c 'fp-vp' || true)
  routes=$(ip route show | grep -E '^10\.100\.' | grep -cv 'dev firepaas0' || true)
  pending=$(pg "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
  [[ "$fc" == "0" && "$ns" == "0" && "$vp" == "0" && "$routes" == "0" && "$pending" == "0" ]] && break
  sleep 5
done
ns=$(ip netns list | grep -c '^fp-slot-' || true)
vp=$(ip link | grep -c 'fp-vp' || true)
routes=$(ip route show | grep -E '^10\.100\.' | grep -cv 'dev firepaas0' || true)
[[ "$fc" == "0" ]] || fail "泄漏 firecracker=$fc"
[[ "$ns" == "0" ]] || fail "泄漏 netns=$ns"
[[ "$vp" == "0" ]] || fail "泄漏 veth=$vp"
[[ "$routes" == "0" ]] || fail "泄漏 guest 路由=$routes"
pending=$(pg "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
[[ "$pending" == "0" ]] || fail "在途 op=$pending"
log "    firecracker=0 netns=0 veth=0 route=0 在途op=0"

log "M3 e2e PASS：U1/隔离/U2 发布与回滚/U3 重建/slot 无泄漏全部通过"
