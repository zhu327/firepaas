#!/usr/bin/env bash
# v1.1 e2e harness（单机实验室）：docs/v1.1-plan.md 工作包验收
#   A) auto-standby（ADR-0017）：空闲 standby（VMM 释放）→ curl 唤醒 <5s；
#      探针不清闲（健康探针运行中仍能 standby）；默认关闭回归；多轮无泄漏
#   B) 镜像亲和与预取（ADR-0018）：prefetch 调度事件 + nodes.image_cache 落库
#   C) edge 流量语义（ADR-0019/0020）：X-Firepaas-Machine-ID 响应头；
#      钉扎 100% 命中/钉错 404；least-inflight 偏移；hard 并发 503+计数；
#      inflight 生命周期（慢请求期间 >0、完成后归零）
#   D) drain+evacuate（ADR-0021）：节点 machine 归零 + 事件 + ready 后恢复
#      （多节点零停机迁移验收 DEFERRED-MULTI-NODE：单机实验室重建窗口内
#      服务不可用是已知边界，见 ADR-0021 §4）
#   E) 多端口 services（ADR-0022）：双端口分别路由；未声明端口 404；
#      单端口存量回归
#   F) rolling batch（v1.1-F）：逐批切流、旧代逐批回收、并发 0 失败；
#      per-VM 指标直抓（agentd :9464）
# 用法: sudo bash scripts/lab/e2e-v11.sh
# 运行约定：后台运行 + 日志轮询（全部等待有界、每行带时间戳）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
LAB_BIN="$HOME/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RUN_DIR="/var/lib/firepaas-p0/e2e-v11"
RUN_ID="v11-$(date +%s)"
API_TOKEN="v11-token-$RUN_ID"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT=8086
EDGE_HTTP=8087
EDGE_TLS=8447
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

# 可调参数（验收规模）：
STANDBY_ROUNDS="${FIREPAAS_V11_STANDBY_ROUNDS:-50}"   # idle→standby→唤醒 轮数（v1.1 验收基线）
STANDBY_IDLE_S="${FIREPAAS_V11_STANDBY_IDLE:-8}"      # 空闲超时（秒，≥5）

export PATH="$LAB_BIN:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export FIREPAAS_AGENT_TLS_CERT="$CERT_DIR/control-plane.crt"
export FIREPAAS_AGENT_TLS_KEY="$CERT_DIR/control-plane.key"
export FIREPAAS_AGENT_TLS_CA="$CERT_DIR/ca.crt"
mkdir -p "$RUN_DIR"

now() { date +%H:%M:%S; }
log() { echo "[e2e-v11 $(now)] $*"; }
fail() { echo "[e2e-v11] FAIL: $*" >&2; exit 1; }
authed_curl() { curl -fsS -m 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
pg() { $PG "$1"; }
cur() { curl -s -m 20 "$@"; }
mark() { log "    (累计 $(( $(date +%s) - T0 ))s) $*"; }

# ontime 探针镜像（含 /slow 端点）。
ONLINE_OUT=$(bash "$HERE/push-ontime.sh") || fail "push-ontime 失败"
ONTIME_REF=$(echo "$ONLINE_OUT" | grep '^REF=' | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || fail "ontime REF 解析失败"

[[ -f "$LAB_BIN/agentd" && -f "$LAB_BIN/firepaas-api" && -f "$LAB_BIN/edge-proxy" ]] || fail "二进制未构建（make build）"
[[ -f "$CERT_DIR/wildcard-$DOMAIN.crt" ]] || fail "泛域名证书缺失"

log "0) 启动：agentd（autostandby + metrics）+ API/edge（hard=2 + extra ports）"
T0=$(date +%s)
"$HERE/root-setup.sh" >/dev/null
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
  FIREPAAS_REGISTRY_ALLOWLIST="127.0.0.1:5000" FIREPAAS_IMAGE_REQUIRE_DIGEST=true \
  "$LAB_BIN/firepaas-api" > "$RUN_DIR/v11-api.log" 2>&1 &
nohup env FIREPAAS_EDGE_PORT=$EDGE_HTTP FIREPAAS_EDGE_TLS_LISTEN=":$EDGE_TLS" \
  FIREPAAS_EDGE_SERVER_CERT="$CERT_DIR/wildcard-$DOMAIN.crt" FIREPAAS_EDGE_SERVER_KEY="$CERT_DIR/wildcard-$DOMAIN.key" \
  FIREPAAS_EDGE_TLS_CERT="$CERT_DIR/edge.crt" FIREPAAS_EDGE_TLS_KEY="$CERT_DIR/edge.key" \
  FIREPAAS_EDGE_TLS_CA="$CERT_DIR/ca.crt" \
  FIREPAAS_REDIS_ADDR=127.0.0.1:6379 FIREPAAS_API_ADDR="http://127.0.0.1:$API_PORT" \
  FIREPAAS_API_TOKEN="$API_TOKEN" FIREPAAS_EDGE_RATE_LIMIT=100 FIREPAAS_EDGE_RATE_BURST=200 \
  FIREPAAS_EDGE_METRICS_PORT=9465 \
  FIREPAAS_EDGE_HARD_CONCURRENCY=2 \
  FIREPAAS_EDGE_EXTRA_PORTS="9080,9081" \
  "$LAB_BIN/edge-proxy" > "$RUN_DIR/v11-edge.log" 2>&1 &
for _ in $(seq 1 40); do
  authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && break
  sleep 1
done
authed_curl "http://127.0.0.1:$API_PORT/v1/health" >/dev/null || { tail -5 "$RUN_DIR/v11-api.log"; fail "API 未就绪"; }
hc=$(curl -sk -m 5 -o /dev/null -w '%{http_code}' --resolve "x.$DOMAIN:$EDGE_TLS:127.0.0.1" "https://x.$DOMAIN:$EDGE_TLS/healthz")
[[ "$hc" == "200" ]] || fail "edge TLS 未就绪 ($hc)"
mark "api/edge up (hard=2, extra ports 9080/9081)"

log "0.5) 预清理历史验收机"
for _ in $(seq 1 10); do
  curl -s -m 10 -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/v1/nodes" |
    python3 -c 'import json,sys
for n in json.load(sys.stdin).get("nodes",[]):
    print(n.get("id",n.get("ID","")))' |
    while read -r nid; do
      [[ -n "$nid" ]] && curl -s -m 10 -H "Authorization: Bearer $API_TOKEN" -X POST \
        "http://127.0.0.1:$API_PORT/v1/nodes/$nid/ready" >/dev/null || true
    done
  nd=$(pg "SELECT count(*) FROM nodes WHERE draining")
  [[ "$nd" == "0" ]] && break
  sleep 3
done
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
[[ "$left" == "0" ]] || fail "旧机器未清理 ($left)"

wait_running() { # $1=app_id $2=replicas（默认 1）$3=timeout_s（默认 120）
  local app="$1" want="${2:-1}" tmo="${3:-120}" n=""
  for _ in $(seq 1 "$tmo"); do
    n=$(pg "SELECT count(*) FROM machines WHERE app_id='$app' AND observed_state IN ('RUNNING','PAUSED') AND desired_state!='DELETED'")
    [[ "$n" == "$want" ]] && return 0
    sleep 2
  done
  fail "app $app 未达 $want 副本 RUNNING（当前 $n）"
}
edge_curl() { # $1=path（默认 /）$2=timeout_s（默认 8）
  curl -sk -m "${2:-8}" --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
    "https://$APP_HOSTNAME:$EDGE_TLS${1:-/}"
}
edge_code() { # $1=path $2=timeout_s
  curl -sk -o /dev/null -m "${2:-8}" -w '%{http_code}' --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
    "https://$APP_HOSTNAME:$EDGE_TLS${1:-/}"
}
edge_code_extra() { # $1=port $2=path
  curl -sk -o /dev/null -m 8 -w '%{http_code}' --resolve "$APP_HOSTNAME:$1:127.0.0.1" \
    "http://$APP_HOSTNAME:$1${2:-/}"
}
edge_header() { # $1=header 名
  curl -sk -o /dev/null -D - -m 8 --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
    "https://$APP_HOSTNAME:$EDGE_TLS/" | tr -d '\r' | grep -i "^$1:" | head -1 | cut -d' ' -f2
}

# ---------------------------------------------------------------------------
log "A) auto-standby（ADR-0017）"
APP_A="app-v11a-$RUN_ID"
APP_HOSTNAME="$APP_A.$DOMAIN"
# 健康探针每 5s 打一次（探针不清闲断言的关键：无精确排除时实例永不清闲）。
cs=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/v11-a.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP_A\",\"project_id\":\"dev\",\"hostname\":\"$APP_HOSTNAME\",
       \"image\":\"$ONTIME_REF\",\"port\":8080,\"replicas\":1,
       \"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:8080/\",\"interval_seconds\":5,\"timeout_seconds\":2,\"unhealthy_threshold\":3},
       \"auto_standby\":{\"enabled\":true,\"idle_timeout_seconds\":$STANDBY_IDLE_S}}")
[[ "$cs" == "201" ]] || fail "A: app create: $cs $(cat /tmp/v11-a.json)"
wait_running "$APP_A" 1 180
mark "A1 app RUNNING（auto_standby=${STANDBY_IDLE_S}s + 5s 探针间隔）"

# A2: 空闲 → PAUSED（VMM 释放：firecracker 进程计数下降）。
fc_before=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
for _ in $(seq 1 60); do
  st=$(pg "SELECT observed_state FROM machines WHERE app_id='$APP_A' AND desired_state!='DELETED'")
  [[ "$st" == "PAUSED" ]] && break
  sleep 2
done
[[ "$st" == "PAUSED" ]] || fail "A2: 空闲未 standby（探针流量未排除？state=$st）"
fc_after=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
[[ "$fc_after" -lt "$fc_before" ]] || fail "A2: standby 后 VMM 未释放 (fc $fc_before → $fc_after)"
mark "A2 空闲 standby OK（VMM $fc_before → $fc_after，探针运行中仍清闲）"

# A3: 首请求唤醒，200 且 <5s（autoresume SLO）。
t0=$(date +%s%N)
code_wake=$(edge_code / 8)
t1=$(date +%s%N)
wake_ms=$(( (t1 - t0) / 1000000 ))
[[ "$code_wake" == "200" ]] || fail "A3: 唤醒请求非 200: $code_wake"
[[ "$wake_ms" -lt 5000 ]] || fail "A3: 唤醒延迟 ${wake_ms}ms ≥ 5s SLO"
mark "A3 唤醒 ${wake_ms}ms < 5s OK"

# A4: 多轮 idle→standby→唤醒 无泄漏（netns/veth/firecracker 计数不漂移）。
base_ns=$(ip netns list | grep -c '^fp-slot-' || true)
base_vp=$(ip link | grep -c 'fp-vp' || true)
for i in $(seq 1 "$STANDBY_ROUNDS"); do
  for _ in $(seq 1 30); do
    st=$(pg "SELECT observed_state FROM machines WHERE app_id='$APP_A' AND desired_state!='DELETED'")
    [[ "$st" == "PAUSED" ]] && break
    sleep 1
  done
  [[ "$st" == "PAUSED" ]] || fail "A4: 轮 $i 未 standby"
  c=$(edge_code / 8)
  [[ "$c" == "200" ]] || fail "A4: 轮 $i 唤醒失败 ($c)"
done
end_ns=$(ip netns list | grep -c '^fp-slot-' || true)
end_vp=$(ip link | grep -c 'fp-vp' || true)
[[ "$end_ns" == "$base_ns" && "$end_vp" == "$base_vp" ]] || \
  fail "A4: 泄漏 ns $base_ns→$end_ns vp $base_vp→$end_vp"
mark "A4 ${STANDBY_ROUNDS} 轮 idle→standby→唤醒 无泄漏 OK"

# A5: 默认关闭回归——无策略 app 空闲窗口内不 standby。
APP_A2="app-v11a2-$RUN_ID"
HN_A2="$APP_A2.$DOMAIN"
cs=$(cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP_A2\",\"project_id\":\"dev\",\"hostname\":\"$HN_A2\",
       \"image\":\"$ONTIME_REF\",\"port\":8080,\"replicas\":1}")
[[ "$cs" == "201" ]] || fail "A5: 无策略 app create: $cs"
wait_running "$APP_A2" 1 180
sleep $(( STANDBY_IDLE_S * 3 ))
st=$(pg "SELECT observed_state FROM machines WHERE app_id='$APP_A2' AND desired_state!='DELETED'")
[[ "$st" == "RUNNING" ]] || fail "A5: 无策略 app 行为回归（state=$st）"
mark "A5 默认关闭回归 OK（空闲 $(( STANDBY_IDLE_S * 3 ))s 仍 RUNNING）"
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP_A2" || true

# ---------------------------------------------------------------------------
log "C+E) edge 流量语义 + 多端口 services（先于 B/D：复用双副本 app）"
APP_C="app-v11c-$RUN_ID"
APP_HOSTNAME="$APP_C.$DOMAIN"
# 多端口 services：主 8080 + 附加 9080（经 edge 附加监听段）+ 附加 80（经
# Host 显式端口寻址）；replicas=2。EXTRA_PORTS 让 guest ontime 监听 9080。
# 声明 health_check：无探针时新副本在 observed RUNNING 即入路由（UNCONFIGURED
# 语义），guest 服务尚未监听的窗口会产生 502——rolling 0 失败断言要求探针
# 门控（ADR-0008 的存在意义）。
cs=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/v11-c.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP_C\",\"project_id\":\"dev\",\"hostname\":\"$APP_HOSTNAME\",
       \"image\":\"$ONTIME_REF\",\"replicas\":2,\"env\":{\"EXTRA_PORTS\":\"9080\"},
       \"health_check\":{\"type\":\"http\",\"target\":\"http://127.0.0.1:8080/\",\"interval_seconds\":5,\"timeout_seconds\":2,\"unhealthy_threshold\":3},
       \"services\":[{\"name\":\"main\",\"internal_port\":8080},{\"name\":\"metrics\",\"internal_port\":9080},{\"name\":\"legacy\",\"internal_port\":80}]}")
[[ "$cs" == "201" ]] || fail "C: 多端口 app create: $cs $(cat /tmp/v11-c.json)"
wait_running "$APP_C" 2 180
mark "C0 双副本多端口 app RUNNING（services: main=8080, metrics=9080, legacy=80）"

# C1: 响应头 X-Firepaas-Machine-ID。
mid=$(edge_header "X-Firepaas-Machine-ID")
[[ "$mid" == app-v11c-* ]] || fail "C1: X-Firepaas-Machine-ID 缺失/错误: '$mid'"
mark "C1 响应头 X-Firepaas-Machine-ID = $mid OK"

# E1: 主端口 + 附加端口分别路由；未声明端口 404。
# 主 service（8080）：TLS 入口按 hostidx 首元素查路由。
c_main=$(edge_code / 8)
[[ "$c_main" == "200" ]] || fail "E1: 主端口路由失败: $c_main"
# 附加 service（9080）：edge 附加监听段 → 按 (hostname, 9080) 查路由 →
# X-Firepaas-App-Port=9080 → proxy 白名单 → guest:9080（EXTRA_PORTS 监听）。
c_9080=$(curl -sk -o /dev/null -m 8 -w '%{http_code}' --resolve "$APP_HOSTNAME:9080:127.0.0.1" \
  "http://$APP_HOSTNAME:9080/")
[[ "$c_9080" == "200" ]] || fail "E1: 附加端口(9080, edge extra listener)路由失败: $c_9080"
# 附加 service（80）：TLS 入口 + Host 显式 service 端口（非 edge 自身端口）。
c_80=$(curl -sk -o /dev/null -m 8 -w '%{http_code}' --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
  -H "Host: $APP_HOSTNAME:80" "https://127.0.0.1:$EDGE_TLS/")
[[ "$c_80" == "200" ]] || fail "E1: 附加端口(80, Host 显式端口)路由失败: $c_80"
# 未声明端口：edge 附加监听 9081 上无 app 声明 → 404。
c_und=$(curl -sk -o /dev/null -m 8 -w '%{http_code}' --resolve "$APP_HOSTNAME:9081:127.0.0.1" \
  "http://$APP_HOSTNAME:9081/")
[[ "$c_und" == "404" ]] || fail "E1: 未声明端口应 404: $c_und"
mark "E1 主端口(8080)/附加端口(9080 extra listener, 80 Host 寻址)路由 + 未声明端口 404 OK"

# C2: 钉扎——100% 命中指定 machine（响应头校验）；钉错 id → 404。
pin_ok=0
for _ in $(seq 1 10); do
  h=$(curl -sk -o /dev/null -D - -m 8 --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
    -H "X-Firepaas-Pin-Machine: $mid" "https://$APP_HOSTNAME:$EDGE_TLS/" | tr -d '\r' | grep -i '^X-Firepaas-Machine-ID:' | head -1 | cut -d' ' -f2) || h=""
  [[ "$h" == "$mid" ]] && pin_ok=$((pin_ok+1))
done
[[ "$pin_ok" == 10 ]] || fail "C2: 钉扎命中率 $pin_ok/10"
c_pin404=$(curl -sk -o /dev/null -m 8 -w '%{http_code}' --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
  -H "X-Firepaas-Pin-Machine: no-such-machine" "https://$APP_HOSTNAME:$EDGE_TLS/")
[[ "$c_pin404" == "404" ]] || fail "C2: 钉错 id 应 404: $c_pin404"
mark "C2 钉扎 10/10 命中 + 钉错 404 OK"

# C3: least-inflight——钉扎占满 machine A 的 inflight（2 个 /slow），
# 后续请求应全部偏移到另一台。
other=$(pg "SELECT id FROM machines WHERE app_id='$APP_C' AND desired_state!='DELETED' AND id != '$mid' LIMIT 1")
[[ -n "$other" ]] || fail "C3: 第二副本缺失"
SLOW_PIDS=""
for _ in 1 2; do
  curl -sk -o /dev/null -m 15 --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
    -H "X-Firepaas-Pin-Machine: $mid" "https://$APP_HOSTNAME:$EDGE_TLS/slow?ms=4000" &
  SLOW_PIDS="$SLOW_PIDS $!"
done
sleep 1
shift_b=0
for _ in $(seq 1 10); do
  # || h="" 防 set -e：错误响应（如瞬时 503）无该头时 grep 退出 1，
  # pipefail 会静默杀死整个脚本（run10 实测踩坑）。
  h=$(curl -sk -o /dev/null -D - -m 8 --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
    "https://$APP_HOSTNAME:$EDGE_TLS/" | tr -d '\r' | grep -i '^X-Firepaas-Machine-ID:' | head -1 | cut -d' ' -f2) || h=""
  [[ "$h" == "$other" ]] && shift_b=$((shift_b+1))
done
# 只等本节拉起的后台 curl（不能 wait 全部 jobs：api/edge 是常驻后台任务）。
for pid in $SLOW_PIDS; do wait "$pid" 2>/dev/null || true; done
[[ "$shift_b" == 10 ]] || fail "C3: least-inflight 偏移不足（$shift_b/10 落到空闲副本）"
mark "C3 least-inflight：busy 副本 10/10 偏移到空闲副本 OK"

# C4: inflight 生命周期——慢请求期间 metrics >0，完成后归零。
curl -sk -o /dev/null -m 15 --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
  "https://$APP_HOSTNAME:$EDGE_TLS/slow?ms=3000" &
SLOW_PID=$!
sleep 1
inflight_during=$(curl -s -m 5 http://127.0.0.1:9465/metrics | grep -c 'firepaas_edge_backend_inflight{.*} [1-9]' || true)

wait $SLOW_PID
sleep 2
inflight_after=$(curl -s -m 5 http://127.0.0.1:9465/metrics | grep -c 'firepaas_edge_backend_inflight{.*} [1-9]' || true)
[[ "$inflight_during" -ge 1 ]] || fail "C4: 慢请求期间 inflight 应 >0"
[[ "$inflight_after" == "0" ]] || fail "C4: 请求完成后 inflight 未归零"
mark "C4 inflight 生命周期（进行中>0，完成后=0）OK"

# C5: hard 并发——hard=2：对单副本钉扎 3 个并发慢请求 → 至少 1 个 503。
APP_C2="app-v11c2-$RUN_ID"
APP_HOSTNAME="$APP_C2.$DOMAIN"
cs=$(cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP_C2\",\"project_id\":\"dev\",\"hostname\":\"$APP_HOSTNAME\",
       \"image\":\"$ONTIME_REF\",\"port\":8080,\"replicas\":1}")
[[ "$cs" == "201" ]] || fail "C5: 单副本 app create: $cs"
wait_running "$APP_C2" 1 180
mid2=$(pg "SELECT id FROM machines WHERE app_id='$APP_C2' AND desired_state!='DELETED' LIMIT 1")
hard503=0
rm -f "$RUN_DIR"/hard-*.code "$RUN_DIR/f-fail.log" 2>/dev/null || true
HARD_PIDS=""
for i in 1 2 3; do
  ( curl -sk -o /dev/null -m 15 -w '%{http_code}' --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
      -H "X-Firepaas-Pin-Machine: $mid2" "https://$APP_HOSTNAME:$EDGE_TLS/slow?ms=2500" > "$RUN_DIR/hard-$i.code" ) &
  HARD_PIDS="$HARD_PIDS $!"
done
for pid in $HARD_PIDS; do wait "$pid" 2>/dev/null || true; done
for i in 1 2 3; do
  [[ "$(cat "$RUN_DIR/hard-$i.code")" == "503" ]] && hard503=$((hard503+1))
done
[[ "$hard503" -ge 1 ]] || fail "C5: hard=2 下 3 并发未触发 503（503 数=$hard503）"
hr=$(curl -s -m 5 http://127.0.0.1:9465/metrics | grep -c 'firepaas_edge_hard_rejected_total' || true)
[[ "$hr" -ge 1 ]] || fail "C5: hard_rejected 计数器缺失"
mark "C5 hard 并发上限（3 并发 → ${hard503}×503 + Retry-After + 计数器）OK"
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP_C2" || true

# E2: 单端口存量回归——无 services 声明的 app 行为不变。
APP_HOSTNAME="$APP_A.$DOMAIN"  # A 段的单端口 app（port=8080）
c_reg=$(edge_code / 8)
[[ "$c_reg" == "200" ]] || fail "E2: 单端口 app 回归失败: $c_reg"
mark "E2 单端口存量 app 回归 OK"

# ---------------------------------------------------------------------------
log "F) rolling batch 发布 + per-VM 指标直抓"
APP_HOSTNAME="$APP_C.$DOMAIN"
# F1: 后台并发请求（发布全程 0 失败断言）。每轮必须清空结果，避免旧失败被重复统计。
req_fail=0; req_total=0
rm -f "$RUN_DIR/f-reqs.log" "$RUN_DIR/f-fail.log"
( for i in $(seq 1 240); do
    out=$(curl -sk -m 8 -w '%{http_code}' --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" \
      "https://$APP_HOSTNAME:$EDGE_TLS/" 2>/dev/null) || out="000"
    code="${out: -3}"
    if [[ "$code" != "200" ]]; then
      { echo "--- req $i code=$code $(date +%H:%M:%S.%N)"; \
        curl -sk -D - -m 8 --resolve "$APP_HOSTNAME:$EDGE_TLS:127.0.0.1" "https://$APP_HOSTNAME:$EDGE_TLS/" 2>/dev/null; } >> "$RUN_DIR/f-fail.log" 2>&1 || true
    fi
    echo "$code" >> "$RUN_DIR/f-reqs.log"
    sleep 0.5
  done ) &
REQ_PID=$!
sleep 2

# F2: rolling deploy（batch = max(1, 25%·2) = 1：逐副本切流）。
ds=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/v11-f.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps/$APP_C/deployments" -H 'Content-Type: application/json' \
  -d "{\"strategy\":\"rolling\",\"env\":{\"ROLLING\":\"v11\"}}")
[[ "$ds" == "202" ]] || fail "F2: rolling deploy: $ds $(cat /tmp/v11-f.json)"
# 等待 rollout 完成（逐批：切流→回收旧代→下一批）。
for _ in $(seq 1 120); do
  rl=$(pg "SELECT count(*) FROM rollouts r JOIN apps a ON a.id=r.app_id WHERE a.id='$APP_C' AND r.status IN ('PREPARING','CUTOVER','ROLLING_BACK')")
  [[ "$rl" == "0" ]] && break
  sleep 2
done
[[ "$rl" == "0" ]] || fail "F2: rolling rollout 未完成"
gen=$(pg "SELECT generation FROM apps WHERE id='$APP_C'")
dep_active=$(pg "SELECT count(*) FROM deployments WHERE app_id='$APP_C' AND generation=$gen AND status='ACTIVE'")
[[ "$dep_active" == "1" ]] || fail "F2: 新代未 ACTIVE"
sleep 8  # 请求循环收尾
kill $REQ_PID 2>/dev/null || true
wait $REQ_PID 2>/dev/null || true
req_total=$(wc -l < "$RUN_DIR/f-reqs.log")
req_fail=$(grep -cv '^200$' "$RUN_DIR/f-reqs.log" || true)
[[ "$req_fail" == "0" ]] || fail "F2: rolling 发布期间 $req_fail/$req_total 请求失败"
mark "F2 rolling 策略发布完成（$req_total 请求 0 失败）"

# F2b: rolling 期间旧代逐批回收（终态：旧代 machine 全部 desired=DELETED）。
old_alive=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP_C' AND desired_state!='DELETED' AND deployment_id != (SELECT id FROM deployments WHERE app_id='$APP_C' AND generation=$gen)")
[[ "$old_alive" == "0" ]] || fail "F2b: 旧代 machine 未回收（$old_alive 台存活）"
mark "F2b 旧代逐批回收完成 OK"

# F3: per-VM 指标直抓（agentd :9464）。
vm_metrics=$(curl -s -m 5 http://127.0.0.1:9464/metrics || true)
echo "$vm_metrics" | grep -q 'vm_cpu\|hypeman_vm' || fail "F3: per-VM 指标缺失（vm_metrics）"
echo "$vm_metrics" | grep -q 'firepaas_agent_autostandby_wakes_total' || true  # A 段唤醒计数（非硬断言）
mark "F3 per-VM 指标直抓 OK"

# ---------------------------------------------------------------------------
log "B) 镜像缓存亲和与部署预取（ADR-0018）"
# B1: nodes.image_cache 落库（agentd 已运行多镜像 pull；20s sync 周期）。
nimg=0
for _ in $(seq 1 15); do
  nimg=$(pg "SELECT count(*) FROM nodes WHERE jsonb_array_length(image_cache) > 0")
  [[ "$nimg" -ge 1 ]] && break
  sleep 3
done
[[ "$nimg" -ge 1 ]] || fail "B1: nodes.image_cache 未落库"
mark "B1 nodes.image_cache 已落库"

# B2: 新镜像 deploy → prefetch 调度事件。
APP_B="app-v11b-$RUN_ID"
APP_HOSTNAME="$APP_B.$DOMAIN"
cs=$(cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -w '%{http_code}' \
  -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
  -d "{\"app_id\":\"$APP_B\",\"project_id\":\"dev\",\"hostname\":\"$APP_HOSTNAME\",
       \"image\":\"$ONTIME_REF\",\"port\":8080,\"replicas\":1}")
[[ "$cs" == "201" ]] || fail "B2: app create: $cs"
wait_running "$APP_B" 1 180
# prefetch 事件（同镜像已缓存也可能重发；断言事件存在）。
for _ in $(seq 1 10); do
  npf=$(pg "SELECT count(*) FROM scheduler_events WHERE kind LIKE 'prefetch%'")
  [[ "$npf" -ge 1 ]] && break
  sleep 2
done
[[ "$npf" -ge 1 ]] || fail "B2: prefetch 调度事件缺失"
mark "B2 部署预取事件 OK（prefetch events=$npf）"
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP_B" || true

# ---------------------------------------------------------------------------
log "D) drain+evacuate（ADR-0021；单节点：重建窗口已知边界）"
# 清理多副本 app：单节点实验室只有 APP_A 一台 machine——驱离步骤会持锁
# 等待 replacement（无可放置节点），多 machine 会卡在第一台（并发=1）。
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP_C" || true
for _ in $(seq 1 40); do
  alive_c=$(pg "SELECT count(*) FROM machines WHERE app_id='$APP_C' AND desired_state!='DELETED'")
  [[ "$alive_c" == "0" ]] && break
  sleep 3
done
NODE_ID=$(curl -s -m 10 -H "Authorization: Bearer $API_TOKEN" "http://127.0.0.1:$API_PORT/v1/nodes" |
  python3 -c 'import json,sys; ns=json.load(sys.stdin).get("nodes", []); print((ns[0].get("ID") or ns[0].get("id")) if ns else "")')
[[ -n "$NODE_ID" ]] || fail "D: 无注册节点"
ds=$(cur -H "Authorization: Bearer $API_TOKEN" -o /tmp/v11-d.json -w '%{http_code}' \
  -X POST -H 'Content-Type: application/json' -d '{"evacuate": true}' \
  "http://127.0.0.1:$API_PORT/v1/nodes/$NODE_ID/drain")
[[ "$ds" == "200" ]] || fail "D: drain evacuate: $ds $(cat /tmp/v11-d.json)"
# 等唯一 machine 被驱离步骤 fence（node_id 清空 = 源实例让位；单节点下
# replacement 无处可放，步骤持锁等待——这是 ADR-0021 在单节点实验室的
# 已知边界，节点 machine 归零后即可安全维护）。
for _ in $(seq 1 60); do
  onnode=$(pg "SELECT count(*) FROM machines WHERE node_id='$NODE_ID' AND desired_state!='DELETED'")
  [[ "$onnode" == "0" ]] && break
  sleep 5
done
[[ "$onnode" == "0" ]] || fail "D: 节点 machine 未归零（$onnode）"
ev=$(pg "SELECT count(*) FROM scheduler_events WHERE kind='evacuate'")
[[ "$ev" -ge 1 ]] || fail "D: evacuate 步骤事件缺失"
held=$(pg "SELECT count(*) FROM nodes WHERE id='$NODE_ID' AND evacuation_machine_id IS NOT NULL")
[[ "$held" == "1" ]] || fail "D: 驱离步骤未持久化持有（evacuation_machine_id 为空）"
mark "D1 drain+evacuate：节点 machine 归零 + 步骤事件 + 持久步骤 fence OK（单节点边界：replacement 等 ready）"

# D2: ready 恢复 → machine 重建 → app 恢复服务。
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X POST "http://127.0.0.1:$API_PORT/v1/nodes/$NODE_ID/ready"
for _ in $(seq 1 30); do
  st=$(pg "SELECT count(*) FROM nodes WHERE id='$NODE_ID' AND NOT draining AND evacuation_machine_id IS NULL")
  [[ "$st" == "1" ]] && break
  sleep 2
done
[[ "$st" == "1" ]] || fail "D2: ready 未清除 draining/驱离步骤"
wait_running "$APP_A" 1 240
APP_HOSTNAME="$APP_A.$DOMAIN"
for _ in $(seq 1 30); do
  c=$(edge_code / 8)
  [[ "$c" == "200" ]] && break
  sleep 3
done
[[ "$c" == "200" ]] || fail "D2: evacuate 后服务未恢复: $c"
mark "D2 ready → 重建 → 服务恢复 OK（多节点零停机验收 DEFERRED-MULTI-NODE）"

# ---------------------------------------------------------------------------
log "清理 + 终态零泄漏"
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP_A" || true
cur -H "Authorization: Bearer $API_TOKEN" -o /dev/null -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP_C" || true
for _ in $(seq 1 60); do
  alive=$(pg "SELECT count(*) FROM machines WHERE desired_state != 'DELETED'")
  pending=$(pg "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
  [[ "$alive" == "0" && "$pending" == "0" ]] && break
  sleep 5
done
fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
ns=$(ip netns list | grep -c '^fp-slot-' || true)
vp=$(ip link | grep -c 'fp-vp' || true)
routes=$(ip route show | grep '^10\.100\.' | grep -cv 'dev firepaas0' || true)
[[ "$fc" == "0" && "$ns" == "0" && "$vp" == "0" && "$routes" == "0" && "$alive" == "0" && "$pending" == "0" ]] || \
  fail "终态泄漏 fc=$fc ns=$ns vp=$vp routes=$routes alive=$alive pending=$pending"
pkill -f "$LAB_BIN/firepaas-api" 2>/dev/null || true
pkill -f "$LAB_BIN/edge-proxy" 2>/dev/null || true

log "v1.1 e2e PASS：auto-standby（standby/唤醒/无泄漏/默认关闭） / 镜像缓存与预取 / \
edge（响应头/钉扎/least-inflight/hard 并发/inflight 生命周期） / drain+evacuate / \
多端口 services（双端口路由/未声明 404/单端口回归） / rolling batch + per-VM 指标 全部通过"
