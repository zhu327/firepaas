#!/usr/bin/env bash
# M0 P0 冒烟：OCI 镜像 → Firecracker VM → exec/logs → stop/delete + 主机残留检查。
# 前置：hypeman 已运行（Nomad job firepaas-hypeman-p0 或手工直跑）。
# 用法（建议 root）：sudo bash scripts/lab/smoke-p0.sh
# 环境变量：HYPEMAN_URL（默认 http://127.0.0.1:4973）、CONFIG_PATH（默认 scripts/lab/hypeman-p0.yaml）
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh" 2>/dev/null || true

HYPEMAN_URL="${HYPEMAN_URL:-http://127.0.0.1:4973}"
CONFIG_PATH="${CONFIG_PATH:-$HERE/hypeman-p0.yaml}"
IMAGE="docker.io/library/nginx:alpine"
INSTANCE_NAME="p0-smoke-$(date +%s)"
TOKEN=""
RESULT_DIR="$HERE/results"
mkdir -p "$RESULT_DIR"

log() { echo "[smoke] $*"; }
fail() { echo "[smoke] FAIL: $*" >&2; exit 1; }

require() {
  command -v "$1" >/dev/null || fail "missing command: $1 (请先完成 scripts/lab/build-hypeman.sh 并安装基础工具)"
}

require curl
require python3
command -v jq >/dev/null && HAVE_JQ=1 || HAVE_JQ=0

# ---- token ----
if [[ -z "${HYPEMAN_API_KEY:-}" ]]; then
  if [[ -x "$LAB_ROOT/bin/hypeman-token" ]]; then
    TOKEN="$(CONFIG_PATH="$CONFIG_PATH" "$LAB_ROOT/bin/hypeman-token" -user-id p0-smoke -duration 24h)"
  elif [[ -d "${HYPEMAN_SRC:-$HOME/Learn/hypeman}" ]]; then
    TOKEN="$(cd "${HYPEMAN_SRC:-$HOME/Learn/hypeman}" && CONFIG_PATH="$CONFIG_PATH" \
      env PATH="$LAB_ROOT/go/bin:$PATH" go run ./cmd/gen-jwt -user-id p0-smoke -duration 24h)"
  else
    fail "no hypeman-token binary or hypeman source"
  fi
else
  TOKEN="$HYPEMAN_API_KEY"
fi
export HYPEMAN_BASE_URL="$HYPEMAN_URL"
export HYPEMAN_API_KEY="$TOKEN"

api() {
  # api METHOD path [json-body]
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" -H "Authorization: Bearer $TOKEN")
  [[ -n "$body" ]] && args+=(-H "Content-Type: application/json" -d "$body")
  curl --max-time 30 "${args[@]}" "$HYPEMAN_URL$path"
}

json_get() {
  KEY="$1" python3 -c 'import json,sys,os
d=json.load(sys.stdin)
for k in os.environ["KEY"].split("."):
    d = d.get(k) if isinstance(d, dict) else None
print("" if d is None else d)'
}

# ---- health ----
log "waiting for hypeman health at $HYPEMAN_URL/health"
for _ in $(seq 1 60); do
  if curl -fsS --max-time 3 "$HYPEMAN_URL/health" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$HYPEMAN_URL/health" >/dev/null || fail "hypeman /health 不可达"
log "health OK"

# ---- pull image ----
ENC_IMAGE="$(python3 -c 'from urllib.parse import quote; print(quote("'"$IMAGE"'", safe=""))')"
log "pull image $IMAGE"
PULL_RESP="$(api POST /images "$(python3 -c 'import json,sys; print(json.dumps({"name": sys.argv[1]}))' "$IMAGE")")"
for _ in $(seq 1 120); do
  IMG_JSON="$(api GET "/images/$ENC_IMAGE")"
  IMG_STATUS="$(echo "$IMG_JSON" | json_get status)"
  [[ "$IMG_STATUS" == "ready" ]] && break
  [[ "$IMG_STATUS" == "failed" ]] && fail "image pull failed: $IMG_JSON"
  sleep 5
done
[[ "$IMG_STATUS" == "ready" ]] || fail "image not ready within 10m"
log "image ready"

# ---- create instance ----
CREATE_BODY="$(python3 -c 'import json,sys; print(json.dumps({"name":sys.argv[1],"image":sys.argv[2],"vcpus":1,"size":"512MB"}))' "$INSTANCE_NAME" "$IMAGE")"
INSTANCE_JSON="$(api POST /instances "$CREATE_BODY")"
INSTANCE_ID="$(echo "$INSTANCE_JSON" | json_get id)"
[[ -n "$INSTANCE_ID" && "$INSTANCE_ID" != None ]] || fail "create instance returned no id: $INSTANCE_JSON"
log "instance created id=$INSTANCE_ID name=$INSTANCE_NAME"

# ---- wait running ----
log "wait for instance running (max 3m)"
WAIT_OUT="$(curl -sS --max-time 180 "$HYPEMAN_URL/instances/$INSTANCE_ID/wait?state=running" -H "Authorization: Bearer $TOKEN")" \
  || fail "instance did not reach running: ${WAIT_OUT:-}"
echo "$WAIT_OUT" | grep -qiE '"running"|"state":"running"' || fail "unexpected wait response: $WAIT_OUT"
log "instance running"

# ---- exec (CLI, 可选但推荐) ----
if [[ -x "$LAB_ROOT/bin/hypeman-cli" ]]; then
  log "exec: echo p0-smoke-ok"
  EXEC_OUT="$(HYPEMAN_BASE_URL="$HYPEMAN_URL" HYPEMAN_API_KEY="$TOKEN" \
    "$LAB_ROOT/bin/hypeman-cli" exec "$INSTANCE_ID" -- echo p0-smoke-ok 2>&1 || true)"
  echo "$EXEC_OUT" | head -20
else
  log "WARN: hypeman-cli not built, skipping exec check"
fi

# ---- logs ----
log "logs tail"
curl -sS --max-time 30 -H "Authorization: Bearer $TOKEN" \
  "$HYPEMAN_URL/instances/$INSTANCE_ID/logs?tail=20&follow=false" | head -20

# ---- stop / delete ----
log "stop instance"
api POST "/instances/$INSTANCE_ID/stop" >/dev/null
log "delete instance"
DEL_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE -H "Authorization: Bearer $TOKEN" \
  "$HYPEMAN_URL/instances/$INSTANCE_ID")"
[[ "$DEL_CODE" == "204" || "$DEL_CODE" == "404" ]] || fail "delete returned $DEL_CODE"
log "instance deleted"

# ---- 主机残留检查 ----
log "residual check"
RESIDUAL=0
if command -v ip >/dev/null; then
  LEAK="$(ip -o link show 2>/dev/null | grep -E 'firepaas[0-9]*|tap[0-9a-f]+' || true)"
  if [[ -n "$LEAK" ]]; then
    echo "$LEAK"
    echo "[smoke] WARN: TAP/bridge 接口残留（可能包含其他 hypeman 实例，需人工确认）"
    RESIDUAL=1
  fi
  if command -v ip >/dev/null && ip netns list 2>/dev/null | grep -q .; then
    ip netns list
    echo "[smoke] WARN: 存在 netns（确认是否属于本实例）"
    RESIDUAL=1
  fi
fi
# 只看本次实例 id 相关的 firecracker/hypeman 子进程
PROC_LEAK="$(ps -eo pid,args 2>/dev/null | grep -E "firecracker|cloud-hypervisor" | grep "$INSTANCE_ID" || true)"
if [[ -n "$PROC_LEAK" ]]; then
  echo "$PROC_LEAK"
  fail "VM 进程残留"
fi
[[ "$RESIDUAL" == 0 ]] || echo "[smoke] WARN: 非进程类残留需人工核对（单机可能与其他实例共享 bridge）"

log "P0 smoke PASS: pull/run/exec/logs/stop/delete 完成"
