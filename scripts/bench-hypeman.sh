#!/usr/bin/env bash
# M0 P0.3 基准 runner：对 hypeman(单机 P0 job) 做冷启动/未缓存冷启动/standby-restore/密度基准。
# 输出原始样本 JSONL + CSV 到 scripts/lab/results/，并打印 p50/p95 汇总。
#
# 用法（建议 root）：
#   sudo bash scripts/bench-hypeman.sh cold [N]       # 镜像已缓存冷启动,N 默认 10
#   sudo bash scripts/bench-hypeman.sh uncached [N]   # 每次先删镜像再拉取,N 默认 3
#   sudo bash scripts/bench-hypeman.sh standby [N]    # standby/restore 往返,N 默认 10
#   sudo bash scripts/bench-hypeman.sh density [MAX]  # 1vCPU/512MiB 实例逐步加满,MAX 默认 16
#
# 环境变量：
#   HYPEMAN_URL（默认 http://127.0.0.1:4973）
#   CONFIG_PATH（默认 scripts/lab/hypeman-p0.yaml）
#   HYPEMAN_IMAGE（默认 docker.io/library/nginx:alpine）
# 结果目录：scripts/lab/results/<run-id>/（原始 JSONL/CSV），不靠手工粘贴。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh" 2>/dev/null || true

HYPEMAN_URL="${HYPEMAN_URL:-http://127.0.0.1:4973}"
CONFIG_PATH="${CONFIG_PATH:-$HERE/hypeman-p0.yaml}"
IMAGE="${HYPEMAN_IMAGE:-docker.io/library/nginx:alpine}"
CMD="${1:-}"
N="${2:-}"
RUN_ID="$(date +%Y%m%dT%H%M%S)-$CMD"
RESULT_DIR="$HERE/results/$RUN_ID"
mkdir -p "$RESULT_DIR"

log() { echo "[bench:$CMD] $*"; }
fail() { echo "[bench:$CMD] FAIL: $*" >&2; exit 1; }

command -v curl >/dev/null || fail "missing curl"
command -v python3 >/dev/null || fail "missing python3"

# ---- token / http helpers ----
if [[ -z "${HYPEMAN_API_KEY:-}" ]]; then
  if [[ -x "$LAB_ROOT/bin/hypeman-token" ]]; then
    TOKEN="$(CONFIG_PATH="$CONFIG_PATH" "$LAB_ROOT/bin/hypeman-token" -user-id p0-bench -duration 24h)"
  elif [[ -d "${HYPEMAN_SRC:-$HOME/Learn/hypeman}" ]]; then
    TOKEN="$(cd "${HYPEMAN_SRC:-$HOME/Learn/hypeman}" && CONFIG_PATH="$CONFIG_PATH" \
      env PATH="$LAB_ROOT/go/bin:$PATH" go run ./cmd/gen-jwt -user-id p0-bench -duration 24h)"
  else
    fail "no hypeman-token binary or hypeman source"
  fi
else
  TOKEN="$HYPEMAN_API_KEY"
fi
export HYPEMAN_BASE_URL="$HYPEMAN_URL"
export HYPEMAN_API_KEY="$TOKEN"

api() {
  local method="$1" path="$2" body="${3:-}" max_time="${4:-60}"
  local args=(-sS -X "$method" -H "Authorization: Bearer $TOKEN")
  [[ -n "$body" ]] && args+=(-H "Content-Type: application/json" -d "$body")
  curl --max-time "$max_time" "${args[@]}" "$HYPEMAN_URL$path"
}

json_get() {
  KEY="$1" python3 -c 'import json,sys,os
d=json.load(sys.stdin)
for k in os.environ["KEY"].split("."):
    d = d.get(k) if isinstance(d, dict) else None
print("" if d is None else d)'
}

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

enc() { python3 -c 'from urllib.parse import quote; import sys; print(quote(sys.argv[1], safe=""))' "$1"; }

health_wait() {
  for _ in $(seq 1 60); do
    curl -fsS --max-time 3 "$HYPEMAN_URL/health" >/dev/null 2>&1 && return 0
    sleep 2
  done
  fail "hypeman /health 不可达"
}

image_ready_wait() {
  local enc_name="$1" timeout="${2:-600}"
  local deadline=$(( $(date +%s) + timeout ))
  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    local json status
    json="$(api GET "/images/$enc_name")" || true
    status="$(echo "$json" | json_get status)"
    [[ "$status" == "ready" ]] && return 0
    [[ "$status" == "failed" ]] && fail "image failed: $json"
    sleep 5
  done
  fail "image not ready within ${timeout}s"
}

instance_wait_running() {
  local id="$1" timeout="${2:-180}"
  curl -sS --max-time "$timeout" -H "Authorization: Bearer $TOKEN" \
    "$HYPEMAN_URL/instances/$id/wait?state=running" \
    | grep -qiE 'running' || fail "instance $id did not reach running (timeout ${timeout}s)"
}

instance_state() {
  api GET "/instances/$1" | json_get state
}

instance_delete() {
  local id="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 60 -X DELETE \
    -H "Authorization: Bearer $TOKEN" "$HYPEMAN_URL/instances/$id" || true
}

create_instance() {
  local name="$1" size="${2:-512MB}" vcpus="${3:-1}"
  local body
  body="$(python3 -c 'import json,sys; print(json.dumps({"name":sys.argv[1],"image":sys.argv[2],"vcpus":int(sys.argv[3]),"size":sys.argv[4]}))' \
    "$name" "$IMAGE" "$vcpus" "$size")"
  api POST /instances "$body" 60
}

pull_image() {
  local body
  body="$(python3 -c 'import json,sys; print(json.dumps({"name":sys.argv[1]}))' "$IMAGE")"
  api POST /images "$body" 60 >/dev/null
}

delete_image() {
  local enc_name
  enc_name="$(enc "$IMAGE")"
  curl -sS -o /dev/null --max-time 60 -X DELETE -H "Authorization: Bearer $TOKEN" \
    "$HYPEMAN_URL/images/$enc_name" || true
}

# 残留检查：与本次 run id 相关的 VM 进程
residual_check() {
  local leak
  leak="$(ps -eo pid,args 2>/dev/null | grep -E 'firecracker|cloud-hypervisor' | grep -v grep | grep "bench-" || true)"
  if [[ -n "$leak" ]]; then
    echo "$leak"
    fail "VM 进程残留"
  fi
}

record() {
  # record metric_label ms
  local label="$1" ms="$2"
  printf '%s\n' "$(python3 -c 'import json,sys; print(json.dumps({"label":sys.argv[1],"ms":int(sys.argv[2]),"ts":__import__("time").time()}))' "$label" "$ms")" \
    >> "$RESULT_DIR/raw.jsonl"
  printf '%s,%s\n' "$label" "$ms" >> "$RESULT_DIR/raw.csv"
}

summarize() {
  log "samples in $RESULT_DIR"
  python3 - "$RESULT_DIR/raw.csv" <<'PY'
import sys, statistics, collections
rows = collections.defaultdict(list)
with open(sys.argv[1]) as f:
    for line in f:
        line=line.strip()
        if not line or line.startswith("label"): continue
        k,v = line.split(",",1)
        rows[k].append(int(v))
for k in sorted(rows):
    vals=sorted(rows[k])
    def q(p):
        if not vals: return None
        idx=(len(vals)-1)*p/100
        lo=int(idx); hi=min(lo+1,len(vals)-1)
        return round(vals[lo]+(vals[hi]-vals[lo])*(idx-lo),1)
    print(f"{k:22s} n={len(vals):3d} p50={q(50)}ms p95={q(95)}ms min={min(vals)}ms max={max(vals)}ms")
PY
}

# ============================== subcommands ==============================
cmd_cold() {
  local n="${N:-10}"
  health_wait
  local enc_img; enc_img="$(enc "$IMAGE")"
  log "ensure image cached"
  local img_status
  img_status="$(api GET "/images/$enc_img" | json_get status)" || true
  if [[ "$img_status" != "ready" ]]; then
    pull_image
    image_ready_wait "$enc_img"
  fi
  log "cold start n=$n image=$IMAGE"
  for i in $(seq 1 "$n"); do
    local name="bench-cold-$RUN_ID-$i" id t0 t1
    t0="$(now_ms)"
    id="$(create_instance "$name" | json_get id)"
    [[ -n "$id" && "$id" != None ]] || fail "create returned no id"
    instance_wait_running "$id"
    t1="$(now_ms)"
    record cold_ms $((t1-t0))
    log "  [$i/$n] id=$id cold=$((t1-t0))ms"
    instance_delete "$id"
    sleep 2
  done
  residual_check
}

cmd_uncached() {
  local n="${N:-3}"
  health_wait
  log "uncached cold start n=$n image=$IMAGE"
  for i in $(seq 1 "$n"); do
    local enc_img name id t0 t1 t2
    enc_img="$(enc "$IMAGE")"
    log "  [$i/$n] delete image + pull"
    delete_image
    t0="$(now_ms)"
    pull_image
    image_ready_wait "$enc_img"
    t1="$(now_ms)"
    name="bench-uncached-$RUN_ID-$i"
    id="$(create_instance "$name" | json_get id)"
    instance_wait_running "$id"
    t2="$(now_ms)"
    record pull_ms $((t1-t0))
    record uncached_total_ms $((t2-t0))
    log "  [$i/$n] pull=$((t1-t0))ms total=$((t2-t0))ms"
    instance_delete "$id"
    sleep 2
  done
  # 恢复缓存镜像供后续基准使用
  pull_image
  image_ready_wait "$(enc "$IMAGE")"
  residual_check
}

cmd_standby() {
  local n="${N:-10}"
  health_wait
  local enc_img; enc_img="$(enc "$IMAGE")"
  local img_status
  img_status="$(api GET "/images/$enc_img" | json_get status)" || true
  if [[ "$img_status" != "ready" ]]; then
    pull_image
    image_ready_wait "$enc_img"
  fi
  local name id t0 t1
  name="bench-standby-$RUN_ID"
  log "create source instance $name"
  id="$(create_instance "$name" | json_get id)"
  instance_wait_running "$id"
  for i in $(seq 1 "$n"); do
    t0="$(now_ms)"
    api POST "/instances/$id/standby" >/dev/null
    while [[ "$(instance_state "$id")" != "Standby" ]]; do sleep 1; done
    t1="$(now_ms)"
    api POST "/instances/$id/restore" >/dev/null
    instance_wait_running "$id"
    record standby_ms $((t1-t0))
    record restore_ms $(( $(now_ms) - t1 ))
    log "  [$i/$n] standby=$((t1-t0))ms restore=$(( $(now_ms) - t1 ))ms"
    sleep 1
  done
  instance_delete "$id"
  residual_check
}

cmd_density() {
  local max="${N:-16}"
  health_wait
  local enc_img; enc_img="$(enc "$IMAGE")"
  local img_status
  img_status="$(api GET "/images/$enc_img" | json_get status)" || true
  if [[ "$img_status" != "ready" ]]; then
    pull_image
    image_ready_wait "$enc_img"
  fi
  log "density 1vCPU/512MiB up to $max (注意:本机与 k8s 共存,结果为参考值)"
  local ids=() i
  for i in $(seq 1 "$max"); do
    local name id err
    name="bench-density-$RUN_ID-$i"
    err=""
    id="$(create_instance "$name" 512MB 1 | json_get id)" || err="create-failed"
    if [[ -n "$id" && "$id" != None ]]; then
      ids+=("$id")
      if instance_wait_running "$id" 120; then
        record density_ok $((i))
        log "  [$i] ok id=$id"
      else
        log "  [$i] wait-failed id=$id"
        break
      fi
    else
      log "  [$i] create-failed (达到单节点上限或资源不足): $err"
      break
    fi
  done
  log "density reached: ${#ids[@]}/$max"
  for id in "${ids[@]}"; do instance_delete "$id"; done
  sleep 5
  residual_check
}

case "$CMD" in
  cold) cmd_cold ;;
  uncached) cmd_uncached ;;
  standby) cmd_standby ;;
  density) cmd_density ;;
  *)
    echo "usage: $0 <cold|uncached|standby|density> [N]" >&2
    exit 2
    ;;
esac

summarize
log "done: $CMD"
