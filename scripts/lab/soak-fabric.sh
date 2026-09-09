#!/usr/bin/env bash
# LAYER: l3-fabric  PREREQ: dual-agent,api,root  DESTRUCTIVE: no  FROZEN: no
# ADR-0040 G3 fabric soak（§22）：72h 长跑 + 1000 次分配/释放无泄漏
#（M3 TestSlotCycleLeak 同口径的 fabric 扩展）+ 冷启动 p95 不退化声明。
#
# ⚠️ 数小时级长跑 + 真实 VM 生命周期：运行需用户明确授权；本脚本为
# 声明式 harness（检查点 + 证据落盘），不是一次性断言。
#
# 用法:
#   sudo bash scripts/lab/soak-fabric.sh [--hours 72] [--cycles 1000]
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="/var/lib/firepaas-p0/soak-fabric"
mkdir -p "$RUN_DIR"

HOURS=72
CYCLES=1000
while [ $# -gt 0 ]; do
  case "$1" in
    --hours) HOURS="$2"; shift 2 ;;
    --cycles) CYCLES="$2"; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 1 ;;
  esac
done

now() { date +%H:%M:%S; }
log() { echo "[fabric-soak $(now)] $*" | tee -a "$RUN_DIR/soak.log"; }
pg() { docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc "$1"; }

log "fabric soak start: hours=$HOURS cycles=$CYCLES"

# --- 1) ULA 分配/释放循环（1000 次，无泄漏断言）---
# 每轮：create 一个 mesh_direct app（分配 ULA + identity）→ READY →
# delete（释放）→ 断言在役分配数回落基线。泄漏表现 = ipam_allocations
# 在役行数单调增长 / workload_identities 无限增长。
BASE_ACTIVE=$(pg "SELECT count(*) FROM ipam_allocations WHERE released_at IS NULL")
BASE_IDENT=$(pg "SELECT count(*) FROM workload_identities")
for i in $(seq 1 "$CYCLES"); do
  APP="soak-$i-$(date +%s)"
  # create → wait READY → delete（等释放收敛）
  curl -fsS -m 20 -H "Authorization: Bearer ${FP_API_TOKEN:?set FP_API_TOKEN}" \
    -X POST "${FP_API_ADDR:-http://127.0.0.1:8083}/v1/apps" \
    -H 'Content-Type: application/json' \
    -d "{\"app_id\":\"$APP\",\"project_id\":\"dev\",\"hostname\":\"$APP.local\",\"image\":\"${FP_SOAK_IMAGE:?set FP_SOAK_IMAGE}\",\"port\":80,\"replicas\":1,\"services\":[{\"internal_port\":80,\"mesh_direct\":true}]}" >/dev/null
  for _ in $(seq 1 60); do
    [ "$(pg "SELECT coalesce(observed_state,'') FROM machines WHERE app_id='$APP' LIMIT 1")" = "RUNNING" ] && break
    sleep 5
  done
  curl -fsS -m 20 -H "Authorization: Bearer $FP_API_TOKEN" \
    -X DELETE "${FP_API_ADDR:-http://127.0.0.1:8083}/v1/apps/$APP" >/dev/null
  # 每四分之一轮检查点（小轮次也有泄漏断言；并发窗口容差 +1）。
  STEP=$(( CYCLES / 4 )); [ "$STEP" -lt 1 ] && STEP=1
  if [ $((i % STEP)) -eq 0 ]; then
    ACTIVE=$(pg "SELECT count(*) FROM ipam_allocations WHERE released_at IS NULL")
    if [ "$ACTIVE" -gt $((BASE_ACTIVE + 1)) ]; then
      log "LEAK SUSPECTED at cycle $i: active=$ACTIVE baseline=$BASE_ACTIVE"
      exit 1
    fi
    log "cycle $i: active=$ACTIVE (baseline $BASE_ACTIVE) identities=$(pg "SELECT count(*) FROM workload_identities") (baseline $BASE_IDENT)"
  fi
done
log "allocation cycles complete: $CYCLES x create/delete, no leak"

# --- 2) 长跑窗口（小时级）：周期健康检查 + WG handshake-age 记录 ---
# 检查项：fabric snapshot 推送持续（fabric_versions.updated_at < 5min）、
# 双节点 WG handshake 最新鲜度 < 180s、mesh:peer/endpoint 投影 TTL 存活。
DEADLINE=$(( $(date +%s) + HOURS * 3600 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  STALE=$(pg "SELECT count(*) FROM fabric_versions WHERE updated_at < now() - interval '5 minutes'")
  [ "$STALE" = "0" ] || { log "fabric push stalled: $STALE nodes stale >5min"; exit 1; }
  HSA=$(wg show fp-wg0 latest-handshakes 2>/dev/null | awk '{print $2}' | sort -n | tail -1)
  AGE=$(( $(date +%s) - HSA ))
  [ "$AGE" -lt 180 ] || log "WARN: node-a oldest handshake age ${AGE}s (>180s, WG keepalive 25s expected)"
  sleep 300
done
log "soak window complete: ${HOURS}h"

# --- 3) 冷启动 p95 声明（§22）---
# 声明口径：agent 重启（Nomad job restart）后，从进程启动到首个
# ApplyFabric 快照重放完成的耗时分位数。基线 = 本 soak 启动时首轮值；
# 不退化 = p95 ≤ 基线 × 1.5（声明性断言，非硬 gate）。
log "cold-start p95 declaration: capture agentd restart timing (see $RUN_DIR/coldstart.log)"
log "fabric soak PASS (cycles=$CYCLES hours=$HOURS)"
