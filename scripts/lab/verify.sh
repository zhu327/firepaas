#!/usr/bin/env bash
# verify.sh：脚本测试体系统一入口（按层依序执行，fail-closed）。
#
# 分层（与 README.md 导航一致）：
#   l2  单机功能回归      smoke-p0 → e2e-m3 → e2e-m4 → e2e-m5（需单机 agentd）
#   l3  fabric 实验室     spike --check-only → stage1 → stage2 → stage3
#                         （--with-chaos 追加 chaos-fabric 全场景；--soak-cycles N
#                          追加有界 soak，默认 20 轮；均为破坏性/长时，须显式授权）
#   l4  多节点 HA         不自动执行：打印前置与命令清单（需 provisioned 环境）
#
# 用法:
#   sudo bash scripts/lab/verify.sh --layer l2
#   sudo bash scripts/lab/verify.sh --layer l3
#   sudo bash scripts/lab/verify.sh --layer l3 --with-chaos --soak-cycles 20
#   bash  scripts/lab/verify.sh --layer l4          # 仅打印清单
#
# 约定：
#   - 任一步 FAIL 立即退出 1；每步结果同时落盘 /var/lib/firepaas-p0/verify/。
#   - 脚本头元数据（LAYER/PREREQ/DESTRUCTIVE/FROZEN）是本入口的调度依据。
#   - l2/l3 需要 root 与 FIREPAAS_MESH=eastwest（l3）；缺失即前置失败，不伪造 PASS。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="/var/lib/firepaas-p0/verify"
mkdir -p "$RUN_DIR" 2>/dev/null || RUN_DIR="${TMPDIR:-/tmp}/firepaas-verify"
RUN_ID="verify-$(date +%Y%m%d-%H%M%S)"
LOG="$RUN_DIR/$RUN_ID.log"
mkdir -p "$RUN_DIR"

LAYER=""; WITH_CHAOS=0; SOAK_CYCLES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --layer) LAYER="$2"; shift 2 ;;
    --with-chaos) WITH_CHAOS=1; shift ;;
    --soak-cycles) SOAK_CYCLES="$2"; shift 2 ;;
    *) echo "unknown arg $1 (want --layer l2|l3|l4 [--with-chaos] [--soak-cycles N])" >&2; exit 1 ;;
  esac
done
[ -n "$LAYER" ] || { echo "--layer required (l2|l3|l4)" >&2; exit 1; }

now() { date +%H:%M:%S; }
log() { echo "[verify $(now)] $*" | tee -a "$LOG"; }

TOTAL=0; FAILED=0
run_step() { # run_step <描述> <命令...>
  local desc="$1"; shift
  TOTAL=$((TOTAL + 1))
  log "STEP $TOTAL: $desc"
  if "$@" >>"$LOG" 2>&1; then
    log "  PASS: $desc"
    return 0
  fi
  FAILED=$((FAILED + 1))
  log "  FAIL: $desc (详见 $LOG)"
  return 1
}

sudo_run() { # lab 脚本大多需要 root 与显式 env（sudo 默认剥环境）。
  # 已 root 则直跑（return 无参传播退出码）；否则以 sudo env 显式带上本入口已知的白名单变量。
  if [ "$(id -u)" = "0" ]; then "$@"; return; fi
  local -a envs=()
  for v in FIREPAAS_MESH NOMAD_ADDR FABRIC_SPIKE_API_TOKEN FABRIC_SPIKE_NODE_A FABRIC_SPIKE_NODE_B \
           FABRIC_SPIKE_IMAGE_REF FABRIC_SPIKE_EDGE_METRICS FP_API_TOKEN FP_SOAK_IMAGE FP_API_ADDR; do
    [ -n "${!v:-}" ] && envs+=("$v=${!v}")
  done
  if [ ${#envs[@]} -gt 0 ]; then sudo env "${envs[@]}" "$@"; else sudo "$@"; fi
}

preflight() { # preflight <描述> <检查命令...>
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then
    log "PRECHECK OK: $desc"; return 0
  fi
  log "PRECHECK FAIL: $desc — 终止（不伪造 PASS）"
  FAILED=$((FAILED + 1)); TOTAL=$((TOTAL + 1))
  return 1
}

case "$LAYER" in
  l2)
    preflight "kvm 存在"            bash -c '[ -e /dev/kvm ]'
    preflight "dev-postgres 就绪"   docker exec dev-postgres-1 pg_isready -U firepaas
    preflight "nomad API 可达"      bash -c 'curl -sf -m 3 "${NOMAD_ADDR:-http://127.0.0.1:4646}/v1/nodes" >/dev/null'
    run_step "P0 冒烟（pull/run/exec/logs 残留检查）" sudo_run bash "$HERE/smoke-p0.sh"
    run_step "M3 验收（slot 数据面/发布回滚/无泄漏）" sudo_run bash "$HERE/e2e-m3.sh"
    run_step "M4 验收（secrets/凭证/限流/Redis 宕机）" sudo_run bash "$HERE/e2e-m4.sh"
    run_step "M5 六段验收"                          sudo_run bash "$HERE/e2e-m5.sh"
    ;;
  l3)
    export FIREPAAS_MESH="${FIREPAAS_MESH:-eastwest}"
    preflight "kvm 存在"            bash -c '[ -e /dev/kvm ]'
    preflight "mesh 模式 eastwest"  bash -c '[ "$FIREPAAS_MESH" = "eastwest" ]'
    preflight "nomad API 可达"      bash -c 'curl -sf -m 3 "${NOMAD_ADDR:-http://127.0.0.1:4646}/v1/nodes" >/dev/null'
    # 节点 ID 从 mesh 成员派生（不依赖外部注入；stage1 需 NODE_A）。
    if [ -z "${FABRIC_SPIKE_NODE_A:-}" ]; then
      nodes=$(docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc \
        "SELECT node_id FROM wg_peers WHERE node_id<>'edge-hub' ORDER BY node_id" 2>/dev/null | tr '\n' ' ')
      FABRIC_SPIKE_NODE_A=$(echo $nodes | awk '{print $1}')
      FABRIC_SPIKE_NODE_B=$(echo $nodes | awk '{print $2}')
      export FABRIC_SPIKE_NODE_A FABRIC_SPIKE_NODE_B
    fi
    if [ -z "${FABRIC_SPIKE_NODE_A:-}" ]; then
      log "PRECHECK FAIL: wg_peers 无节点（mesh 未起）"
      TOTAL=$((TOTAL+1)); FAILED=$((FAILED+1))
      log "==== 汇总（提前终止）：steps=$TOTAL failed=$FAILED ===="
      exit 1
    fi
    log "fabric 节点：A=${FABRIC_SPIKE_NODE_A:0:8} B=${FABRIC_SPIKE_NODE_B:0:8}"
    # stage2/3 前置：API token（实验室约定路径）与镜像引用（本地 registry tag）。
    [ -n "${FABRIC_SPIKE_API_TOKEN:-}" ] || FABRIC_SPIKE_API_TOKEN="$(cat /tmp/fabric-api-token 2>/dev/null || true)"
    [ -n "${FABRIC_SPIKE_API_TOKEN:-}" ] || { log "PRECHECK FAIL: FABRIC_SPIKE_API_TOKEN 未设置且 /tmp/fabric-api-token 不存在"; TOTAL=$((TOTAL+1)); FAILED=$((FAILED+1)); log "==== 汇总（提前终止）：steps=$TOTAL failed=$FAILED ===="; exit 1; }
    export FABRIC_SPIKE_API_TOKEN
    export FABRIC_SPIKE_IMAGE_REF="${FABRIC_SPIKE_IMAGE_REF:-127.0.0.1:5000/firepaas/ontime:1}"
    run_step "spike 前置门禁（check-only）" sudo_run bash "$HERE/e2e-fabric-spike.sh" --check-only
    run_step "spike stage1（host→guest ULA）" sudo_run bash "$HERE/e2e-fabric-spike.sh" --stage1
    run_step "spike stage2（跨节点 WG + eBPF 策略）" sudo_run bash "$HERE/e2e-fabric-spike.sh" --stage2
    run_step "spike stage3（edge mesh direct + 回落）" sudo_run bash "$HERE/e2e-fabric-spike.sh" --stage3
    if [ "$WITH_CHAOS" = "1" ]; then
      log "chaos 已授权（--with-chaos）：破坏性场景链开始"
      run_step "chaos-fabric 全场景" sudo_run bash "$HERE/chaos-fabric.sh" --scenario all
    else
      log "跳过 chaos（--with-chaos 未指定；破坏性操作须显式授权）"
    fi
    if [ "$SOAK_CYCLES" -gt 0 ] 2>/dev/null; then
      run_step "soak-fabric ${SOAK_CYCLES} 轮分配/释放" sudo_run env \
        FP_API_TOKEN="${FP_API_TOKEN:?set FP_API_TOKEN}" \
        FP_SOAK_IMAGE="${FP_SOAK_IMAGE:?set FP_SOAK_IMAGE}" \
        FP_API_ADDR="${FP_API_ADDR:-http://127.0.0.1:8083}" \
        bash "$HERE/soak-fabric.sh" --cycles "$SOAK_CYCLES" --hours 0
    fi
    ;;
  l4)
    log "L4 多节点 HA 需要 provisioned 环境（bootstrap-lab.sh 3 server + 2 compute），不自动执行。"
    log "清单（逐项按对应 runbook 授权执行）："
    log "  1. bash scripts/bootstrap-lab.sh                  # 环境引导"
    log "  2. sudo bash scripts/lab/e2e-multinode-scheduler.sh"
    log "  3. sudo bash scripts/lab/chaos-node-failover.sh"
    log "  4. sudo bash scripts/lab/chaos-control-quorum.sh"
    log "  5. sudo bash scripts/lab/e2e-vip-failover.sh"
    log "  6. sudo bash scripts/lab/dr-rehearsal.sh"
    log "  7. bash scripts/lab/soak-ha-72h.sh                # 72h，见 runbook-72h-soak.md"
    log "  8. bash scripts/lab/observe-30d.sh                # 30 天观察门"
    log "L4 清单打印完成（无执行）。"
    exit 0
    ;;
  *) echo "unknown layer $LAYER (l2|l3|l4)" >&2; exit 1 ;;
esac

log "==== 汇总：steps=$TOTAL failed=$FAILED ===="
[ "$FAILED" = "0" ] || exit 1
log "verify $LAYER PASS"
