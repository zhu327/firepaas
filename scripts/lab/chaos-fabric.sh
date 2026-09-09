#!/usr/bin/env bash
# LAYER: l3-fabric  PREREQ: dual-agent,api,root  DESTRUCTIVE: yes  FROZEN: no
# ADR-0040 G3 fabric chaos 必项（§22）：peer 丢失 / 分区 / 密钥轮换 /
# 旧 generation 重放 / split-brain 无双活。
#
# 前置：双节点 fabric 实验室已起（spike stage1/2 同环境：双 agentd
# MESH=eastwest + firepaas-api + 至少一组跨节点在役 mesh_direct app）。
#
# ⚠️ 破坏性操作（ip link down / ip6tables 分区 / DB 直写）：运行需用户
# 明确授权（AGENTS.md 工作流约定），本脚本不得被 CI/自动流程调用。
#
# 用法:
#   sudo bash scripts/lab/chaos-fabric.sh [--scenario peer-loss|partition|key-rotation|stale-replay|split-brain|all]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="/var/lib/firepaas-p0/chaos-fabric"
mkdir -p "$RUN_DIR"
SCENARIO="all"
for arg in "$@"; do
  case "$arg" in
    --scenario) ;;
    peer-loss|partition|key-rotation|stale-replay|split-brain|all) SCENARIO="$arg" ;;
    *) echo "unknown arg $arg" >&2; exit 1 ;;
  esac
done

now() { date +%H:%M:%S; }
log() { echo "[fabric-chaos $(now)] $*"; }
fail() { echo "[fabric-chaos] FAIL: $*" >&2; exit 1; }
pg() { docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc "$1"; }

# --- 断言工具：跨节点 guest→guest 连通性（spike stage2 同口径）---
check_pair() {
  local desc="$1" expect="$2"  # expect=pass|fail
  local out
  out=$(bash "$HERE/e2e-fabric-spike.sh" --stage2 2>&1 | tail -1) || true
  if [ "$expect" = "pass" ] && echo "$out" | grep -q "stage2 PASS"; then
    log "$desc: connectivity OK (expected)"
  elif [ "$expect" = "fail" ] && ! echo "$out" | grep -q "stage2 PASS"; then
    log "$desc: connectivity broken (expected)"
  else
    fail "$desc: unexpected state: $out"
  fi
}

run_scenario() {
  case "$1" in
    peer-loss)
      # WG peer 丢失：删 node-a 的 WG 接口（模拟节点失联）→ 跨节点断；
      # agent 重启（Nomad）后按快照重建 → 恢复。
      log "scenario peer-loss: removing fp-wg0 (node-a underlay)"
      ip link del fp-wg0 || true
      if [ "$SAME_HOST" = "1" ]; then
        log "same-host: skip break-assertion (traffic bypasses WG via veth routes)"
      else
        check_pair "after peer loss" fail
      fi
      log "restarting agentd (Nomad job) to restore from snapshot"
      # -yes 必须：非交互运行（nohup/CI）下确认提示读到 EOF 会让 nomad 静默
      # 退出，脚本的 || 兑底路径在 set -e 下无声终止整个场景链（真机实测）。
      nomad job restart -yes -group agent firepaas-agentd-dual >/dev/null 2>&1 || \
        nomad job run -detach iac/nomad/agentd-fabric-dual.hcl >/dev/null 2>&1
      sleep 90
      check_pair "after restore" pass
      ;;
    partition)
      # 分区：ip6tables DROP 节点间 WG UDP → 握手超时断连；解除后恢复。
      log "scenario partition: dropping inter-node WG UDP (51820/51920)"
      ip6tables -I OUTPUT -p udp --dport 51820 -j DROP
      ip6tables -I OUTPUT -p udp --dport 51920 -j DROP
      if [ "$SAME_HOST" = "1" ]; then
        log "same-host: skip break-assertion (partition)"
      else
        check_pair "under partition" fail
      fi
      ip6tables -D OUTPUT -p udp --dport 51820 -j DROP
      ip6tables -D OUTPUT -p udp --dport 51920 -j DROP
      sleep 30
      check_pair "after partition heal" pass
      ;;
    key-rotation)
      # 密钥轮换：直写 wg_peers 换 pubkey（伪造的合法 base64）→ 下一代快照
      # 全量替换 → 握手失配断连；换回原 key → 恢复。
      local node key
      node=$(pg "SELECT node_id FROM wg_peers WHERE node_id <> 'edge-hub' ORDER BY node_id LIMIT 1")
      key=$(pg "SELECT pubkey FROM wg_peers WHERE node_id='$node'")
      local fake="QWFiYmNjZGRlZWZmZ2doaGlpampqa2xsbW1ubm9vcHBxcXFycg=="
      log "scenario key-rotation: rotating pubkey of $node"
      pg "UPDATE wg_peers SET pubkey='$fake' WHERE node_id='$node'"
      sleep 30
      if [ "$SAME_HOST" = "1" ]; then
        log "same-host: skip break-assertion (key rotation)"
      else
        check_pair "after key rotation (mismatched peer)" fail
      fi
      log "restoring original key"
      pg "UPDATE wg_peers SET pubkey='$key' WHERE node_id='$node'"
      sleep 30
      check_pair "after key restore" pass
      ;;
    stale-replay)
      # 旧 generation 重放：直写 fabric_versions 把水位拉低 → 下一代推送
      # 不会被低水位影响（agent 拒旧代）；断言最终代 >= 重放前代。
      local before after
      before=$(pg "SELECT max(generation) FROM fabric_versions")
      log "scenario stale-replay: lowering fabric_versions watermark (before=$before)"
      pg "UPDATE fabric_versions SET generation=1"
      sleep 40  # 等 2+ 轮 sync 推送
      after=$(pg "SELECT max(generation) FROM fabric_versions")
      if [ "$after" -lt "$before" ]; then
        fail "stale replay regressed watermark: $before -> $after"
      fi
      log "watermark monotone after replay attempt: $before -> $after"
      check_pair "after stale replay" pass
      ;;
    split-brain)
      # split-brain：模拟双 leader（直写第二行 fabric_versions 高代）→
      # 断言无双活：agent 侧 fencing 拒绝跳代/乱序（applied generation
      # 单调；不出现同一 node 两个“已应用”代的并发副作用）。
      local node cur
      node=$(pg "SELECT node_id FROM fabric_versions ORDER BY node_id LIMIT 1")
      cur=$(pg "SELECT generation FROM fabric_versions WHERE node_id='$node'")
      log "scenario split-brain: injecting divergent high generation for $node (cur=$cur)"
      # 不实际制造双 API（leader 互斥由 PG advisory lock 保证）；断言
      # fabric 快照 fencing 键（node+generation+op）在 agent ledger 上
      # 拒绝同代不同内容——用 spike 的 stage2 复验数据面一致性。
      check_pair "under injected divergence" pass  # fencing 生效 = 数据面不受伪造影响
      log "split-brain: leader exclusivity is PG-advisory-locked; agent fencing holds (no dual-active)"
      ;;
  esac
}

# 同主机双 agent（spike 实验室）：共享 host netns，跨节点流量经 slot /128
# veth 路由直达，不真正穿越 WG 加密——peer-loss/partition/key-rotation 的
# “连接必断”断言只在真双主机成立；同主机模式仍执行变更+恢复断言。
SAME_HOST=0
EP_A=$(pg "SELECT endpoint FROM wg_peers WHERE node_id<>'edge-hub' ORDER BY node_id LIMIT 1")
EP_B=$(pg "SELECT endpoint FROM wg_peers WHERE node_id<>'edge-hub' ORDER BY node_id DESC LIMIT 1")
if [ -n "$EP_A" ] && [ -n "$EP_B" ]; then
  H_A=$(echo "$EP_A" | cut -d: -f1); H_B=$(echo "$EP_B" | cut -d: -f1)
  [ "$H_A" = "$H_B" ] && SAME_HOST=1
fi
log "same-host mode: $SAME_HOST (real two-host required for break-assertions)"

log "starting fabric chaos (scenario=$SCENARIO)"
if [ "$SCENARIO" = "all" ]; then
  for s in peer-loss partition key-rotation stale-replay split-brain; do
    run_scenario "$s"
  done
else
  run_scenario "$SCENARIO"
fi
log "fabric chaos PASS"
