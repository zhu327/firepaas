#!/usr/bin/env bash
# evacuate/rebuild agentd 升级演练（v1.1，ADR-0021）。
# 与 M5.5 版的区别：drain 时带 {"evacuate": true}——controller 逐实例驱离
# 存量 machine（换代重建到其它节点）→ 节点 machine 归零后才重启 Nomad job，
# 消除“agentd SIGTERM 带走全部 VM”的停机窗口（M3 已知行为）。
# 单节点实验室：驱离后重建仍落回本节点（重启后 ready 恢复放置）；
# 多节点形态下重建自然落其它节点（本脚本不做假设）。
# 失败时严格退出；节点只会在重启成功且对账通过后 ready。
set -euo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[upgrade $(TS)] $*"; }
die() { say "FAIL $*" >&2; exit 1; }

NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
API_ADDR="${FP_API_ADDR:-http://127.0.0.1:8081}"
API_TOKEN="${FP_API_TOKEN:?FP_API_TOKEN required}"
LAB_BIN="${LAB_BIN:-$HOME/.local/firepaas-lab/bin}"
GO="${FIREPAAS_GO:-$HOME/.local/firepaas-lab/go/bin/go}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "$HERE/../.." && pwd)}"
NOMAD_JOB="${FIREPAAS_AGENT_NOMAD_JOB:-firepaas-agentd}"

api() { curl --fail-with-body -sS --connect-timeout 5 --max-time 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
node_state() { api "$API_ADDR/v1/nodes" | python3 -c '
import json,sys
node_id=sys.argv[1]; nodes=json.load(sys.stdin).get("nodes") or []
for n in nodes:
 if n.get("ID", n.get("id")) == node_id:
  print((n.get("Status", n.get("status", ""))).upper(), str(bool(n.get("Draining", n.get("draining", False)))).lower()); break
else: raise SystemExit("node absent")' "$1"; }

command -v nomad >/dev/null || die "nomad not found"
[[ -x "$GO" ]] || die "Go binary not executable: $GO"
[[ -d "$REPO" ]] || die "repository not found: $REPO"
api "$API_ADDR/v1/health" >/dev/null
curl --fail-with-body -sS --connect-timeout 5 --max-time 10 "$NOMAD_ADDR/v1/status/leader" >/dev/null

say "1/5 build agentd"
cd "$REPO"
PATH="$(dirname "$GO"):$PATH" CGO_ENABLED=0 "$GO" build -o "$LAB_BIN/agentd.new" ./cmd/agentd
[[ -s "$LAB_BIN/agentd.new" ]] || die "agentd build produced no binary"

say "2/5 drain node (v1.1 ADR-0021；单节点降级常用 drain)"
NODE_COUNT=$(api "$API_ADDR/v1/nodes" | python3 -c 'import json,sys
ns=json.load(sys.stdin).get("nodes") or []
print(sum(1 for n in ns if n.get("Status")=="HEALTHY"))')
NODE_ID=$(api "$API_ADDR/v1/nodes" | python3 -c 'import json,sys
ns=json.load(sys.stdin).get("nodes") or []
n=next((n for n in ns if n.get("Status")=="HEALTHY" and not n.get("Draining", False)), None)
print((n.get("ID") or n.get("id")) if n else "")')
[[ -n "$NODE_ID" ]] || die "no registered node"
if [[ "${NODE_COUNT:-0}" -le 1 ]]; then
  # ADR-0021 的驱离要求 replacement 在“非源节点”服务——单节点无可达目标，
  # evacuate=true 会结构性死锁（验收实测 600s 超時）。降级为常用 drain：
  # 重启后 controller R3 重建（M3 已知行为：agentd SIGTERM 带走本机 VM）。
  say "    single compute node: skip evacuate (ADR-0021 requires a second node)"
  EVAC_MODE=plain
  api -X POST -H 'Content-Type: application/json' -d '{}' \
    "$API_ADDR/v1/nodes/$NODE_ID/drain" >/dev/null
else
  EVAC_MODE=evacuate
  api -X POST -H 'Content-Type: application/json' -d '{"evacuate": true}' \
    "$API_ADDR/v1/nodes/$NODE_ID/drain" >/dev/null
fi
for _ in $(seq 1 20); do
  read -r STATUS DRAINING < <(node_state "$NODE_ID")
  [[ "$DRAINING" == true ]] && break
  sleep 2
done
[[ "${DRAINING:-}" == true ]] || die "node did not enter draining state"
# 驱离等待：controller 每 5s 推进一个 machine（删除→换代重建→READY）。
# 实验室已缓存镜像冷启动 p95 <5s + 探针周期；每副本按 ~30s 预算。
# 驱离完成判定：节点上期望存活的 machine 数为 0（ADR-0021 完成语义）。
machines_on_node() {
  api "$API_ADDR/v1/machines" | python3 -c '
import json,sys
node_id=sys.argv[1]
machines=json.load(sys.stdin).get("machines") or []
alive=[m for m in machines if m.get("DesiredState", m.get("desired_state")) not in ("DELETED",)]
on_node=[m for m in alive if (m.get("NodeID") or m.get("node_id") or "") == node_id]
print(len(on_node))' "$NODE_ID"
}
EVAC_TIMEOUT="${FIREPAAS_EVACUATE_TIMEOUT:-600}"
EVAC_DEADLINE=$(( $(date +%s) + EVAC_TIMEOUT ))
if [[ "$EVAC_MODE" == "evacuate" ]]; then
  while :; do
    N=$(machines_on_node) || N=99
    [[ "$N" == "0" ]] && break
    [[ $(date +%s) -ge $EVAC_DEADLINE ]] && die "evacuate did not drain node in ${EVAC_TIMEOUT}s (machines=$N)"
    sleep 5
  done
  say "    node evacuated (0 machines remain)"
else
  say "    node draining (plain; machines 假定重建于 R3)"
fi

say "3/5 atomically replace binary and restart $NOMAD_JOB"
mv -f "$LAB_BIN/agentd.new" "$LAB_BIN/agentd"
# M5 修复（实测踩坑）：drain 后失败必须自动恢复 ready，否则节点永久
# draining，后续所有放置被过滤（e2e 连续两轮卡死根因）。
restore_ready() {
  [[ -n "${NODE_ID:-}" ]] || return 0
  api -X POST "$API_ADDR/v1/nodes/$NODE_ID/ready" >/dev/null 2>&1 || true
}
trap restore_ready EXIT
timeout 60 nomad job restart -address="$NOMAD_ADDR" -on-error fail "$NOMAD_JOB"
for _ in $(seq 1 40); do
  timeout 10 nomad job status -address="$NOMAD_ADDR" "$NOMAD_JOB" >/dev/null && break
  sleep 3
done
# Nomad status itself is insufficient: wait until API has rediscovered a HEALTHY,
# non-draining node（firepaas 状态机是 HEALTHY/UNKNOWN，不是 READY——M5 实测修复）.
say "4/5 restore node"
api -X POST "$API_ADDR/v1/nodes/$NODE_ID/ready" >/dev/null
# M5 实测：agentd 重启→Nomad 重注册→nodemanager 发现→READY 全链路冷启动
# 可达 ~145s（实验室），原 120s 窗口偶发误报。放宽到 240s。
for _ in $(seq 1 80); do
  read -r STATUS DRAINING < <(node_state "$NODE_ID")
  [[ "$STATUS" == HEALTHY && "$DRAINING" == false ]] && break
  sleep 3
done
[[ "${STATUS:-}" == HEALTHY && "${DRAINING:-}" == false ]] || die "node did not converge HEALTHY after restart (status=${STATUS:-} draining=${DRAINING:-})"
trap - EXIT

say "5/5 verify operation backlog is zero"
# M5 实测：节点刚收敛时在途 op 还需 1-2 个 controller 周期落账，给 90s 等待窗。
backlog() {
  api "$API_ADDR/v1/operations?status=PENDING" | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("operations") or []))'
}
for _ in $(seq 1 30); do
  N=$(backlog) || N=99
  [[ "$N" == "0" ]] && break
  sleep 3
done
[[ "${N:-99}" == "0" ]] || die "operation backlog remains after 90s: pending=$N"
say "PASS upgrade rehearsal completed"
