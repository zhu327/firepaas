#!/usr/bin/env bash
# upgrade-agentd.sh：M5.5（mvp-plan §9.5）drain/rebuild 版 agentd 升级演练。
#
# 承诺语义：drain（不再放新车）+ rebuild（job restart 重建 agentd）。
# 不承诺零中断：agentd SIGTERM 会带走本机运行中 VM（M3 已知行为），
# controller 观察到 UNKNOWN 后自动重建——全流程 e2e-m5 E 段验收。
#
# 用法：sudo bash scripts/lab/upgrade-agentd.sh
set -euo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[upgrade $(TS)] $*"; }

NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
API_ADDR="${FP_API_ADDR:-http://127.0.0.1:8081}"
API_TOKEN="${FP_API_TOKEN:?FP_API_TOKEN required}"
LAB_BIN="${LAB_BIN:-/home/zty/.local/firepaas-lab/bin}"
GO="${FIREPAAS_GO:-/home/zty/.local/firepaas-lab/go/bin/go}"
REPO="${REPO:-/home/zty/Learn/firepaas}"

say "1/5 构建 agentd"
cd "$REPO"
PATH="$(dirname "$GO"):$PATH" CGO_ENABLED=0 go build -o "$LAB_BIN/agentd.new" ./cmd/agentd

say "2/5 排水节点（node drain）"
NODE_ID=$(curl -s -H "Authorization: Bearer $API_TOKEN" "$API_ADDR/v1/nodes" |
  python3 -c 'import json,sys; ns=json.load(sys.stdin)["nodes"]; n=ns[0] if ns else {}; print(n.get("ID") or n.get("id") or "")')
[[ -n "$NODE_ID" ]] || { say "no node registered"; exit 1; }
curl -s -XPOST -H "Authorization: Bearer $API_TOKEN" "$API_ADDR/v1/nodes/$NODE_ID/drain" >/dev/null
say "node $NODE_ID draining"
sleep 5

say "3/5 rebuild：原子替换二进制并重启 system job"
mv "$LAB_BIN/agentd.new" "$LAB_BIN/agentd"
timeout 60 nomad job restart -on-error fail firepaas-agentd
for _ in $(seq 1 40); do
  STATUS=$(timeout 10 nomad job status -short firepaas-agentd 2>/dev/null | awk '$2=="running"{print $2}')
  [[ "$STATUS" == "running" ]] && break
  sleep 3
done

say "4/5 恢复节点（node ready）"
for _ in $(seq 1 30); do
  R=$(curl -s -XPOST -H "Authorization: Bearer $API_TOKEN" "$API_ADDR/v1/nodes/$NODE_ID/ready")
  [[ "$R" == *"ready"* ]] && break
  sleep 3
done

say "5/5 对账收敛检查（nodes READY + 无 PENDING 积压）"
sleep 15
curl -s -H "Authorization: Bearer $API_TOKEN" "$API_ADDR/v1/nodes" | python3 -c '
import json,sys
ns=json.load(sys.stdin)["nodes"]
bad=[(n.get("ID") or n.get("id")) for n in ns
     if (n.get("Draining") or n.get("draining")) or (n.get("Status") or n.get("status"))!="READY"]
print("PASS nodes converged" if not bad else f"FAIL {bad}")'
curl -s -H "Authorization: Bearer $API_TOKEN" "$API_ADDR/v1/operations?status=PENDING" |
  python3 -c 'import json,sys; ops=json.load(sys.stdin)["operations"]; print("PASS pending=0" if not ops else f"WARN pending={len(ops)}")'
say "upgrade rehearsal done"
