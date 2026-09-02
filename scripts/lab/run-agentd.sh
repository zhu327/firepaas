#!/usr/bin/env bash
# 部署 M1 单机 agentd system job（root）。用法: sudo bash scripts/lab/run-agentd.sh
# 前置：Nomad 已 root 运行（root-setup.sh）；agentd 二进制已构建（见 build-agentd.sh 或 make build）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"

export PATH="$HOME/.local/firepaas-lab/bin:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"

BIN="$HOME/.local/firepaas-lab/bin/agentd"
CONFIG="$HERE/agentd.yaml"
READY_TIMEOUT="${FIREPAAS_AGENT_READY_TIMEOUT:-10m}"

[[ -x "$BIN" ]] || { echo "ERROR: $BIN 不存在，先在 firepaas 根目录 make build 并复制产物" >&2; exit 1; }
[[ -r "$CONFIG" ]] || { echo "ERROR: $CONFIG 不可读" >&2; exit 1; }
curl -fsS "$NOMAD_ADDR/v1/status/leader" >/dev/null || { echo "ERROR: Nomad 不可达" >&2; exit 1; }

cd "$ROOT_DIR"
JOB_VARS=(-var "repo_root=$ROOT_DIR" -var "lab_bin=$(dirname "$BIN")" -var "agentd_binary_sha256=$(sha256sum "$BIN" | awk '{print $1}')")
echo "==> nomad job plan"
nomad job plan "${JOB_VARS[@]}" iac/nomad/agentd-single.hcl || echo "    (plan rc=$?, Nomad 2.x 在有待提交变更时返回 1，继续 run)"

echo "==> nomad job run"
nomad job run -detach "${JOB_VARS[@]}" iac/nomad/agentd-single.hcl >/dev/null

JOB_VERSION="$(nomad job inspect -json firepaas-agentd | python3 -c 'import json,sys; print(json.load(sys.stdin)["Version"])')"
current_alloc() {
  nomad job allocs -json firepaas-agentd 2>/dev/null | python3 -c '
import json, sys
version = int(sys.argv[1])
allocs = json.load(sys.stdin)
current = [a for a in allocs if a.get("DesiredStatus") == "run" and a.get("JobVersion") == version]
current.sort(key=lambda a: a.get("CreateIndex", 0), reverse=True)
print(current[0]["ID"] if current else "")
' "$JOB_VERSION"
}

ALLOC_ID=""
for _ in $(seq 1 30); do
  ALLOC_ID="$(current_alloc)"
  [[ -n "$ALLOC_ID" ]] && break
  sleep 1
done
[[ -n "$ALLOC_ID" ]] || { echo "ERROR: job version $JOB_VERSION 未创建 alloc" >&2; exit 1; }
echo "==> 等待当前 agentd alloc 就绪：${ALLOC_ID:0:8}（job version $JOB_VERSION，timeout $READY_TIMEOUT）"
DEADLINE=$(( $(date +%s) + $(python3 - "$READY_TIMEOUT" <<'PY'
import re, sys
m = re.fullmatch(r"([1-9][0-9]*)([smh])", sys.argv[1])
if not m:
    raise SystemExit("FIREPAAS_AGENT_READY_TIMEOUT must match <positive integer>[s|m|h]")
print(int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2)])
PY
) ))
while (( $(date +%s) < DEADLINE )); do
  status="$(nomad alloc status -json "$ALLOC_ID" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("ClientStatus", ""))' 2>/dev/null || true)"
  case "$status" in
    running)
      if (echo > /dev/tcp/127.0.0.1/5108) 2>/dev/null; then
        echo "agentd gRPC OK"
        exit 0
      fi
      ;;
    failed|lost)
      break
      ;;
  esac
  sleep 2
done
echo "ERROR: 当前 agentd alloc ${ALLOC_ID:0:8} 未就绪。alloc 状态/日志：" >&2
nomad alloc status "$ALLOC_ID" >&2 || true
nomad alloc logs "$ALLOC_ID" agentd 2>&1 | tail -80 >&2 || true
exit 1
