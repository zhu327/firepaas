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

[[ -x "$BIN" ]] || { echo "ERROR: $BIN 不存在，先在 firepaas 根目录 make build 并复制产物" >&2; exit 1; }
[[ -r "$CONFIG" ]] || { echo "ERROR: $CONFIG 不可读" >&2; exit 1; }
curl -fsS "$NOMAD_ADDR/v1/status/leader" >/dev/null || { echo "ERROR: Nomad 不可达" >&2; exit 1; }

cd "$ROOT_DIR"
echo "==> nomad job plan"
nomad job plan -var "repo_root='$ROOT_DIR'" -var "lab_bin='$(dirname "$BIN")'" iac/nomad/agentd-single.hcl || echo "    (plan rc=$?, Nomad 2.x 在有待提交变更时返回 1，继续 run)"

echo "==> nomad job run"
nomad job run -var "repo_root='$ROOT_DIR'" -var "lab_bin='$(dirname "$BIN")'" iac/nomad/agentd-single.hcl || echo "    (run rc=$?, 继续检查 alloc 就绪；旧 deployment 可能仍标记 failed)"

echo "==> 等待 agentd gRPC :5108"
for _ in $(seq 1 60); do
  if (echo > /dev/tcp/127.0.0.1/5108) 2>/dev/null; then
    echo "agentd gRPC OK"
    exit 0
  fi
  sleep 2
done
echo "ERROR: agentd :5108 不可达。alloc 日志：" >&2
ALLOC_ID="$(nomad job allocs -json firepaas-agentd 2>/dev/null \
  | python3 -c 'import json,sys; a=json.load(sys.stdin); print(a[0]["ID"] if a else "")' 2>/dev/null || true)"
[[ -n "$ALLOC_ID" ]] && nomad alloc logs "$ALLOC_ID" agentd 2>&1 | tail -80 >&2 || true
exit 1
