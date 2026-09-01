#!/usr/bin/env bash
# 部署 hypeman P0 job（root）。用法: sudo bash scripts/lab/run-p0.sh
# 前置：Nomad 已由 scripts/lab/start.sh 启动；hypeman 已由 build-hypeman.sh 构建。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"

export PATH="$HOME/.local/firepaas-lab/bin:$HOME/.local/firepaas-lab/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"

BIN="$HOME/.local/firepaas-lab/bin/hypeman"
CONFIG="$HERE/hypeman-p0.yaml"

[[ -x "$BIN" ]] || { echo "ERROR: $BIN 不存在，先运行 bash scripts/lab/build-hypeman.sh" >&2; exit 1; }
[[ -r "$CONFIG" ]] || { echo "ERROR: $CONFIG 不可读" >&2; exit 1; }
curl -fsS "$NOMAD_ADDR/v1/status/leader" >/dev/null || { echo "ERROR: Nomad 不可达" >&2; exit 1; }

cd "$ROOT_DIR"
echo "==> nomad job plan"
nomad job plan iac/nomad/hypeman-p0.hcl || echo "    (plan rc=$?, Nomad 2.x 在有待提交变更时返回 1，继续 run)"

echo "==> nomad job run"
nomad job run iac/nomad/hypeman-p0.hcl

echo "==> 等待 alloc"
for _ in $(seq 1 30); do
  ALLOC_ID="$(nomad job allocs -json firepaas-hypeman-p0 2>/dev/null \
    | python3 -c 'import json,sys; a=json.load(sys.stdin); print(a[0]["ID"] if a else "")' 2>/dev/null || true)"
  [[ -n "$ALLOC_ID" ]] && break
  sleep 2
done
[[ -n "${ALLOC_ID:-}" ]] || { echo "ERROR: 未找到 alloc" >&2; nomad job status firepaas-hypeman-p0 >&2; exit 1; }

echo "==> alloc 状态"
nomad alloc status "$ALLOC_ID"
echo
echo "==> 检查 /health"
for _ in $(seq 1 60); do
  if curl -fsS --max-time 3 http://127.0.0.1:4973/health >/dev/null 2>&1; then
    echo "hypeman /health OK"
    exit 0
  fi
  sleep 2
done
echo "ERROR: hypeman /health 不可达。日志：" >&2
nomad alloc logs "$ALLOC_ID" hypeman 2>&1 | tail -80 >&2 || true
exit 1
