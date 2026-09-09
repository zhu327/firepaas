#!/usr/bin/env bash
# LAYER: l1-base  PREREQ: -  DESTRUCTIVE: no  FROZEN: no
# 检查单机实验室状态。用法: bash scripts/lab/status.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

echo "== Nomad leader:"
curl -fsS "$NOMAD_ADDR/v1/status/leader" 2>/dev/null || echo "  (unreachable)"
echo
echo "== Nomad node pools:"
nomad node pool list 2>/dev/null || echo "  (unavailable)"
echo
echo "== Nomad nodes:"
nomad node status 2>/dev/null || echo "  (unavailable)"
echo
echo "== firepaas-hypeman-p0 allocations:"
nomad job status firepaas-hypeman-p0 2>/dev/null || echo "  (not running / not deployed)"
