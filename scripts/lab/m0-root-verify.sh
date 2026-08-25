#!/usr/bin/env bash
# 单机 M0 一键 root 验证（HITL）：准备 → root Nomad → P0 job → 冒烟。
# 用法: sudo bash scripts/lab/m0-root-verify.sh
# 基准单独跑（耗时较长）:
#   sudo bash scripts/bench-hypeman.sh cold 10
#   sudo bash scripts/bench-hypeman.sh standby 10
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "====> 1/3 root-setup"
"$HERE/root-setup.sh"

echo "====> 2/3 deploy P0 job"
"$HERE/run-p0.sh"

echo "====> 3/3 smoke"
"$HERE/smoke-p0.sh"

echo
echo "====> M0 root verify PASS"
echo "下一步（基准，各需数分钟到数十分钟）："
echo "  sudo bash scripts/bench-hypeman.sh cold 10"
echo "  sudo bash scripts/bench-hypeman.sh standby 10"
echo "  sudo bash scripts/bench-hypeman.sh uncached 3"
echo "  sudo bash scripts/bench-hypeman.sh density 16"
