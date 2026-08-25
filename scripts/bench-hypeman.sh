#!/usr/bin/env bash
# P0.3 基准测试脚本:在 compute 节点上对 hypeman(或 agentd)做冷启动/快照/密度基准。
# 输出 docs/benchmarks.md 可粘贴的 markdown 表格。
#
# 前置(重要,避免脚本跑不起来):
#   1. hypeman 以 server 模式运行(Nomad job,见 iac/nomad/hypeman-p0.hcl);
#      其配置不设置任何 ingress(内嵌 Caddy/DNS 不启动)。
#   2. hypeman CLI 是独立仓库(hypeman-cli):安装并完成 token 配置后使用;
#      无 CLI 时改走 REST + JWT(用 hypeman cmd/gen-jwt 生成 token),下方命令需相应替换。
# 用法:bash scripts/bench-hypeman.sh <hypeman-cli-binary> <data-dir>
set -euo pipefail

BIN="${1:-hypeman}"
DATA="${2:-/var/lib/firepaas-p0/hypeman}"

echo "TODO(P0.3):"
echo "  1. 冷启动:time ${BIN} run --image nginx:alpine(镜像已缓存;确认 CLI 实际参数)"
echo "  2. standby/restore 各 20 次,统计 p50/p95"
echo "  3. 密度:逐步创建 1vCPU/512MiB 实例直到内存/CPU 打满,记录最大值"
echo "  4. fork:time ${BIN} fork <instance-id>"
echo "  5. 无 CLI 时改用 REST + JWT(命令替换为 curl,经 /instances 等 API)"
echo "结果写入 docs/benchmarks.md,验收阈值见 docs/mvp-plan.md P0.3"
