# Runbook：operations 处置（告警 → 归因）

告警：`FirePaasOpsBacklog`（积压>20）/ `FirePaasAgentRPCErrors`（速率>0.1/s）。

## 快速归因（一条命令链）

```bash
export FP_API_ADDR=http://127.0.0.1:8081 FP_API_TOKEN=...
fpctl ops ls --status FAILED | head    # 最近终态失败
fpctl ops show <operation-id>          # 完整 request/result（已脱敏）+ attempts
```

## 决策表

| 现象 | 归因 | 动作 |
|---|---|---|
| kind=create 反复 FAILED 且 attempts 大 | 镜像不可解析（永久错误）或 agent 容量拒 | `fpctl ops show` 看 error；镜像问题改镜像；容量问题缩容 |
| CLAIMED 卡死 | 写者 crash/leader 切换 | 自动回收（30s RequeueStaleClaimed）；`/metrics` `firepaas_operation_stale_claims_recovered_total` 应增长 |
| RPC errors 上升 + nodes_unhealthy>0 | 节点失联 | 节点替换 runbook |
| 全部 PENDING 不执行 | controller 非 leader 或 loop 停滞 | `GET /v1/nodes` 确认 API 仍 leader；重启 firepaas-api（leader 锁自动接管）|
| pause/resume FAILED（FailedPrecondition） | 快照缺失/损坏 | controller 自动转 cold-start 重建（design）；观察新 create 是否成功 |

## 复盘要求（压测后一次，mvp-plan §9.3）

任一压测结束：导出 `GET /v1/operations?limit=500` 与 `/metrics` 快照；
按 machine_id 汇总 created→claimed→completed 时间轴（op trace 字段全在），
把 P50/P95 dispatch 延迟记入 results/，异常即归档为 events。
