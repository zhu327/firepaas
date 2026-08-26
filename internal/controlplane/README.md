# control-plane 内部结构（M2 已落地：发现/调度/预约/leader/决策表）

```
cmd/api/                        # 入口：REST machines 最小 CRUD + /v1/nodes + /metrics
internal/controlplane/db/       # PG 连接 + 嵌入式 migrations（0001-0005）
internal/controlplane/store/    # PG desired/operations/nodes/scheduler_events 权威
internal/controlplane/catalog/  # Redis route/location 投影（可重建，ADR-0005）
internal/controlplane/agentclient/ # agent gRPC 客户端（mTLS fail-closed）
internal/controlplane/nodemanager/ # Nomad discovery + 节点状态机 + 连接池（M2.1）
internal/controlplane/leader/   # PG advisory lock 选主（M2a，ADR-0007）
internal/controlplane/controller/  # 操作 reconcile + 决策表 R1-R8 + route 重建
internal/controlplane/reservations/ # Redis Lua 预约：配额/pending TTL/重建（M2.4）
internal/scheduler/             # 先过滤后打分 Best-of-K（ADR-0002/0009）
internal/controlplane/api/      # 完整 OpenAPI 路由（M3 起填）
internal/controlplane/auth/     # JWT + API key + scopes（M5）
internal/controlplane/reconcile/# 按 app 分区的对账（M2b 后需要时再拆）
```

关键参考(本地 ../infra)：
- placement:  `infra/packages/api/internal/orchestrator/placement/`
- nodemanager: `infra/packages/api/internal/orchestrator/nodemanager/`
- reservations: `infra/packages/api/internal/sandbox/reservations/`
- catalog:     `infra/packages/shared/pkg/sandbox-catalog/`
