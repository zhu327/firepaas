# control-plane 内部结构（M1.5 已落地，M2 补齐调度/对账）

```
cmd/api/                        # 入口：REST machines 最小 CRUD + controller 启动
internal/controlplane/db/       # PG 连接 + 嵌入式 migrations
internal/controlplane/store/    # PG desired/operations（M1 单实例）
internal/controlplane/catalog/  # Redis route/location 投影（可重建，ADR-0005）
internal/controlplane/agentclient/ # 单节点 agent gRPC 客户端（mTLS 可选）
internal/controlplane/controller/  # operation reconcile + observed 同步 + 投影发布
internal/controlplane/api/      # 完整 OpenAPI 路由（M2 起填）
internal/controlplane/auth/     # JWT + API key + scopes（M1.3 后）
internal/controlplane/nodemanager/ # Nomad 服务发现（M2）
internal/scheduler/             # Best-of-K 放置算法（M2）
internal/controlplane/reservations/ # Redis Lua 预约（M2）
internal/controlplane/reconcile/    # orphan/ACK 丢失决策表（M2）
```

关键参考(本地 ../infra)：
- placement:  `infra/packages/api/internal/orchestrator/placement/`
- nodemanager: `infra/packages/api/internal/orchestrator/nodemanager/`
- reservations: `infra/packages/api/internal/sandbox/reservations/`
- catalog:     `infra/packages/shared/pkg/sandbox-catalog/`
