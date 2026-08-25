# control-plane 内部结构(P1-P2 填充)

```
control-plane/
├── cmd/api/                    # 入口:REST + 内部 gRPC + 后台对账循环
├── internal/api/               # OpenAPI 路由:projects/apps/deployments/machines/images/volumes/secrets
├── internal/auth/              # JWT + API key + scopes(迁移 hypeman lib/scopes)
├── internal/db/                # Postgres migrations + 查询(sqlc 或 pgx)
├── internal/store/             # PG desired/operations + Redis 可重建 route/lease 投影
├── internal/controllers/       # machine/deployment/route 调和控制器
├── internal/nodemanager/       # Nomad 服务发现 -> 节点连接 -> 20s 同步(移植 e2b)
├── internal/scheduler/         # Best-of-K 放置算法(移植 e2b)
├── internal/reservations/      # Redis Lua 预约(单写者稳定后启用)
└── internal/reconcile/         # 对账：PG desired <-> agent observed；重建 Redis、处理 orphan
```

关键参考(本地 ../infra):
- placement:  `infra/packages/api/internal/orchestrator/placement/`
- nodemanager: `infra/packages/api/internal/orchestrator/nodemanager/`
- reservations: `infra/packages/api/internal/sandbox/reservations/`
- catalog:     `infra/packages/shared/pkg/sandbox-catalog/`
