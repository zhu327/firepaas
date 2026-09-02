# control-plane 内部结构（M2 已落地：发现/调度/预约/leader/决策表）

```
cmd/api/                        # 入口：REST machines 最小 CRUD + /v1/nodes + /metrics
internal/controlplane/db/       # PG 连接 + 按文件名记录的嵌入式历史 migrations（当前 0001-0031）
internal/controlplane/store/    # PG desired/operations/nodes/scheduler_events 权威
internal/controlplane/catalog/  # Redis route/location 投影（可重建，ADR-0005）
internal/controlplane/agentclient/ # agent gRPC 客户端（mTLS fail-closed）
internal/controlplane/nodemanager/ # Nomad discovery + 节点状态机 + 连接池（M2.1）
internal/controlplane/leader/   # PG advisory lock 选主（M2a，ADR-0007）
internal/controlplane/controller/  # 操作 reconcile + 决策表 R1-R8；触发 route 重建
internal/controlplane/routepublisher/ # 纯 route 派生 + PG-first/Redis-second 发布
internal/controlplane/placement/ # 一致节点快照 + 调度/PG 配额/Redis 预约/派发节点提交
internal/controlplane/reservations/ # Redis Lua 预约：配额/pending TTL/重建（M2.4）
internal/scheduler/             # 先过滤后打分 Best-of-K（ADR-0002/0009）
internal/controlplane/api/      # 完整 OpenAPI 路由（M3 起填）
internal/controlplane/auth/     # JWT + API key + scopes（M5）
internal/controlplane/reconcile/# 按 app 分区的对账（M2b 后需要时再拆）
```

历史设计曾参考 e2b-dev/infra 的 placement、nodemanager、reservations 与 catalog 分层；当前行为以本仓库代码、测试、架构文档和 ADR 为准，不依赖 sibling checkout。
