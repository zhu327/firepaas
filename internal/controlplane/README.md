# control-plane 内部结构（M2 已落地：发现/调度/预约/leader/决策表）

```
cmd/api/                        # 入口：依赖装配 + 进程生命周期（HTTP 由 httpapi 承载）
internal/controlplane/httpapi/  # 控制面 HTTP 层：路由表（api.go 的 Register 是唯一登记点）、
                                # 各领域 handler、鉴权/审计/限流中间件；不读进程 env，
                                # 可变准入参数由 cmd/api 装配期解析传入
internal/controlplane/db/       # PG 连接 + 按文件名记录的嵌入式历史 migrations（当前 0001-0039）
internal/controlplane/store/    # PG desired/operations/nodes/scheduler_events 权威
internal/controlplane/catalog/  # Redis route/location 投影（可重建，ADR-0005）
internal/controlplane/agentclient/ # agent gRPC 客户端（mTLS fail-closed）
internal/controlplane/nodemanager/ # Nomad discovery + 节点状态机 + 连接池（M2.1）
internal/controlplane/leader/   # PG advisory lock 选主（M2a，ADR-0007）
internal/controlplane/controller/  # 独立调和环（controller.go 的 Run/reconcileLoops）：
                                # operations/observed/rollout/evacuation/node-resources/
                                # autoscale/route-rebuild/stale-claims/retention/gc/scrub/fabric-gc
                                # 每环自有 goroutine 与节拍，单环阻塞/panic 不饿死其他环；
                                # controller/autoscale.go  # ADR-0041 并发弹性决策（10s 拍，只经 CAS 写 desired）
internal/controlplane/routepublisher/ # 纯 route 派生 + PG-first/Redis-second 发布
internal/controlplane/placement/ # 一致节点快照 + 调度/PG 配额/Redis 预约/派发节点提交
internal/controlplane/reservations/ # Redis Lua 预约：配额/pending TTL/重建（M2.4）
internal/scheduler/             # 先过滤后打分 Best-of-K（ADR-0002/0009）
```

历史设计曾参考 e2b-dev/infra 的 placement、nodemanager、reservations 与 catalog 分层；当前行为以本仓库代码、测试、架构文档和 ADR 为准，不依赖 sibling checkout。
