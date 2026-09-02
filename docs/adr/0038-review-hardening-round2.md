# ADR-0038：评审加固第二轮（R2）安全与正确性决策集

状态：已接受（2026-09-02）
关联：ADR-0003、ADR-0005、ADR-0007、ADR-0010、ADR-0024、ADR-0035；计划与逐项实现记录见 [docs/plans/2026-09-02-review-hardening-round2.md](../plans/2026-09-02-review-hardening-round2.md)。

## 背景

两份外部独立评审（2026-09-02）确认的剩余 findings：3 个 P0（apikey 管理越权、secrets 主密钥 fail-open、agent resource admission fail-open）、约 20 个 P1，以及两项 Phase-2 投影加固（route 单调 revision、reservation epoch）。用户裁决：edge token 控制面耦合维持现状（仅文档化断流预算 SLA，写入 architecture.md §4.3）；route revision 与 reservation epoch 本轮落地。本 ADR 汇总全部安全/正确性裁决；延期项登记于计划文"延期登记"。

## 决策

1. **apikey 管理仅限全局身份**。`/v1/apikeys` 三组端点仅接受 root token 或 `project_id` 为空的 admin key；handler 层 `requireGlobalIdentity` 做第二道校验（不依赖中间件单点），project 受限 admin 一律 403。
2. **secrets 主密钥 fail-closed**。controller 派发带 `secret_refs` 的 create 时若 `cfg.Secrets == nil`（`FIREPAAS_SECRETS_MASTER_KEY` 未配置），operation 直接置 FAILED 并打告警日志/指标/用户事件，绝不发起 agent RPC、绝不创建 VM。
3. **生命周期派发严格 fence**。`processLifecycle` 使用 operation 的 `(execution_id, generation)`；派发前与 machine 当前 fence 比对，漂移即置 SUPERSEDED 终态且不发 RPC；派发后 observed 写回走 CAS，CAS 失败不记 SUCCEEDED。
4. **app 删除原子化**。墓碑 + 全部未收敛 machine 的 delete 入队在同一个 PG 事务（`SoftDeleteAppAndEnqueueDeletes`）；重复 DELETE（already_deleted 分支）幂等补发仍未收敛的机器 delete（幂等键照旧），收敛到与中途崩溃重试一致的终态。
5. **agent admission fail-closed**。resource inventory 失败时 `info.AdmissionSnapshot` 返回 `resourcesValid=false`；server create 准入对无效快照返回 gRPC `codes.Unavailable`；容量上报使用 ≤60s 新鲜的 last-known-good。
6. **pause/resume 迁移 claimed mutation 协议**。与 create/delete 共享 `LockMachine` 串行化：durable claim → 崩溃窗口内由 `ConvergePause`/`ConvergeResume` 从实例实际状态恢复收敛 → 幂等完成，不再"intentionally unlocked"。Delete 改分阶段（runtime absent → slot release → egress remove → credential drop → health remove）：VM NotFound 只代表 runtime 阶段完成，后续阶段失败独立报告可重试错误、重试时继续补做。
7. **IPv6 默认拒绝**。fp-isolation 增加 nft `ip6` family 表（slot veth 入向/转发默认 drop，无 established 放行口），与 `ip` family 同构；ensure 路径对升级节点（ip 表已存在、ip6 表缺失）补齐。
8. **fence GC 绑定机器存活**。`Fences.PruneBeforeUnlessLive`：agentd 组装期注入 liveness 回调（当前实例清单存在者视为存活），活机器的 fence 条目不被年龄 GC 回收。
9. **route 发布 revision 君主化**。新 PG 表 `route_publication_revisions(hostname PK, revision bigint)` 随发布事务递增分配；Redis catalog `ReplaceHostRoutes` Lua 以 `routerev:{hostname}` 高水位 CAS（高水位键不随投影删除，作为删除路径的墓碑记忆；旧形态条目视为 revision 0）；edge RouteCache 拒绝更低 revision 的回源投影并导出 `firepaas_edge_route_revision_rejects_total`；serve-stale 与负缓存语义不变。
10. **reservation epoch 化**。键空间 `resv:{epoch}:...`，active epoch 指针 `resv:active` 经 Lua 原子读/切；重建先写新 epoch 再切指针，旧 epoch 靠 TTL 自然过期（不再 KEYS/SCAN 全库）；`Acquire` 在脚本内先读 active epoch；对外 API（Acquire/Release/Prune/Reset 等）签名不变。

配套加固（非独立决策，记录于此）：agent proxy 的 HTTP 身份中间件 `RequireClientIdentity` 强化为必须存在 `VerifiedChains` 才信任客户端证书（与 gRPC 侧 `PeerCN` 同强度，防止 ClientAuth 放宽后悄然 fail-open）；控制面 `/readyz` 做真实依赖探活（PG SELECT 1 + Redis PING，各 ≤1s 超时），consul check 切至 `/readyz`、`/v1/health` 保留静态。

## 后果

- 投影布局增量演进：旧形态 route 条目按 revision 0 兼容、旧无前缀 reservation 键不再被读写，滚动升级无需停服清库。
- `routerev:*` 高水位键与 epoch 化 `resv:*` 键常驻 Redis（基数有界），重建/迁移时不得以"清库"方式处理。
- edge 断流预算 SLA 已在 architecture.md §4.3 文档化；edge token 耦合 redesign 与统一 resource accountant 等延期项见计划文登记。

## 回滚

fences/ledger 格式向后兼容，逐条可用二进制回滚收敛：route revision 与 reservation epoch 均为增量布局，回滚二进制后新键安全残留（旧 reader 忽略 revision 字段/无前缀键）。已应用的 migration（0032 operations 索引、0033 route_publication_revisions）保留，不执行 down migration；app 删除回滚到旧逻辑前须先排空在途 tombstone+enqueue 事务。
