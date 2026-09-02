# 评审加固第二轮计划（R2：外部评审 findings 修复）

> 来源：两份外部独立评审（2026-09-02）。已逐项核实（去重第一轮已修项），剩余 findings 全部验证为真实后立项。
> 用户裁决：①edge token 控制面耦合**维持现状**，只文档化"控制面断流预算 SLA"（方案 3）；②route publication revision + reservation epoch rebuild 两项 Phase-2 工作**本轮一起完成**。
> 执行规则同第一轮：只改不提交；工作树含用户既有改动，基于当前内容追加，不得回退无关内容。

## Goal

修复外部评审确认的 3 个 P0（apikey 越权、admission fail-open、master key fail-open）、~20 个 P1 与两项 Phase-2 架构加固（route 单调 revision、reservation epoch），运行态完成度达到 internal beta 门槛。

## 跨 slice 契约

### D-1 admission 有效性
`info.AdmissionSnapshot`（或等价结构）带资源采集有效性：inventory 失败时 `ResourcesValid=false`，server create 准入 fail-closed 返回 `codes.Unavailable`；Capacity 上报用 ≤60s 新鲜的 last-known-good。

### D-2 route publication revision（Slice J 契约）
- 新 PG 表 `route_publication_revisions(hostname text primary key, revision bigint not null)`（顺序新 migration）。
- publisher：发布时在同一 PG 事务内 `INSERT ... ON CONFLICT DO UPDATE SET revision = route_publication_revisions.revision + 1 RETURNING revision`；同一 leader 进程内 rebuild 用互斥串行化。
- Redis catalog 条目带 `revision`；`ReplaceHostRoutes` Lua 仅当 incoming.revision > existing.revision 时生效（miss 时直接生效）。
- edge RouteCache：取到的投影 revision 低于当前条目时不回写（记 `firepaas_edge_route_revision_rejects_total`）；这时把 RouteCache 的负缓存/陈旧语义保持原样。

### D-3 reservation epoch（Slice J 契约）
- 键空间 epoch 化：`resv:{epoch}:...`；active epoch 指针键 CAS 切换；重建先写新 epoch 再切指针，旧 epoch 靠 TTL 自然过期（不再 KEYS/SCAN 全库）；Acquire 在脚本内先读 active epoch。
- 外部 API（Acquire/AcquireR/Release/Prune 等）签名不变。

## 依赖表

| Task | Type | Blocked by | 写集 |
|---|---|---|---|
| E agent 加固 | AFK | — | `internal/agent/**`、`cmd/agentd/**` |
| F edge/redact 加固 | AFK | — | `internal/edge/**`、`cmd/edge-proxy/**`、`internal/security/redact/**` |
| G 控制面加固 | AFK | — | `cmd/api/**`、`internal/controlplane/**`（除 routepublisher/catalog/reservations）、`iac/nomad/control-plane.hcl`、`Makefile` |
| J Phase-2 投影 | AFK | E/F/G | `routepublisher/**`、`catalog/**`、`reservations/**`、route 相关 store/migration、`internal/edge/edge.go` 的 revision 守卫 |
| K 文档+终验+独立审查 | HITL | J | `docs/**` 等 |

**注意**：J 与 F 都在 `internal/edge/edge.go` 有写集交集 → J 必须在 F 完成后串行（依赖表已锁）。J 新增 migration 编号接着 G 的最大号（G 若新增 0032，J 用 0033；派发 J 时核实）。

## Task E：agent 加固【高风险】

Goal：补齐 mutation 崩溃一致性剩余面 + 网络/停机硬化。
Acceptance：
1. **admission fail-closed（D-1）**：`cmd/agentd` 的 resource inventory 失败不得变成 0 占用；`info.AdmissionSnapshot` 带有效性；`server` create 准入对无效快照返回 `codes.Unavailable`；容量上报用 ≤60s last-known-good。测试：fake inventory 错误 → 新 create 被拒，既有 machine 操作不受影响。
2. **Pause/Resume 迁移 claimed 协议**：复用 `runClaimed` 模型：Begin claim → 自 instance 实际状态（RUNNING/PAUSED）恢复收敛 → 幂等完成；同 machine 与其他 mutation 共享 serialization owner（LockMachine）。不再"intentionally unlocked"；注释更新。崩溃恢复测试：claim 在途 + 实例已 PAUSED → 收敛成功。
3. **Delete 分阶段可恢复快**：`RunDeleteMachine`/adapter.Delete 改成阶段化：runtime absent → slot release → egress remove → credential drop → health remove；"VM 不存在"只代表第一阶段完成，后续阶段重试继续补做。失败阶段 log warn 并返回可重试错误。测试：注入 slot Release 失败 → 重试 delete 完成清理。
4. **IPv6 默认拒绝**：fp-isolation 增加 `ip6` family 表（slot veth 入向默认 drop，可配管理放行段与 ip family 对齐），或在 slot 接口禁用 IPv6；选实现成本低的正确方案并在注释说明。
5. **GracefulStop deadline**：`FIREPAAS_AGENT_GRACEFUL_STOP_TIMEOUT`（默认 30s）：超时强转 `Stop()` 并 log；Exec/stream 语义注释。
6. **fence GC 绑定机器存活**：`Fences` 剪除条件增加"机器非存活"判定（agentd 组装期注入 liveness 回调——当前实例清单存在且在 fence 中记载者，高水位保留）；测试：活机器高水位不被年龄 GC 回收。
7. P2 顺手：agent proxy `http.Server` 加 `ReadHeaderTimeout`/IdleTimeout（WS/SSE 同 edge 注释理由）；`Proxy.entries` 死切片清理；CopyTo spool glob 补齐 `firepaas-copy-to-*`。
Files：`internal/agent/**`、`cmd/agentd/**`（含 volumes admission 部分——见下条）。
8. **volume 准入收敛**：节点级 admission 序列化（create volume 与 dataset import 共享一个 admit mutex/预算核算）；容量 GiB 换算改 ceiling。统一 accountant 不建（延期登记）。
9. **canonical network policy**：新 `internal/agent/netpolicy` 包：单一 canonical 私网/保留 CIDR 集合（对齐 datasetReservedPrefixes 中 IPv4 子集 + CGNAT/loopback/link-local + 可配置管理网段），生成 Go matcher 与 nftables set 文本；slot 的 `privateDst`、volumes 的 dataset 保留校验统一消费。测试：slot nft 脚本含 100.64/10 与 127/8。
Validation：`go build ./... && go test ./internal/agent/... ./cmd/agentd/... -count=1 && go vet ./...`
Risk controls：mutation 协议改动属高风险；ledger/fence 格式向后兼容；测试覆盖崩溃窗口重放；不动用户既有 snapshots.go 改动语义。

## Task F：edge + redact 加固

Goal：TokenClient cache 有界化与统一 LRU 原语；readiness 显式化；redact 补齐；观测口径；mTLS helper 强度对齐。
Acceptance：
1. **统一 bounded LRU 原语**（`internal/edge` 内部小类型或提取）：RouteCache 与 TokenClient 复用；TokenClient entries 容量上限（默认 env 可调，如 `FIREPAAS_EDGE_TOKEN_CACHE_MAX`），过期淘汰；增长有界测试。
2. **readiness 显式化**：backend eligibility 只接受 `READY`/`UNCONFIGURED`；空串/未知/NOT_READY 拒绝且打 `firepaas_edge_backend_ineligible_total{reason=}`。先核实 publisher 只写 READY/UNCONFIGURED（若非，先修口径再收紧）。
3. **redact 补齐**：黑名单增加裸 `token`/`credential` 键（与 `source_url_digest` 特判/既有 key 关系核查，避免误伤）；fallback 覆盖 camelCase（如 `trafficToken`、`proxyCredential`）与嵌套数组内的对象键。行为测试。
4. **请求总量/状态码分母指标**：`firepaas_edge_requests_total{code_class=}`（2xx/4xx/5xx），连同既有计数在 handler 出口一处打点。
5. **RequireClientIdentity 强度**：CN 从 peer 证书取经 `VerifiedChains[0][0]`（与 gRPC 拦截器同强度），不再直接读 `PeerCertificates[0].Subject`（验证它在两个路径的差异并存测试）。
6. 未决小项登记（不改代码的不确定性项写到 slice 报告：rate-limiter LRU 驱逐重置桶的利用面评价、WS drain、metrics 端口 opt-in 现状）。
Files：`internal/edge/**`、`cmd/edge-proxy/**`、`internal/security/redact/**`
Validation：`go test ./internal/edge/... ./internal/security/... ./cmd/edge-proxy/... -count=1 -race`

## Task G：控制面加固【含 P0】

Goal：apikey 越权回收、master key fail-closed、生命周期 fence 修复、app 删除原子化、dispatch 并发化（有界）、索引/retention、readyz、错误映射长尾、请求体防护、hcl TLS 修复。
Acceptance：
1. **P0 apikey 越权**：`/v1/apikeys` 三组端点仅接受**全局身份**（root token 或 project_id=="" 的 admin key）；project 受限 admin 拒绝 403；同时 handler 层再校验（不依赖中间件单点）。负向测试：project admin 创建全局/他项目 key、list、revoke 全部 403。
2. **P0 master key fail-closed**：controller 派发带 `secret_refs` 的 create 而 `cfg.Secrets == nil` → operation FAILED + 高亮日志/指标/用户事件，绝不创建 VM；cmd/api 启动时 master key 缺失打 WARN（FG secrets disabled 现状保留）。测试：Secrets=nil + secret_refs deployment → op FAILED，无 agent 调用。
3. **lifecycle fenced 派发**：`processLifecycle` 使用 `op.ExecutionID/op.Generation`；派发前列表更新校验（machine 当前 fence 与 op 不一致 → op 置 SUPERSEDED 终态并跳 RPC）；observed 写回 CAS 失败不得记 SUCCEEDED。测试：fence 漂移 → SUPERSEDED 且无 agent RPC。
4. **app 删除原子化**：tombstone + 全部 machine delete 入队进**同一个 PG 事务**（store 新方法）；重复 DELETE（already_deleted 分支）改为补发全部为收敛的机器 delete（幂等键照旧），不再只返回。测试：模拟中途崩溃（注入部分入队失败）→ 重试 DELETE 收敛。
5. **dispatch 有界并发**：`reconcileOperations` 内 fan-out 工作池（默认 4，env 可调）；同 machine 的操作经进程内 per-machine 锁串行；路由 buildRoutes 保持在批次结束后；单写者账本语义不变。测试：挂死 op 不超过其自身超时拖累其他 machine 的 op。
6. **operations(machine_id) 索引 + retention**：新顺序 migration 加 `CREATE INDEX CONCURRENTLY` 不行（迁移器走事务）→ 用普通 `CREATE INDEX IF NOT EXISTS`；终端态 operations 按可配天数（默认 7d）周期清理+指标；幂等键重放在保留窗内的兼容性在注释说明。
7. **/readyz**：探测 PG ping + Redis ping（各≤1s 超时），失败 503；`/v1/health` 保留静态；`control-plane.hcl` consul check 切到 `/v1/health` 还是 `/readyz` 由该 slice 判断并注释（建议 readyz）。
8. **错误映射长尾**：扫完 cmd/api 剩余的"DB 错误 →404 且回吐 err.Error()"模式（authm5 中间件外的 handler；前面轮次已修主文件，本项专扫 runtimeops/snapshots/apps/wait/lifecycle/nodes/operations 的 residual）。原则：not-found 用哨兵区分，否则 5xx 固定文案。
9. **请求体防护**：全部 JSON decoder 端点统一 `http.MaxBytesReader`（1MiB，流式 cp 端点按其语义例外并注释）；负数/零 vcpu/mem 等数值输入 400 显式拒绝（不再靠 uint64 转换）；deleteMachine 的 `execution_id` 与当前 machine 不一致 → 409。
10. **401 限流**：无效/过期 key 的 401 计入来源 IP 的令牌桶（默认 20/min 级），超限 429；与 ratelimit 包既有实现一致风格。
11. **wait 轮询抖动**：wait 长轮询内部 PG 轮询加 10-25% 抖动（且总预算不变）。
12. **hcl TLS 修复**：`control-plane.hcl` 按 edge.hcl 同款 template 把 PEM 物化到 `secrets/` + env 指向容器内路径（变量仍为无默认 required；不破坏现有变量名/校验）。
13. **Makefile clean**：不再删除已跟踪 `shared/gen/`；保留 bin 清理。
Validation：`go build ./... && go test ./cmd/api/... ./internal/controlplane/... -count=1 && make sim && go vet ./...`（PG/Redis gate 测试本地服务可用时实跑）
Risk controls：migration 只增不改；dispatch 并发是 controller 高风险改动——单写者账本、per-machine 串行、fatal 不发生位不变。

## Task J：Phase-2 投影加固【高风险】

Goal：按契约 D-2/D-3 落地 route 单调 revision（并发旧快照不可覆盖）与 reservation epoch（无 KEYS 阻塞）。
Acceptance（除 D-2/D-3 外）：
1. publisher 序列化（进程内 mutex/singleflight）；重建路径产出包含全部 hostname 的完整新版式。
2. edge 拒绝 revision 回退 + 指标；catalog 写入端与读端的协议变更考虑 Redis 里既有旧条目兼容（无 revision 的旧条目视为 revision 0）。
3. reservation：active epoch 指针读写原子（Lua）；重建完成后新 epoch 生效 ≥2×rebuild 间隔后旧键自然过期（TTL）；Prune/Reset 语义重写为指向先行 active epoch；gratis 测试无 KEYS 调用（grep/Lua 脚本用例断言）。
4. 测试：乱序发布（先入旧 revision 再入新 revision CAS 拒绝）、leader 同进程并发 rebuild、epoch 切换在途 Acquire 一致性、TTL 兜底。
Files：`internal/controlplane/{routepublisher,catalog,reservations}/**`、route 相关 store 方法与一处新 migration（接 G 后的最大编号）、`internal/edge/edge.go`（仅 revision 守卫）、对应 `*_test.go`
Validation：`go test ./internal/controlplane/routepublisher/... ./internal/controlplane/catalog/... ./internal/controlplane/reservations/... ./internal/edge/... -count=1` + 全量 `make check` + `make sim`
Risk controls：与 D-2/D-3 契约逐条对拍；Redis 更新脚本以幂等为准则；edge 回退守卫不得影响 serve-stale 既有语义（修订额定测试）。

## Wave 3（J 之后）

1. 文档：architecture.md §4.3 记录"edge token 控制面断流预算 SLA"（维持现状裁决）+ route revision/reservation epoch 现状回写；ADR-0006/0007 如需补记后果；本计划文未登记延期项（统一 resource accountant、rate-limiter 重置桶分析、WS drain、copy-to/cleanup、leader epoch、controller 队列化）。
2. 终验：`make check`、全树 `-race`、PG/Redis gated、`make sim`、`bash -n`、Nomad validate。
3. 独立 `code-reviewer` 审查本次 changeset（一次性）。

## 执行结果

> 2026-09-02 完成。代码仅在工位树（未提交）；文档回写见 architecture.md §4.3/§5 与 ADR-0038。

### 各 slice 交付

- **E（agent 加固）**：admission fail-closed（`info.AdmissionSnapshot` 返回 `resourcesValid`，server create 对无效快照返回 `codes.Unavailable`，容量上报用 ≤60s last-known-good；fake inventory 错误下新 create 被拒、既有操作不受影响）；pause/resume 迁移 claimed mutation（`mutation.RunLifecycle`，与 create/delete 共享 `LockMachine`，崩溃窗口经 `ConvergePause`/`ConvergeResume` 收敛，回归测试 `lifecycle_claim_test.go`）；delete 分阶段可恢复（runtime absent → slot release → egress remove → credential drop → health remove，VM NotFound 仅代表首阶段完成，后续阶段重试补做）；IPv6 默认拒绝（fp-isolation 增加 nft `ip6` family 表，ensure 路径补齐升级节点）；GracefulStop deadline（`FIREPAAS_AGENT_GRACEFUL_STOP_TIMEOUT` 默认 30s，超时强转 Stop）；fence GC 绑定存活（`Fences.PruneBeforeUnlessLive` + agentd liveness 回调）；proxy `http.Server` 超时、`Proxy.entries` 死切片清理、CopyTo spool glob 补 `firepaas-copy-to-*`；volume 准入节点级串行化 + GiB ceiling；新 `internal/agent/netpolicy` canonical 私网/保留 CIDR 集（slot `privateDst` 与 dataset 保留校验统一消费）。
- **F（edge/redact 加固）**：统一有界 LRU 原语（RouteCache/TokenClient/RateLimiter 共用），TokenClient 容量上限（`FIREPAAS_EDGE_TOKEN_CACHE_MAX`）；backend eligibility 只接受 READY/UNCONFIGURED，未知态计 `firepaas_edge_backend_ineligible_total{reason=}`；redact 补裸 `token`/`credential` 键、camelCase fallback（`trafficToken`/`proxyCredential`）与嵌套数组对象键；`firepaas_edge_requests_total{code_class=}` 在 handler 出口统一打点；`RequireClientIdentity` 强化为必须存在 `VerifiedChains`（与 gRPC `PeerCN` 同强度，防 ClientAuth 放宽后 fail-open）。
- **G（控制面加固）**：P0 apikey 仅限全局身份（`requireGlobalIdentity` handler 层二次校验，project admin 全量 403）；P0 secrets 主密钥 fail-closed（`cfg.Secrets==nil` + secret_refs → op FAILED，零 agent RPC）；lifecycle fenced 派发（派发前 fence 比对 → SUPERSEDED 且零 RPC，observed 写回 CAS）；app 删除原子化（`SoftDeleteAppAndEnqueueDeletes` 单 PG 事务墓碑+入队，already_deleted 幂等补发）；dispatch 有界并发（默认 4 worker + 进程内 per-machine 串行锁，路由 build 在批次后）；migration 0032 operations(machine_id) 索引 + 终端态 op 周期清理（默认 7d）；`/readyz` 真实依赖探活（PG SELECT 1 + Redis PING 各 ≤1s），`control-plane.hcl` consul check 切 `/readyz`、`/v1/health` 保留静态；错误映射长尾清扫、全端点 `http.MaxBytesReader` 1MiB（流式 cp 例外）、负数/零资源显式 400、deleteMachine execution 不一致 409；无效 key 401 计入源 IP 令牌桶（超限 429）；wait 轮询 -10%~+25% 抖动；hcl TLS PEM 物化修復（同 edge.hcl）；Makefile clean 不再删已跟踪 `shared/gen/`。
- **J（Phase-2 投影）**：migration 0033 `route_publication_revisions`；publisher 在同一 PG 事务内 insert-on-conflict-increment 分配 revision（进程内 mutex 串行化 rebuild）；catalog `ReplaceHostRoutes` Lua 以 `routerev:{hostname}` 高水位 CAS（旧条目视 revision 0；高水位键不随投影删除）；edge RouteCache revision 回退守卫（计 `firepaas_edge_route_revision_rejects_total`，serve-stale/负缓存语义不变）；reservation epoch 化（`resv:{epoch}:...` + `resv:active` 指针 Lua 原子切换，旧 epoch TTL 过期，无 KEYS/SCAN；对外 API 签名不变）。
- **coordinator 串行项**：mTLS `RequireClientIdentity` VerifiedChains 强化（见 F）；edge revision-rejects 指标导出于 `/metrics`（handler.go）。

### 验证证据

- `make check`（build + vet + test + tidy-check）绿。
- 全树 `go test ./... -race` 绿。
- PG/Redis gated 测试（`FIREPAAS_TEST_PG`/`FIREPAAS_TEST_REDIS`）实跑绿（含 catalog 乱序发布、reservation epoch 切换、route revision 同事务分配、app delete 崩溃重放回归）。
- `make sim`（100,000 次调度仿真）绿。
- `nomad job validate iac/nomad/control-plane.hcl` 通过；`bash -n scripts/lab/*.sh` 通过。

### 延期登记（deferral registry）

1. **统一 resource accountant**：create volume 与 dataset import 暂未共享单一预算核算器，节点级 admit mutex 已收敛竞态；统一 accountant 量级收益不足，延期。
2. **edge token 控制面耦合 redesign**：用户裁决维持现状；断流预算 SLA 已写入 architecture.md §4.3（token TTL 30s + serve-stale 120s）。
3. **rate-limiter LRU 驱逐重置桶分析**：驱逐重置后攻击者重建桶的成本比被驱逐者重累积约 50:1，低于 P2 阈值，仅登记不再加固。
4. **WS graceful drain**：WebSocket 长连接优雅排空未实现，随会话能力规划。
5. **metrics 端点 bind/auth 部署决策**：edge `/metrics` 由 `FIREPAAS_EDGE_METRICS_PORT` opt-in（未设置不监听，独立 ServeMux，无鉴权）；控制面 `/metrics` 默认开放、`FIREPAAS_METRICS_TOKEN` 可收口。是否默认绑定 localhost/强制鉴权属部署决策，登记。编辑注：本轮新增的真实依赖探活 `/readyz` 在控制面（cmd/api）ServeMux 上；edge 侧保持 `/healthz` 轻探针。
6. **401 限流 soft-limit race 说明**：401 令牌桶在并发下存在 soft-limit 竞态（多笔并发读取同一桶旧值），接受为已知限流精度边界。
7. **volume admit 不 gate snapshot 有效性**：卷准入只做容量预算，不以 snapshot 内容有效性为准入条件（由 ADR-0036 协议独立保证），登记确认。
8. **machine create `inflightDisk` 与 volume `inflightVolumeDisk` 是独立计数器**：两条准入路径各自记账，不互相扣减；统一核算隨 accountant 一并延期。
9. **ip-family 遗留 nft `privateDst` set 不在升级节点自动扩展**：canonical CIDR 集合变更后，既有节点的 `privateDst` 元素不自动增补；下一个宿主机升级窗口核对。
10. **controller work-queue 隔离（按类型分片 backpressure）**：当前单队列 + per-machine 串行已消除跨 machine 拖索；按 op 类型分片的队列化延期。
11. **leader epoch**：控制面选主未引入显式 leader epoch 概念，登记。
12. **CopyTo/cleanup 残留**：spool glob 已补齐；更早遗留的 cleanup 路径统一治理延期。
13. **完整 execution-materialization 状态机**：Phase-1 架构项，保持登记。
14. **统一 durable command/outbox 模式**：app delete 已首个迁移至单事务墓碑+入队；其余写者路径的统一 outbox 模式延期。

编辑注：IPv6 隔离使用 nft `ip6` family 表实现（非禁用接口 IPv6），与 `ip` family 同构的默认拒绝。

### 独立审查回流（code-review-expert，verdict: COMMENTS，无 invariant 违反、无安全回归）

- P2：app 删除遇 `ErrRequestConflict`（历史脏数据）现在返回 409 + 明确审计日志，不再是不区分的 500。
- P2：controller 派发 per-machine 锁改为引用计数回收（refs==0 才摘除，`TryLock` 机会回收存在摘除后仍有人持旧锁的窗口，改为同临界区引用计数，经 `-race` 验证）。
- P3：`runClaimed` 顺序对齐为 已完成重放 → fence → Begin（stale 新请求不再留孤儿 PENDING claim），与 RunCreateMachine 一致。
- P3：`proxiedReqs` 恢复每客户端请求计一次（重试不重复），与 `requests_total` 分母语义统一；R1 旧测试断言同步更新。
- 复核整改验证：`make check`、全树 `-race`、`make sim` 重新通过。
