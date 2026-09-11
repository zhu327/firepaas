# 2026-09-10 全面 review 整改计划

> 依据 2026-09-10 的独立 review（控制面/agent/安全/调度/DX 五路审查）。
> 范围与决策：
>
> - 工作流：P0 安全与正确性、测试/迁移/保留加固、交付/CI、可观测性/CLI（全部四项）。
> - RBAC：采用 handler 层全局身份复核（不新增 scope 契约）。
> - 重复派发：固定 dispatch node + 重复副本回收 + 指标。
> - 提交策略：只改工作树，不提交；工作树中已有的网络相关改动不得被覆盖。
>
> 本文不重复 review 证据；每条任务的行号引用以 2026-09-10 工作树为准。

## Goal

关闭 review 中会导致越权、配额超卖、重复实例、状态机不收敛的安全与正确性缺陷；
补齐迁移/保留/测试隔离的工程缺口；补上交付/CI 与最小可观测性/CLI 能力。不改网络数据面。

## Architecture

沿用既有分层：PG 是业务事实、controller 单写者调和、agent 本地硬准入、Redis 只做可重建投影。
本计划的改动都落在现有边界内，不新增跨层契约；proto 不变。

## Validation

- 每个切片：目标 package `go test -count=1 ./<pkg>/...`（红 → 绿）。
- 依赖 PG/Redis 的切片：`FIREPAAS_TEST_POSTGRES=... FIREPAAS_TEST_REDIS=...`（本地 dev 容器）。
- 波次集成：`GOWORK=off make check` + `make check-lab` + `make sim`。
- 高风险 changeset 收尾：独立 `code-reviewer` 审查一次。

## 依赖与并行

| 任务 | 类型 | 依赖 | 可并行 |
|---|---|---|---|
| W1-1 RBAC handler 复核 | 安全 | 无 | 否（authm5/nodes/system/governance） |
| W1-2 配额请求量 | 安全 | 无 | 否（placement） |

> 复核修正（2026-09-10）：ProjectUsage/ProjectMachineUsage 已包含本次在途
> create，`used > quota` 即含本次请求的超限判定，不存在“再加一笔”的超卖。
> W1-2 改为语义锁定：Place 注释 + store 边界回归测试，无行为变更。
| W1-3 幂等 TOCTOU | 正确性 | 无 | 否（store/store.go） |
| W1-4 派发固定+重复副本回收 | 并发/fencing | W1-3 | 否（placement/controller） |
| W1-5 fork 唯一键+空 hostname | 正确性 | 新 migration | 否（snapshots/store） |
| W1-6 volume/dataset 收敛 | 状态机 | 无 | 否（store/volumes、controller/volumes） |
| W1-7 rollout 旧代 draining | 正确性 | 无 | 否（routepublisher） |
| W1-8 磁盘准入合并 | 正确性 | 无 | 否（agent/server） |
| W1-9 健康检查 fail-closed | 正确性 | 无 | 否（agent/machine） |
| W2-1 迁移锁与 checksum | 迁移 | 无 | 是（db/migrate.go） |
| W2-2 保留与索引 | 迁移/运维 | 无 | 是（migration、controller） |
| W2-3 测试隔离 | 测试 | 无 | 是（fabric/store tests） |
| W3 CI/交付（子代理） | 交付 | 无 | 是（.github、Dockerfile、docs） |
| W4 可观测性/CLI（子代理） | DX | 无 | 是（metrics、cmd/api、cmd/fpctl） |

## 高风险任务的控制

- **W1-1**：不改 scope 语义，只在 handler 层拒绝非全局身份；回滚 = 还原 handler 检查。
  对 `GET /v1/capabilities` 改变为对受限身份脱敏而非 403（避免破坏租户读能力）。
- **W1-4**：`dispatch_node_id` 已记录且节点可达 → 固定重放；节点不可达时保留现行为但在
  事件/指标中标记，由重复副本回收兜底。回收只删除非 home 的重复 observed execution，
  home 取 operation.dispatch_node_id，其次 machine.node_id。路由推导按 execution 去重，
  避免 route_backends 主键冲突。回滚 = 关闭 controller 的 divergence 回收开关。
- **W1-5**：新增 migration 0031（partial unique index，排除 `replica_ordinal < 0`）；
  已发布 migration 不重写。回滚 = 新 migration 重建原索引（forward-only 语义下不自动回退）。
- **W2-1**：schema_migrations 增加 checksum 列；已达 NULL 的历史行按首次校验回填，
  之后不一致即 fail closed。锁泄漏改为 commit 失败时归还连接前显式 unlock。
- **W1-8/W1-9**：agent 行为变化（拒绝非法探针/合并准入计数）只影响新请求；
  回滚 = 还原对应分支。

## 非目标

- 不改网络数据面（eBPF/WireGuard/CNI/DNS/fabric）实现。
- 不引入源码构建、autoscaling、日志归档（v2 范围）。
- 不新增 scope/枚举值，不重写已发布 migration。

## 实施结果（2026-09-10）

全部工作包已实施，工作树保留改动不提交。

### W1 安全与正确性

| 任务 | 结果 | 验证 |
|---|---|---|
| W1-1 RBAC | `requireGlobalIdentity` 应用于 drain/ready、listNodes、scheduler-events、reproject、quota/rate-limit；capabilities 对受限身份脱敏 | `cmd/api` 新增回归（受限 admin key 全平台路由 403、全局身份放行、node_ids 脱敏） |
| W1-2 配额 | **复核修正：不是漏洞**。ProjectUsage/ProjectMachineUsage 已含本次在途 create，`used > quota` 即含本次请求的边界；加注释 + store 边界回归锁定 | `TestProjectUsageCountsPendingRequestAtQuotaBoundary` |
| W1-3 幂等 | 4 处 `ON CONFLICT DO NOTHING` 后重读比对 request；移除不可达 23505 分支 | 确定性竞态测试在 HEAD 红、修复后绿（`TestEnqueueOperationConflictAfterPrecheck`） |
| W1-4 派发 | 新增 `PinnedNodeID` 硬过滤 + `dispatchPin`（可达即固定、不可达记 `firepaas_dispatch_rehomes_total` + 事件）；controller R2.5 回收非 home 的同 execution 存活副本 | scheduler/placement/controller 新增测试；全量 race 通过 |
| W1-5 fork | migration 0037 部分唯一索引（replica_ordinal>=0）；`ActiveRouteMachines` 排除空 hostname | store schema/投影回归 |
| W1-6 volume | `MarkVolumeReady`/`SealDataset` 幂等收敛，Digest/size 冲突仍然拒绝 | store 收敛回归 |
| W1-7 rollout | `draining` 从 deployment 终态（SUPERSEDED/FAILED）派生，与是否有活跃 rollout 无关 | routepublisher 新增测试 |
| W1-8 磁盘 | `admit` 与 `admitVolume` 共享 `inflightDisk + inflightVolumeDisk` 视野 | 跨路径准入回归 |
| W1-9 探针 | 声明但无法编码的探针在 create 时 InvalidArgument（fail closed） | agent server 新增接受/拒绝测试 |

### W2 测试/迁移/保留

- W2-1：迁移器 advisory lock 在 commit 失败路径也释放；`schema_migrations.checksum` 首次回填、之后不匹配即 fail closed；
  独立 schema 测试覆盖“篡改 → 拒绝”“失败后锁可被其它会话获取”。
- W2-2：`DeleteSchedulerEventsOlderThan` + controller 保留期（`FIREPAAS_SCHEDULER_EVENTS_RETENTION`，默认 168h/上限 720h）；
  migration 0038 补 `user_events(at)`、`machines/volumes/snapshots(node_id)`、`apps(project_id)` 索引。
- W2-3：fabric 测试改用独立 schema，共享 lab 库污染导致的稳定失败已修复。

### W3 交付/CI（子代理）

- CI：`images` 加 `needs: [check,race,lint,proto]` + 仅 `refs/tags/v*` 发布签名；新增 release 制品 job（agentd/fpctl/agentctl + sha256）；
  新增 govulncheck（v1.7.0，advisory `continue-on-error`，原因见下）；修正 hypeman 版本注释。
- `.dockerignore` 落地并实测 `docker build` 两个镜像成功。
- `Makefile release`（VERSION + 6 个 linux/amd64 二进制）；文档/配置对齐（shared README、releases v1.5 行、AGENTS.md 过时描述、
  `.env.example` 未使用变量清理、capacity-model 与代码对齐）。

### W4 可观测性/CLI（子代理 + 收口）

- metrics registry：HELP/TYPE 元数据（DescribeCounter/Gauge/Histogram）、自动类型、固定桶直方图；API RED 指标与 request_id 审计字段。
- 日志：`FIREPAAS_LOG_LEVEL`/`FIREPAAS_LOG_FORMAT`（抽到 `shared/pkg/logging`，三个进程共用）。
- 版本注入：api `buildVersion`、agentd `serviceVersion`（const→var）、edge-proxy/fpctl/agentctl `version`；`/v1/health`、edge `/healthz`、`fpctl version` 暴露。
- fpctl：共享 http client（普通 30s header timeout / 长连接 6m，避免截断 wait/stream）、全局 `--addr/--token/--project/--json/--version/-h`、
  `app create` 新增 env/label/node-pool/anti-affinity/health-check flags、路径转义、错误正文收敛。

### 与原 review 的偏差（重要）

1. **配额超卖不成立**（见 W1-2）：原 review 的“Critical”结论有误，已修正并锁定语义。
2. **route_backends 主键冲突不成立**：路由推导按 machine 行生成 backend，同一 machine 只有一个行/一个 backend；重复副本的真实危害是
   观测状态抖动与资源泄漏，已由 R2.5 回收覆盖。
3. **govulncheck 当前为 advisory**：树内 6 条可达漏洞中 `cilium/ebpf v0.12.3` 需升到 v0.22.0 并重新生成 eBPF 对象（与进行中的网络
   工作冲突），其余 grpc/x/text/otel 升级建议走 dependabot；依赖清干净后应移除 `continue-on-error`。
4. **fpctl 幂等键**：服务端目前只有 images 三个端点消费 `Idempotency-Key`；`snapshot create/fork/rescue` 客户端已有 header 但服务端
   未接线，属既存不一致，本轮未改。

### 残余风险 / 后续

- W1-4 的 rehome 窗口（dispatch node 不可达 + 原 RPC 已落地）仍有极小概率双 VM，依赖 R2.5 回收收敛；未做跨节点 execution fence。
- `TestAllocateULAConcurrentReplayConverges` 曾在高并发多包测试下偶发一次（active=[]），本轮 3 次全量 + race 未复现，未定位根因。
- AGENTS.md 被 `.gitignore` 忽略，本轮对其修正不会进入提交。
- 其余未纳入本轮：源码构建、autoscaling、日志归档、durable volume（v2 范围）。

## 独立审查与修复（2026-09-10）

独立 `code-reviewer` 对 changeset 给出 REQUEST_CHANGES；以下问题已修复并补测试：

| 审查项 | 修复 |
|---|---|
| H1 重复副本 survivor 未优先 `operation.dispatch_node_id` | 新增 `Store.LatestCreateDispatchNode`；survivor 顺序 = dispatch_node_id > m.NodeID > agent 排序；新增两个测试用例 |
| M1 STOPPED home 压过 RUNNING 副本 | 新增 `copyLivenessRank`（RUNNING>PAUSED>…>STOPPED），同优先级内才用前述顺序；新增测试用例 |
| M2 资源不足也清除 pin（加宽双 VM 窗口） | `pinnedRejectionReason` 区分暂态 `resources:` 与永久原因；暂态保留 pin 并记 `firepaas_dispatch_pin_holds_total{reason=no_capacity}` + `dispatch_pin_hold` 事件；永久才 rehome |
| M3 lock defer 注册晚于 `pg_advisory_lock` | defer 移到锁语句之前（未持锁时 unlock 为 no-op） |
| L1 inflight 计数负值 uint64 回绕 | `admit`/`admitVolume` 先做 int64 负值 fail-closed（Unavailable），补测试 |
| L2 `jsonEqual` float64 精度 | `json.Decoder.UseNumber()`，>2^53 整数不再判等 |
| L3 迁移锁测试 1s 假阳性窗口 | 改为带 15s context 的阻塞 `pg_advisory_lock` |
| L4 capabilities 测试写共享 nodes 表固定 ID | 唯一 node ID + 断言针对本节点，而非全局计数 |
| L5 GET rate-limits 误伤项目 admin 读取 | 读取保留（projectGated 已限本 project），仅写要求全局身份；调整测试 |
| L6 入站 X-Request-Id 无界 | `validRequestID`（≤128、[A-Za-z0-9._-]），非法重新生成；补测试 |
| L7 变更前已存在的非法探针机器 | 状态而非代码：已在“残余风险”记录，重建时会 fail-closed（InvalidArgument） |

修复后 `make check`、`go test -race ./...`（PG/Redis）、`make check-lab`（含 10 万次调度仿真）全部通过。
