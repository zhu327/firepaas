# M2：单机双节点语义收敛（2026-08-25，执行计划）

> 状态：执行中。上游依据：mvp-plan §6、ADR-0002/0005/0007/0009/0012。
> 单机实验室基线（ADR-0012）下，M2 的多节点项按「代码+仿真全真、部署验收折叠」
> 执行：所有多节点逻辑在真实 Nomad 发现链路上实现并用虚拟节点仿真验证；
> 部署验收在唯一 compute 节点上做单节点实例，两节点部署项标记
> `DEFERRED-MULTI-NODE`（与 M0 同一模式）。

## Goal

让多 API/agent 失败条件下仍满足「不重复副本、不越过资源硬上限、最终收敛」。

## 单机折叠范围

| M2 原项 | 单机落地 |
|---|---|
| Nomad native discovery → ServiceInfo | 真实现（读真实 Nomad `/v1/nodes` + `/v1/job/firepaas-agentd/allocations`），单机只发现 1 个 compute 节点；多节点逻辑由虚拟节点单元测试 + sim 覆盖 |
| 健康/Draining/Unhealthy 状态机 | 真实现；单机上只能注入 ready/drain 两种状态 |
| Best-of-K + pending 记账 | 真实现；scheduler 为纯函数式包，单元测试用虚拟节点 |
| Redis Lua 预约 + 项目配额 + TTL | 真实现（单节点上跑真实 Redis；并发/配额语义用测试覆盖） |
| PG 幂等 + 并发重试 | 真实现；e2e 用 1000 并发 POST 验收 |
| reconcile 决策表 | 真实现；chaos 脚本注入 API/agent crash、ACK 丢失、Redis 清空 |
| 两节点创建/删除 20 轮无泄漏 | 单节点 20 轮（VM/TAP/bridge endpoint/Redis lease/PG 终态）；跨节点项 DEFERRED-MULTI-NODE |
| 10 万次仿真 | 真实现（tools/sim，虚拟 5 节点） |

## 架构决策（ADR-0014 摘要）

1. **节点发现**：Nomad HTTP API 是节点/alloc 的唯一事实源；控制面 10s 轮询
   `/v1/nodes` 与 `firepaas-agentd` allocations，取 grpc/proxy 端口，按节点建
   mTLS gRPC 连接池；每 20s 调 ServiceInfo 把容量/用量/标签/状态投影进
   PG `nodes` 表（observed projection，可重建）。
2. **状态机**：`HEALTHY / DRAINING / UNHEALTHY`。Nomad 节点非 ready、
   scheduling ineligible 或 agent ServiceInfo 非 HEALTHY → UNHEALTHY；
   Nomad drain → DRAINING；其余 HEALTHY。
3. **调度**：先过滤后打分（ADR-0009）。过滤顺序：状态 → node_pool/labels →
   资源（硬准入：allocated+pending+req ≤ R·capacity，mem R=1.0）→
   DEPLOYMENT 反亲和（尽力：候选为空时回退并记录调度事件）。打分
   Best-of-K（R=4/K=3/α=0.5），每轮 `req+allocated+pending+α·usage`。
   pending 从 PG 在途 create 操作推导，20s ServiceInfo 校正。
4. **预约**：Redis Lua 原子预约（node hash 增量 + operation 键 TTL 120s），
   只保障短时并发与项目配额；成功后 optimistic add，失败/完成即释放；
   controller 每周期重建 pending 并回收过期 TTL。
5. **写者**：M2a leader——PG `pg_advisory_lock('firepaas:leader')` 会话租约；
   controller（reconcile+放置）只在 leader 上跑，备实例只读待命。
   M2b 多写创建路径推迟（与 ADR-0007 一致）。
6. **reconcile 决策表**：见下节，所有动作生成 operation 或 scheduler_event
   行，审计可解释。

## Reconcile 决策表（M2.3）

对 PG machines（desired）与 agent List（observed）求差，每周期执行：

| # | 观测 | 动作 | 幂等约束 |
|---|---|---|---|
| R1 | desired=CREATED，agent 有同 machine 且 execution 一致、state=RUNNING | 写 observed + route | 已有 op 终态不重复下单 |
| R2 | desired=CREATED，agent 有同 machine 但 execution 不同 | 旧 execution 为 orphan：入队 delete(旧exec)；删除完成后重建新 execution | 每个 (machine,exec) 一个 op |
| R3 | desired=CREATED，agent 无此 machine，且当前 execution 的 create op 已 SUCCEEDED 超过 grace(30s) | ACK 丢失：入队 delete(清残留)→create(新 exec) | 新 operation_id 派生自 execution |
| R4 | desired=CREATED，节点 UNHEALTHY | 不重建（无可用节点时不反复下单）；observed 置 UNKNOWN，route 摘除；节点恢复后按 R3 | 节点恢复后一次 create |
| R5 | desired=DELETED，agent 仍有 | 入队 delete(当前 exec) | 已有 PENDING delete 不重复 |
| R6 | agent 有、PG 无（含 PG 已 DELETED 但 delete op 已完成而 agent 残留） | orphan：入队 delete(agent 的 exec, machine_id 直接引用) | operation 允许无 machines 行 |
| R7 | PG route/Redis 投影缺失 | buildRoutes 全量重建 + prune | 每次 sync 全量幂等重放 |
| R8 | create op CLAIMED 但对应 machine 已 RUNNING 且 execution 一致 | ACK 丢失的补账：直接 CompleteOperation(SUCCEEDED) | 幂等，不二次 Create |

注：R3 的 grace 期避免与正常初始化（pull 镜像可达 60s）竞争；CREATE 的
INITIALIZING 状态不算缺失，不触发重建。

## 任务分解

| # | 切片 | 验证 |
|---|---|---|
| T1 | migration 0003（nodes/scheduler_events/operations.dispatch_node_id）+ store 扩展 | `make test` + 真库迁移 |
| T2 | nodemanager（Nomad discovery + 连接池 + PG 节点投影 + 状态机） | 单测（httptest 假 Nomad）+ 真机 `/v1/nodes` |
| T3 | scheduler（过滤/打分/pending/事件） | 单测：过滤顺序、硬上限、反亲和、失联排除 |
| T4 | reservations（Lua 预约/配额/TTL/重建） | 单测（真实 Redis）+ 并发 100 预约 |
| T5 | leader（advisory lock） | 双进程实测：仅一个跑 controller |
| T6 | controller 集成（派发/换节点重试/决策表/metrics）+ agent 硬准入 | chaos 实测 2 分钟收敛 |
| T7 | API（replica_ordinal 并发幂等 23505 修复、/v1/nodes、/v1/events、/metrics） | e2e 1000 并发 |
| T8 | tools/sim（100k 放置断言） | `make sim` |
| T9 | e2e-m2.sh + chaos-m2.sh | 真机 PASS |

## 验收映射（mvp-plan §6）

- 同一 ordinal 1000 次并发重试 → 1 machine/execution；不同 ordinal 并发创建：`scripts/lab/e2e-m2.sh` ✅ PASS
- 10 万次仿真断言：`make sim` ✅ PASS
- API/agent crash、ACK 丢失、Redis 清空后 2 分钟内收敛且审计可解释：`scripts/lab/chaos-m2.sh` ✅ PASS
- 20 轮创建/删除无 VM/TAP/bridge endpoint/Redis lease 泄漏：`scripts/lab/e2e-m2.sh`（单机；跨节点 DEFERRED-MULTI-NODE）✅ PASS

## 执行记录（2026-08-26）

- 全部切片 T1–T9 完成；真机验收两次脚本 PASS（见 mvp-plan §6 执行记录）。
- 双 compute 节点的真实放置/反亲和/跨节点 proxy 验收保持
  DEFERRED-MULTI-NODE，上线前必须补测。
- 关键真机修复（已带回归测试或脚本防回归）：
  1. 换代重建必须清空 observed（否则 R8 补账短路成无限换代）→
     store 回归测试 `TestEnsureCreateExecutionChangeClearsObserved`。
  2. create FAILED 退避重试的 opID 必须全局唯一（uuid 后缀），否则撞
     历史成功 op 的幂等键。
  3. reconcile 清理 delete 与用户 delete 分离：operations.kind=reap，
     成功后不得把 desired 置 DELETED。
  4. graceful `nomad job restart` 不杀 VM（hypeman 新进程收养）；crash
     注入必须 kill agentd + firecracker 才构成“节点崩溃”。

## 风险

- Nomad 2.0 system job 的 Latest Deployment 脏状态（M1 遗留）：发现逻辑只看
  alloc 实际状态，不读 deployment 字段。
- 单机无第二 compute 节点，多节点派发逻辑只能靠 sim/单测；上线前必须在
  真实双节点环境复验（DEFERRED-MULTI-NODE）。
- agentd 崩溃杀 VM 是已知行为：M2 的 R2/R3 决策表负责收敛，chaos 脚本
  已把「agent crash → 2 分钟内重建」作为验收项并 PASS。
