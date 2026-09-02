# ADR-0014:M2 节点发现、写者演进与 reconcile 收敛(单机基线)

状态:已接受(2026-08-25)
依据:mvp-plan §6、ADR-0002/0005/0007/0009/0012

## 决策

### 1. Nomad HTTP API 是节点发现的唯一事实源

控制面 `nodemanager` 每 10s 轮询 `/v1/nodes` 与
`/v1/job/firepaas-agentd/allocations`，从 alloc 的 `network.ports` 取 grpc/
proxy 地址，按节点建立 mTLS gRPC 连接池；每 20s 调 `InfoService.ServiceInfo`
把容量/用量/标签/状态投影进 PG `nodes` 表。nodes 表是**可重建的 observed
projection**，PG 权威仍是 machines/operations/routes；Redis 只承担 route
与 reservation 两类易失状态。

理由:不引入 Consul DNS 依赖（单机实验室 Consul 长期可停）；Nomad 2.0 原生
service API 尚在变化，job alloc 端口是稳定接口。发现失败时保留上一轮快照并
把节点置 UNKNOWN，调度器排除之。

### 2. 节点状态机三值

`HEALTHY / DRAINING / UNHEALTHY`。Nomad 节点非 ready、scheduling
ineligible，或 ServiceInfo 非 HEALTHY → UNHEALTHY；Nomad drain →
DRAINING；其余 HEALTHY。调度器只接受 HEALTHY；DRAINING 不接新
machine；UNHEALTHY 触发 R4（不重建，摘路由，等恢复）。

### 3. 调度:先过滤后打分,pending 由 PG 推导

过滤顺序（ADR-0009）：状态 → node_pool/labels → 资源硬准入 →
DEPLOYMENT 反亲和（尽力而为，候选为空回退并记调度事件）。Best-of-K
打分公式 `(req+allocated+pending+α·usage)/(R·capacity)`，R=4、K=3、
α=0.5（CPU），内存 R=1.0。pending 来自 PG 在途 create 操作（PENDING/
CLAIMED），20s ServiceInfo 校正 allocated。

### 4. Redis Lua 预约只保障短时并发与配额

预约 key：`resv:node:{node_id}` hash（pending_vcpu/pending_mem_mib）+
`resv:op:{operation_id}`（TTL 120s，含 node/cpu/mem/project）。Lua 原子
检查节点剩余与项目配额（projects.vcpu_quota/mem_mib_quota）后增量；成功
后 optimistic add；完成/失败释放；controller 每周期重建 pending 并回收
TTL 过期键。预约不是 deployment 唯一键，业务幂等仍由 PG 唯一索引保障。

### 5. 写者演进停在 M2a

PG `pg_advisory_lock(hashtext('firepaas:leader'))` 会话租约：controller
（reconcile+放置）只在持锁实例运行，备实例只读待命；锁随连接断开自动释放。
M2b 多写创建路径推迟到真实多实例部署出现时（ADR-0007 的表不提前兑现）。

### 6. reconcile 决策表

R1–R8（历史执行记录见 [M2 单机记录](../archive/milestones/2026-08-25-m2-single-node.md)）。所有纠正动作写
operations（幂等键）或 scheduler_events（只读观测），保证审计可解释；
Redis miss 靠全量重建 route 投影收敛。

## 后果

- 新增 PG 表 `nodes`、`scheduler_events`，`operations` 增加
  `dispatch_node_id` 列（migration 0003）。
- scheduler 包从骨架变成纯函数式实现，单元测试即仿真断言的基础。
- 单机实验室只能部署 1 compute 节点；两节点真实放置/反亲和验收标记
  DEFERRED-MULTI-NODE，上线前必须复验。
- M2b 前 API 横向扩容仍受 count≤2 限制（ADR-0007）。
