# ADR-0003：Postgres 持有期望状态；agent 持有观测状态；Redis 仅为可重建投影

状态：已接受（2026-08-25，修订）

依据：可行性分析附录 D.3 与架构评审

## 决策

| 状态 | 权威所有者 | 说明 |
|---|---|---|
| app/deployment/machine 的期望状态、generation、配额、用户、操作与审计 | PostgreSQL | 唯一 durable business truth |
| 实际 VM/进程/slot/资源/健康 | agent | observed runtime truth，启动扫描后重新上报 |
| route backend、machine location、reservation、短期操作结果与节点指标缓存 | Redis（TTL） | 派生投影；可从 PG + agent 重建 |
| 镜像 blob、可迁移快照工件 | 对象存储 | 以 checksum/version 标识 |
| 本地磁盘、node-local snapshot、日志、镜像缓存 | agent 本地盘 | 可判 lost；不作为 API 读路径权威 |

控制器调和 `PG desired state ↔ agent observed state` 并发布 Redis 投影。它不以 Redis 消失或单次 agent 报告直接改写业务期望。

## fencing 与幂等

- 一个逻辑副本由稳定 `machine_id` 和 `(deployment_id, replica_ordinal)` 标识；`deployment_id` 不是创建幂等键。
- 每次实际创建/重建生成新的 `execution_id` 与递增 `generation`。
- 所有变更操作必须携带 `machine_id`、`execution_id`、`generation`、`operation_id`；agent 拒绝旧 generation，并对重复 operation 返回已持久化结果。
- agent operation ledger 以 `operation_id` 为唯一键，保存 request hash 与结果；相同 ID、不同请求必须拒绝。记录采用原子写，重启后可重放；machine 删除后仍保留可配置的去重窗口，再由 GC 清理。
- PG 用唯一 idempotency key 和 operation 行保证 API 重试不会创建第二个副本；Redis reservation 仅防止短窗口并发与配额超卖。

## 对账与灾难恢复

1. agent 启动扫描本地 runtime 并报告 observed machines；
2. controller 读取 PG desired state，验证 execution/generation 后收养、重建或清理 orphan；
3. Redis 丢失时暂停/限流路由变更，从 PG 的 active route generation 与 agent observed location 重建 Redis；
4. agent 失联时不再放置新 VM；无状态 machine 按 restart policy 重建，node-local volume/snapshot machine 标记为需要人工恢复或 cold start（按产品能力）。
5. Redis 不可用期间，数据面依赖 edge 的 route generation 本地缓存在声明的 stale 窗口内 serve-stale，超窗受控失败；恢复后按上述路径重建投影。是否引入 sentinel 依据 M4 验收决定（可用性语义见 architecture.md §4.3）。

## 后果

- `machines.node_id` 是最近/期望放置记录，不是 agent runtime 的替代品；`state` 拆分为 desired 与 observed，不能只存一个模糊状态字段。
- slot 不是全局资源；如持久化，只约束 `(node_id, slot_index)`。
- 新平台不需要导入 hypeman JSON；存量接管另设迁移方案。
