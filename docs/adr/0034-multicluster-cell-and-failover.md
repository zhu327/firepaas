# ADR-0034：Multi-cluster Cell 所有权、全局路由与 Failover

状态：提议（v2，单集群 GA 后）
补充：ADR-0003、ADR-0005、ADR-0016。

## 背景

地域、合规和 blast-radius 可能要求多集群，但跨集群共享一个 machine reconcile、PG 写域或 Redis catalog 会放大延迟和脑裂风险。

## 决策

1. 每个 cluster 是独立 cell，拥有自己的 PG、Redis、controller、agent、edge 和 workload CA；machine/execution/route 的写入只发生在 cell 内。
2. global plane 只拥有 project/app 的 cluster placement intent、cluster registry、artifact replication 状态、global hostname 和 failover operation。
3. global route 为 `hostname → cluster endpoint generation`；进入 cluster 后仍使用现有 `hostname/port → machine backend set`。不合并跨 cluster machine backend。
4. 第一阶段只支持 active/passive：app 有唯一 active cell；artifact 完整复制和目标 readiness 通过后才能切 global generation。
5. global plane 的强一致存储分配单调 `active_epoch`。cell controller 的 mutation、global edge 的公开流量和 failover operation 都必须携带并校验 epoch；旧 epoch 永久失效，不能只依赖可过期缓存。
6. 采用 CP 优先：cell 与 global authority 失联超过租约宽限后停止新的 mutation 和公开入口；已建立的数据连接可在有界 drain 窗口内结束。global plane 故障可能影响管理面，但不能形成双 active。
7. operator failover 前必须证明旧 cell 已隔离/fenced，或显式接受其不可达且等待旧租约、global edge/DNS 最大 TTL 全部过期；仅更新数据库标志不足以切换。
8. failover/failback 都是新的 fenced operation 和 deployment generation；绝不复用旧 cell 的 execution。旧 cell 恢复后先同步 active epoch，未同步前保持隔离。
9. cell 间不共享 PostgreSQL 写域或 Redis catalog；artifact 通过 ADR-0031 的 immutable content plane 复制。
10. 首版自动 gate 只接受 immutable image/template/dataset/snapshot。LOCAL_RW 必须先 quiesce/seal 为明确 volume version；灾难切换需显示 RPO 和 data-loss acknowledgement，禁止自动 failback 覆盖 standby 新数据。
11. 每 cell 使用独立 workload CA 或可撤销中间 CA；global plane credential 不授予直接操作任意 guest 的权限。

## 理由

Cell 模型限制故障域并保留现有单集群不变量；active/passive 比一开始构建全局 active/active 更容易验证数据和流量唯一性。

## 后果

- multi-cluster 不能早于单集群 HA、durable artifact 和 DR 证据；
- 不支持跨集群 running VM 迁移、全局 inflight/session affinity 或 RW volume；
- 需要 global generation、artifact replication gate、CA 撤销和 split-brain 演练；
- ADR-0016 继续成立：multi-cluster 不引入 VM overlay、6PN 或 service mesh。
