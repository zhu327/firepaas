# firepaas v2 计划：可移植工件、源码构建与多集群

> 状态：方向性规划草案 v1（2026-08-28）。v2 不承诺一次性交付全部工作包；
> 采用 capability-driven 的分阶段发布。关键决策见 ADR-0031～0034；balloon、
> UFFD、GPU 仅在证据触发后另立 ADR。

## 1. 版本定位

v2 的目标是把 v1.x 的节点本地能力演进为可移植的平台能力：

1. volume、snapshot、template 具有内容寻址、校验、复制和 GC；
2. 从“部署 OCI image”扩展到隔离的“源码/Dockerfile → OCI → deployment”；
3. 在确有启动 SLO 需求时增加分层、预启动 template；
4. 在单集群 HA 已充分验证后，以 cell 模式支持多集群/BYOC；
5. balloon、UFFD、GPU 是相互独立的条件轨道，不作为 v2 GA 的捆绑承诺。

建议按 **v2.0 artifact plane、v2.1 build、v2.2 portable snapshot/template、v2.3
multi-cluster** 发布。2 人总体约 9～15 个月，需根据真实需求逐阶段立项。

## 2. 全局入口门禁

- v1.3 node-local snapshot/volume 至少运行一个生产观察周期；
- `docs/ga-observation-scorecard.md` 的单集群 HA、DR、72h 和 30 天观察全部 PASS；
- 目标对象存储完成容量、吞吐、故障和成本基准；
- artifact compatibility、checksum、加密、保留和删除责任已有审查结论；
- multi-cluster 需要真实地域、合规、容量或 blast-radius 需求，不能仅为架构完整性立项。

## 3. V2.0：Durable Artifact / Volume Content Plane

**ADR：** [ADR-0031](adr/0031-content-addressed-artifact-plane.md)。

### 目标

建立 volume、snapshot、template 共用但类型隔离的 durable content plane：

- PG：artifact manifest、version、引用、replica、operation、encryption key version；
- S3/MinIO：不可变 chunk/object 内容；
- agent 本地盘：可丢失 materialized cache；
- Redis：短期 upload/download/replication lease，不保存内容真相。

### 能力

- 内容寻址 manifest、chunk checksum、总大小和 lineage；
- resumable upload/download；短期、audience-bound token；
- 客户端/agent 直连内容服务，主 API 不代理大文件；
- node-local materialization cache、引用感知 high/low watermark GC；
- dataset version pin、clone、snapshot；
- envelope encryption 与 key rotation metadata；
- node loss 后从对象存储重新 materialize；
- 带宽、并发和费用指标。

### 一致性

artifact 只有在所有必需对象上传、checksum 验证且 manifest 原子发布后才进入 READY；
任何未完成上传不可被 restore/attach。删除先 tombstone，再等待引用和租约归零，最后
清理内容；GC 不依赖单一 DB 引用计数，必须能 mark-and-sweep 校验。

### 验收

- 上传中断和 agent crash 后可续传或安全重试；
- 任意 chunk 损坏在使用前被发现；
- origin node 永久丢失后可在另一节点 materialize；
- 并发 clone/delete/attach 不造成 use-after-delete；
- 跨项目 token 和对象路径访问拒绝；
- 对象存储短时不可用时行为受控且不发布半成品。

## 4. V2.1：MicroVM Build Service

**ADR：** [ADR-0032](adr/0032-microvm-build-trust-and-provenance.md)。

### 目标与拓扑

```text
source upload → PG durable build queue → dedicated build node pool
→ disposable microVM + rootless BuildKit → registry digest → deployment
```

### 必须满足

- `builds/build_attempts/builders` 是 PG 业务事实，leader 切换后可恢复；
- build worker 与应用 compute pool 隔离；
- source 通过 artifact plane 上传，API 不缓冲；
- 每次 attempt 使用一次性 VM，默认无宿主特权；
- registry token 短时且 repository-scoped；
- build secret 通过 one-shot channel，不进入 layer/cache/log；
- build egress 强制执行 v1.3 policy；
- project 级 build 并发、CPU、内存、磁盘和超时配额；
- 输出只以 digest 进入 deployment；
- provenance 包含 source/Dockerfile/base/output digest、BuildKit/runtime 版本、
  policy generation；生成 SBOM，签名可作为后续增量。

### 非目标

- 自研 Dockerfile frontend、完整 CI、Git hosting、privileged build；
- build 自动部署生产；必须显式创建 deployment；
- 跨租户可写 cache。

### 验收

- queue/API/worker 任意点 crash 后不双发布或丢 build；
- secret 不进入 layer、cache、日志或 provenance；
- 恶意 source archive 和 Dockerfile 不能逃逸 VM；
- build storm 不影响应用 compute SLO；
- cache 命中与不命中输出 digest 一致。

## 5. V2.2：分层 Template 与跨节点 Snapshot

**ADR：** [ADR-0033](adr/0033-template-layering-and-snapshot-portability.md)。

### 分层模型

```text
OCI base
→ filesystem template
→ optional booted memory template
→ per-execution writable overlay
```

每层都由 digest 和 lineage 标识。memory template 绑定完整 compatibility key：

- hypervisor + snapshot format；
- kernel/init/guest-agent；
- CPU vendor/family/model/features；
- filesystem parent digest；
- machine shape 和设备模型。

### 能力

- build recipe step 内容寻址缓存，首次变化之后才重建；
- snapshot memory/rootfs artifact 上传对象存储；
- compatibility-aware placement；
- 节点本地 cache 与预取；
- memory restore 不兼容时仅按用户选择降级 filesystem/cold boot；
- fork fan-out 的 hardlink/reflink 是本地优化，不改变 durable artifact 语义；
- 可选启动页 prefetch profile。

### 证据触发

memory template、UFFD 和复杂分层不是默认必做。至少满足一项才启用 memory template：

- cached OCI cold start p95 无法满足明确 SLO；
- 同一模板具有足够高 fan-out；
- profile 证明 page-in/boot 是主要瓶颈。

当前约 2.17s cached cold start 和 95ms restore 本身不足以证明需要 E2B 全套 NBD/UFFD。

### 验收

- 跨节点 restore checksum、文件系统和内存完整性；
- 不兼容节点不会被错误选择；
- 分层缓存不会跨项目泄漏 secret；
- ancestor 删除、并发 GC、上传未完成时不破坏后代；
- P2P 或 lazy fetch 若未启用，不影响正确性。

## 6. V2.3：Multi-cluster / BYOC Cell

**ADR：** [ADR-0034](adr/0034-multicluster-cell-and-failover.md)。

### 架构

采用 cell，而不是跨集群共享一个 machine 控制循环：

- 每个 cluster 有独立 PG、Redis、controller、agent、edge 和 workload CA；
- global plane 只维护 project/app placement intent、cluster registry、artifact replication、
  global hostname 和 failover operation；
- cluster 内 route generation 不跨集群合并；
- global edge/DNS 先选择 cluster endpoint，再由 cluster edge 选择 machine；
- global strong-consistency authority 分配单调 active epoch；cell mutation API、controller
  和 global edge 必须校验 epoch；失联超过宽限后停止新写和公开入口（CP 优先）；
- artifact 经 registry/object storage 复制；running machine 不跨集群迁移；
- cluster loss 以新 deployment/generation 表达，绝不复用旧 execution。

### 第一阶段只做 Active/Passive

1. app 固定 home cluster；
2. image/template/immutable dataset/snapshot 异步复制到 standby；LOCAL_RW 必须先
   quiesce/seal 为明确 volume version，并声明 replication watermark/RPO；
3. operator 显式 failover；先隔离旧 cell，或等待其 lease 与 global edge/DNS 最大
   TTL 全部过期；灾难切换要求 data-loss acknowledgement；
4. readiness、目标 artifact version 和复制水位通过后切 global DNS/edge；
5. failback 是新的 fenced operation，禁止覆盖 standby 侧更新的数据。

### 非目标

- 跨集群共享 PostgreSQL 写域或 Redis catalog；
- active/active app 调度；
- 跨集群全局 inflight/session affinity；
- 跨集群 RW block volume；
- 在线 VM 迁移；
- global service mesh。

### 验收

- 隔离断开 home cluster 后，operator failover 在声明 RTO 内完成；
- 未复制完成的 artifact 阻止切流；
- global plane partition、双向 cell partition、旧 cell 恢复和 DNS 缓存未过期场景下，
  旧 epoch 都不能继续接收 mutation/公开流量；
- failover/failback 不复用 execution，不回退 route generation；
- standby 必须达到指定 artifact version/replication watermark；RPO 不满足时阻断，
  除非操作者显式确认数据丢失；
- cluster 凭证撤销、CA 轮换和 project 隔离通过安全测试。

## 7. 条件优化轨道

这些能力分别立项、分别验收，不阻塞 v2 核心版本。

### 7.1 Active Ballooning

触发：真实密度数据证明内存是主瓶颈，且 PSI/OOM/tail latency 可观测。

- 首版只做 node-level pressure controller；
- 配额和调度仍按 requested memory，不把回收后的 RSS 当作可售承诺；
- latency-sensitive workload 可 opt-out；
- 需独立 ADR：pressure、floor、step、cooldown、失败与计费语义。

### 7.2 UFFD Lazy Restore / Graduation

触发：profile 证明大内存 snapshot 的 page-in 或复制成为恢复瓶颈。

- 默认关闭，只用于 capability 匹配的 Firecracker 节点；
- pager 是节点故障域，session 丢失时 machine 必须 unhealthy/reap；
- 需要版本化 systemd unit、drain、graduation 和故障注入；
- UFFD 只优化性能，不提供 durability。

### 7.3 GPU / 多 Hypervisor 节点池

触发：真实 workload、硬件、驱动/许可矩阵和至少两台 GPU 节点齐备。

- 普通池继续 Firecracker；GPU 池使用经验证的 Cloud Hypervisor/QEMU；
- ServiceInfo 上报结构化 GPU inventory/profile；
- scheduler/Redis/agent 对 profile slot 做三级 reservation/admission；
- GPU machine 默认不支持 memory snapshot/fork/standby，除非 capability 和真机测试
  明确证明；
- 不一次性承诺所有 hypervisor 的功能等价。

## 8. 工作包依赖

```text
v1.3 stable
   ↓
V2.0 artifact/content plane
   ├──────────────→ portable volume
   ├→ V2.1 microVM build → V2.2 layered template
   └→ V2.2 cross-node snapshot ─┐
                                ├→ V2.3 multi-cluster active/passive
single-cluster GA evidence ─────┘

metrics evidence → balloon / UFFD / GPU（独立轨道）
```

不得在 content plane 的 manifest、引用和删除语义稳定前开始 multi-cluster；不得在
one-shot secret 和 egress policy 稳定前开放用户构建。

## 9. 版本级验收

### v2.0

- node loss 后 durable volume/snapshot 可跨节点恢复；
- checksum、断点续传、并发删除、GC、跨项目访问测试全绿；
- 对象存储故障不会产生 READY 半成品。

### v2.1

- source→build→digest→deployment 闭环；
- build secret/registry token 不落层；
- queue/worker/leader crash 可恢复；
- build pool 压测不影响 app SLO。

### v2.2

- 跨节点 memory/filesystem restore；
- compatibility 硬过滤；
- 分层缓存/lineage/GC 故障注入；
- 性能收益达到立项时设定阈值，否则默认关闭 memory template。

### v2.3

- active/passive cluster failover/failback 演练；
- artifact replication gate、split-brain fence、global route generation 全绿；
- 两个 cluster 各自完成 72h soak，再执行隔离 DR。

## 10. 全局非目标

- WireGuard mesh、6PN、VM 跨节点直连；
- 在线 VM 热迁移；
-跨集群共享数据库写域；
-跨集群 RW block volume；
-自研完整 CI/CD 或 Git 平台；
-通用 service mesh/DLP；
-未经验证的 GPU+snapshot/fork/standby 组合；
-以 balloon 后 RSS 作为租户内存承诺；
-以 UFFD 替代 durable snapshot storage；
-因 hypeman 已实现某能力就跳过 firepaas 的 capability、fencing、审计和多节点验收。

## 11. 主要风险

| 风险 | 控制 |
|---|---|
| artifact 引用和 GC 复杂度 | immutable manifest、tombstone、lease、mark-and-sweep、恢复演练 |
| build 扩大供应链攻击面 | disposable VM、rootless BuildKit、短期 token、SBOM/provenance、独立节点池 |
| memory template 复杂度无收益 | 证据触发、默认关闭、达不到阈值则保留 OCI prefetch |
| multi-cluster 导致双写与脑裂 | cell 独立写域、active/passive、global generation fence、operator failover |
| volume 语义膨胀 | v2 首先做不可变版本和 restore，不承诺跨集群共享写 |
| 条件轨道拖累主线 | 每项单独 ADR、硬件/基准和发布门禁，不进入核心 GA 条件 |
