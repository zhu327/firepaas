# ADR-0033：Template 分层与 Snapshot 可移植性

状态：提议（v2，证据触发）
依赖：ADR-0028、ADR-0031、ADR-0032。

## 背景

E2B 通过分层 template、预启动 memory snapshot、lazy loading 和 prefetch 获得极快启动；firepaas 当前 cached cold start 和本地 restore 已较快，不能在缺少瓶颈证据时引入全部复杂度。

## 决策

1. 分层模型为 `OCI base → filesystem template → optional memory template → execution overlay`，每层不可变并有 digest/lineage。
2. recipe step 的 cache key 必须包含输入文件、指令、base、builder/runtime 和影响输出的策略；缓存映射只在 artifact 完整发布后可见。
3. memory template 的 compatibility key 至少包含 hypervisor/snapshot format、kernel/init/guest-agent、CPU feature set、machine shape、设备模型和 filesystem parent digest。
4. scheduler 对 compatibility 做硬过滤，对 template locality 只做软打分；未缓存仍可从 durable artifact 获取。
5. 不兼容 memory restore 不得静默 cold boot；只能依据请求的 `filesystem/auto` 语义降级。
6. hardlink/reflink、P2P chunk、UFFD 和 page prefetch 都是本地/性能优化，不改变 artifact durability 和 checksum 语义。
7. memory template 只有在 cold-start/fan-out profile 达到立项阈值后启用，并可按 template/node pool 关闭。

## 理由

先冻结可移植格式和兼容边界，再逐步加入优化，可以避免将节点本地实现细节变成平台契约。

## 后果

- 需要 lineage/ancestor GC、跨节点完整性和兼容矩阵测试；
- 默认仍可只使用 filesystem template 与 OCI prefetch；
- UFFD、P2P 和高级 prefetch 分别立项，失败时回退 file-backed restore。
