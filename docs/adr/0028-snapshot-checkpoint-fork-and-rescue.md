# ADR-0028：Snapshot 资源、Checkpoint、Fork 与 Rescue 语义

状态：已接受（v1.3）
补充：ADR-0003、ADR-0013、ADR-0015。

## 背景

pause/resume 已可用，hypeman 也支持 snapshot/fork，但 firepaas 没有 first-class snapshot 资源、引用/保留模型和清晰的 node-local durability 边界。

## 决策

1. PG 保存 snapshot 元数据和 operation 事实；v1.3 artifact 仍由 origin agent 本地持有。API 必须返回 locality、origin node、durability 和 compatibility key。
2. snapshot 不可变，绑定 source machine/execution；状态机为
   `CREATING → READY → DELETING → DELETED`。origin node 暂时不可达时
   `READY → UNAVAILABLE ↔ READY`；只有节点退役、人工确认或 agent inventory
   权威证明 artifact 不存在时才能进入不可逆 `LOST`。compression 是正交子状态。
3. checkpoint 为 `pause → capture → resume source`；源 execution 不变。失败必须尽力恢复源，且 artifact 与 checksum 未完成的快照不能发布 READY。
4. fork 从 READY snapshot 创建新的 debug/ephemeral machine，必须有新 ID、execution、slot、credential 和 TTL；默认无 public route，不直接加入 production rollout。
5. fork 不继承 secret 值；secret refs 需显式重新授权。LOCAL_RW volume 不继承；DATASET_RO 可重新 attach。
6. memory restore 必须匹配完整 compatibility key。filesystem restore 忽略内存/device state并冷启动；`auto` 只对列明的兼容错误降级，不吞掉损坏、checksum 或权限错误。
7. filesystem checkpoint 经 guest channel sync/fsfreeze，并在成功、失败、timeout、cancel 路径 thaw；一致性标记为 `clean` 或 `crash-consistent`。
8. writable volume 存在时拒绝 memory checkpoint/fork，直至另有一致性协议。
9. 支持 none/zstd/lz4 异步压缩和 `max_count/max_age`；schedule retention 不删除手工 checkpoint，删除前检查 restore/fork 引用。
10. 遵循 ADR-0024：接收过 secret 的 execution 禁止 memory checkpoint/fork；只允许 filesystem-only checkpoint，且平台注入路径不写 rootfs。应用主动复制 secret 到持久文件不在平台可证明边界内；恢复时重新授权交付。

## 理由

将 snapshot 建模为资源，才能对 locality、兼容、引用、GC 和用户预期作出一致承诺；fork 不能绕过 app rollout 和租户安全边界。

## 后果

- 新增 snapshot schema/API/controller 和 capability；
- v1.3 不承诺跨节点 snapshot durability 或在线迁移；
- 需要 crash、ACK loss、leader handover、compression race、freeze/thaw 和泄漏测试；
- 修改 agent proto 时必须遵循 ADR-0013 的兼容流程。
