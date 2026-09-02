# ADR-0030：Readonly dataset 与 per-execution CoW 生命周期

状态：已接受（v1.3）
依赖：ADR-0029。

## 背景

模型、数据集和工具链通常体积大、只读、被多个 VM 共享。为每台 machine 复制完整本地卷浪费磁盘；直接共享可写卷又会引入一致性和租户串扰。

## 决策

1. DATASET_RO 由 agent 从短时预签 URL 流式导入；验证 digest、总大小和文件数，并拒绝 path traversal、symlink escape、device/FIFO 和 archive bomb；导入完成并原子 seal 后才可 attach。
2. DATASET_RO base 以 content digest 标识，seal 后不可修改；同一 project 内可被多个 execution 只读 attach。
3. 可选 writable CoW overlay 的目标语义是 per execution 独立，base 永不接收写回。当前版本未广告该 capability，并对 CoW 请求 fail closed；只有底层 volume adapter、记账和验收全部落地后才可启用。
4. overlay 默认随 execution 删除，不构成持久数据；若未来需要持久化，必须生成新的 volume/artifact version。
5. fork 创建独立 overlay；checkpoint 只记录 base digest，并按 snapshot policy 决定是否包含 overlay diff。
6. base 和 overlay 分别记账。GC roots 包括 active attachment、READY snapshot、fork/restore/import in-flight operation。
7. v1.3 不自动跨节点复制 dataset。调度只选择已有且校验通过的节点；无候选时明确失败或等待运维预热。
8. base materialization 完成并校验 digest 前不可 attach；删除采用 tombstone 和引用归零。

## 理由

只读 base + execution-local CoW 能覆盖模型/数据集共享的主要价值，同时避免 RW 共享协议和跨节点一致性。

## 后果

- DATASET_RO 已实现；writable overlay 仍需要 hypeman volume 的 readonly/overlay 适配、磁盘 accounting 和独立验收；
- 不提供 overlay durability、跨节点可用性或多写语义；
- v2 content plane 可为相同 digest 提供跨节点 materialization，不改变本 ADR 的不可变和隔离语义。
