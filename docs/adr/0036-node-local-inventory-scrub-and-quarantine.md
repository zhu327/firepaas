# ADR-0036：节点本地 Inventory、Scrub 与 Quarantine 删除协议

状态：已接受（v1.4，2026-09-01）
关联：ADR-0003、ADR-0028、ADR-0029、ADR-0030。

## 背景

heartbeat 只能证明 agent 可达，不能证明 node-local snapshot、volume 或 dataset 的内容存在且可用。直接根据一次列表缺席或控制面缓存删除本地内容会造成不可恢复的数据丢失。

## 决策

1. agent 每次启动生成不可复用的 inventory epoch；同一 epoch 内 generation 严格递增。只有 `complete=true`、epoch 非空、generation 非零且时间合理的完整列表才是权威 observation。agent wall clock 只用于诊断，跨 epoch 顺序以控制面接收顺序为准；已退役 epoch 的迟到响应必须拒绝。
2. 控制面先持久化 observation 顺序，再在一个事务中更新资源 availability、integrity 和 observation 引用。旧 agent、不完整列表、RPC 失败或 metadata 不足只能产生 UNKNOWN，不能恢复 READY 或推导 MISSING。
3. snapshot 必须比较 immutable checksum/size/kind/compatibility；sealed DATASET_RO 必须比较 digest/size/sealed；LOCAL_RW 只验证 materialization、device/filesystem metadata。普通 presence 不能清除 CORRUPT。
4. scrub 是有预算的独立能力。不可变 snapshot 和 sealed dataset 可做内容校验；attached LOCAL_RW 禁止内容 scrub，detached LOCAL_RW 首版只做 metadata health。revision 改变后旧 scrub 立即失效。
5. 自动删除 allowlist 仅包含 OCI image cache、具有 credential-free digest-pinned 来源的可重建 DATASET_RO，以及 ledger 证明不在途的过期 spool。snapshot、LOCAL_RW、不可重建 dataset 永不自动物理删除。
6. destructive GC 必须执行：PG claim 并阻止新引用 → agent 在对象锁内重查 attachment、instance、ledger 和 revision → 同文件系统原子 rename 至 quarantine → grace period → PG 与 agent 再次重查 → finalize。grace 内出现新 root 必须 rollback。任一步查询失败都中止。
7. claim token 只保存摘要；RPC 不返回本地路径。quarantine manifest 必须先持久化并 fsync，rename 后 fsync 父目录。所有动作按 claim ID 幂等。
8. `FIREPAAS_LOCAL_GC_MODE=off|dry-run|delete` 默认 `off`。未广告 quarantine capability 的节点最多 dry-run。delete 按节点启用，禁止全局隐式开启。
9. hypeman 未提供持有其内部锁的 quarantine/scrub API 时，firepaas 只能报告和逻辑隔离，不得直接操作 hypeman 私有路径；physical delete 保持不可启用。

## 回滚

先切换 local GC 为 off，停止新 claim/scrub，将 CLAIMED/QUARANTINED 收敛至 ABORTED/ROLLED_BACK，再回滚二进制。新增表列保留，不执行 down migration。已 finalize 的可重建 cache 通过原来源重建；不可恢复资源从不进入 finalize。
