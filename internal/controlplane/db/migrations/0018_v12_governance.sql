-- 0018_v12_governance.sql：v1.2-E（ADR-0035）资源治理扩展。
--
-- 1. projects 增加 disk_mib_quota（默认 1TiB）、machine_concurrency、
--    runtime_session_concurrency 与 quota_revision（乐观锁）。配额降低
--    不驱逐已有 machine/会话，只拒绝新的 create/restart/exec/cp。
-- 2. machines.requested_disk_mib：磁盘调度/预约/准入的 requested 维度；
--    0 = 使用默认（contracts.DefaultDiskMib = 10GiB，与 hypeman 默认
--    overlay 对齐），历史行按 0 解释为默认值，不回填。

ALTER TABLE projects ADD COLUMN IF NOT EXISTS disk_mib_quota bigint NOT NULL DEFAULT 1048576;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS machine_concurrency int NOT NULL DEFAULT 64;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS runtime_session_concurrency int NOT NULL DEFAULT 32;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS quota_revision bigint NOT NULL DEFAULT 1;

ALTER TABLE machines ADD COLUMN IF NOT EXISTS requested_disk_mib bigint NOT NULL DEFAULT 0;
