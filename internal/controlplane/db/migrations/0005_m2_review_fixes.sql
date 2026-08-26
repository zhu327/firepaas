-- 0005_m2_review_fixes.sql：M2 评审修复（P1/P2/P3）。
-- 语义变更：
--   1. operations.attempts：claim 尝试计数，驱动指数退避重派（P1-3/P3-2），
--      与 claimed_at（最近一次领取时间）配合，requeue 后不再立即被重新领取。
--   2. machines.placement：持久化 MachineSpec.placement（P2-6）。换代重建/
--      退避重派必须保留放置约束（node_pool/labels/anti_affinity），否则
--      多副本 app 重建后反亲和失效，违反 ADR-0009。
--   3. operations 状态相关查询索引（claim 退避与 stale-claim 回收路径）。

ALTER TABLE operations ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS placement jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS operations_status_updated
    ON operations (status, updated_at);
