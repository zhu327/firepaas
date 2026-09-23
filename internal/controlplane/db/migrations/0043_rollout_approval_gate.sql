-- 0043_rollout_approval_gate.sql：Wave3 Stage B——人工审批 gate。
--
-- 新增 PAUSED_FOR_APPROVAL 态（PREPARING(allReady)→PAUSED→CUTOVER），只在
-- 审批 gate 生效时进入（rolling canary 显式启用，或 deploy 带
-- approval_required=true opt-in）；PAUSED 无自动超时回滚，无限期等人工
-- （Argo pause step 语义）。
--
-- approval_required/deploy 时写入；approved_by/approved_at 在放行时写入。
-- 单 rollout 互斥的部分唯一索引重建纳入新态（否则 PAUSED 期间可并发起
-- 第二 rollout）。
--
-- 执行窗口：索引重建（DROP+CREATE，非 CONCURRENTLY——迁移器逐文件包事务，
-- CONCURRENTLY 不可用）短暂阻塞 rollouts 写（DeployApp/RolloutTo*），请在
-- 发布静默期执行；rollouts 是低频追加写表，阻塞窗口通常毫秒级。
--
-- 回滚（诚实口径）：旧二进制看不见 PAUSED 行（计数查询与终结路径都不含该态），
-- 但新索引仍把它算作 active——残留 PAUSED 会导致该 app 后续 deploy 撞唯一索引
-- 报 ErrRolloutBusy。降级前必须先终结所有 PAUSED（放行或手工完成）：
--   UPDATE rollouts SET status='COMPLETE', failed=false, completed_at=now(),
--     updated_at=now() WHERE status='PAUSED_FOR_APPROVAL';
-- 删列需后续 migration（forward-only，已发布 migration 不重写）。
ALTER TABLE rollouts ADD COLUMN IF NOT EXISTS approval_required boolean NOT NULL DEFAULT false;
ALTER TABLE rollouts ADD COLUMN IF NOT EXISTS approved_by text NOT NULL DEFAULT '';
ALTER TABLE rollouts ADD COLUMN IF NOT EXISTS approved_at timestamptz;
DROP INDEX IF EXISTS rollouts_one_active_per_app;
CREATE UNIQUE INDEX IF NOT EXISTS rollouts_one_active_per_app
    ON rollouts (app_id) WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK','PAUSED_FOR_APPROVAL');
