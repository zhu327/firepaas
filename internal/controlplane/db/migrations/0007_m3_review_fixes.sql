-- 0007_m3_review_fixes.sql：M3 评审修复（P0/P2）。
--
-- 1. apps.deleted_at：app 生命周期终态标记（P0-1）。deleteApp 置位后
--    reconcileAppScale 不再补建副本，消灭"删除的 app 被 controller 复活"。
-- 2. deployments 无 schema 变化（终态语义沿用 status 文本）。

ALTER TABLE apps ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
