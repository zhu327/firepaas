-- 0016_v12_lifecycle.sql：v1.2-D（ADR-0026）lifecycle wait / TTL / restart。
--
-- 1. machines.expires_at：绝对到期时间（NULL = 关闭）。到期先摘 route，
--    再下发 fenced delete；已删除/已过期 machine 不能通过更新 TTL 复活。
-- 2. machines.restart_*：控制面是 restart policy 唯一权威（不启用 hypeman
--    本地 restart controller）。attempts 按 logical machine 持久化；
--    stable window 从新 execution READY 开始，连续 READY 满窗口才清零。
--    超限进入 restart_blocked，直到新 deployment 或管理员 reset。

ALTER TABLE machines ADD COLUMN IF NOT EXISTS expires_at timestamptz;

ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_mode text NOT NULL DEFAULT 'NEVER';
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_max_attempts int NOT NULL DEFAULT 3;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_backoff_seconds int NOT NULL DEFAULT 10;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_stable_window_seconds int NOT NULL DEFAULT 300;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_attempts int NOT NULL DEFAULT 0;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_next_attempt_at timestamptz;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_stable_since timestamptz;
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_blocked boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS machines_expires_at
    ON machines (expires_at) WHERE expires_at IS NOT NULL;
