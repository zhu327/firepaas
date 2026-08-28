-- 0013_scheduler_events_project.sql：项目级 scheduler event 隔离。
-- 空 project_id 仅用于不能可靠归属某个项目的平台运维事件；API 仅向 admin 返回它们。
ALTER TABLE scheduler_events
    ADD COLUMN IF NOT EXISTS project_id text NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS scheduler_events_project_at
    ON scheduler_events (project_id, at DESC, id DESC);
