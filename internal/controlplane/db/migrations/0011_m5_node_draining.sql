-- M5.5 升级：节点排水标记（mvp-plan §9.5 drain/rebuild 承诺）。
-- draining=true：调度不再放置新 VM（scheduler filter "draining"），
-- 已有流量继续服务。nodemanager 的正常 upsert 不覆盖本列（见 store 实现）。
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS draining boolean NOT NULL DEFAULT false;
