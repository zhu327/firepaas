-- 0028_v14_local_integrity.sql：v1.4-B（docs/v1.4-plan.md §6）node-local
-- snapshot/volume 的正交 integrity observation。
--
-- integrity 与业务生命周期状态（status/state）正交：availability 表示节点
-- 可达性，integrity 表示本地产物完整性观测。合法值：
--   UNKNOWN            未观测（含旧 agent 不完整 inventory：只可得 UNKNOWN）
--   METADATA_VERIFIED  权威 inventory 证明产物存在且必要事实完整
--   CONTENT_VERIFIED   内容 checksum 重新计算并匹配（scrub；v1.4 未启用）
--   MISSING            完整 inventory 证明本地产物不存在
--   CORRUPT            校验失败（内容 checksum 不匹配等）
-- MISSING/CORRUPT 阻止 restore/attach；LOST 仍只能由节点退役、人工确认或
-- 权威 inventory 证据触发（不在本 migration 的自动路径内）。
--
-- 回滚兼容：仅新增带默认值列；回滚二进制保留数据。

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS integrity text NOT NULL DEFAULT 'UNKNOWN'
    CHECK (integrity IN ('UNKNOWN','METADATA_VERIFIED','CONTENT_VERIFIED','MISSING','CORRUPT'));
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS integrity_observed_at timestamptz;

ALTER TABLE volumes ADD COLUMN IF NOT EXISTS integrity text NOT NULL DEFAULT 'UNKNOWN'
    CHECK (integrity IN ('UNKNOWN','METADATA_VERIFIED','CONTENT_VERIFIED','MISSING','CORRUPT'));
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS integrity_observed_at timestamptz;
