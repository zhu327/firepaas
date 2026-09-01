-- 0015_v12_capabilities.sql：v1.2-A（ADR-0023）runtime capability discovery。
--
-- 1. nodes.feature_ids jsonb：agent ServiceInfo 上报的稳定 feature ID 列表
--    （未知 ID 语义上可忽略，但完整落库以便审计/能力统计）。
-- 2. nodes.protocol_version / nodes.snapshot_compatibility_key：agent 契约
--    版本与 snapshot 兼容键（跨节点 snapshot 移植判断输入）。
-- 3. deployments.required_features jsonb：平台从 deployment 语义推导的启动
--    正确性能力（例如绑定 secret_refs 的 deployment 需要 secret.oneshot.v1）。
--    客户端不得直接声明内部 feature；列只由控制面写入。

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS feature_ids jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS protocol_version text NOT NULL DEFAULT '';
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS snapshot_compatibility_key text NOT NULL DEFAULT '';

ALTER TABLE deployments ADD COLUMN IF NOT EXISTS required_features jsonb NOT NULL DEFAULT '[]'::jsonb;
