-- 0012_v11_deployments_and_nodes.sql：v1.1 工作包 A/B/E 的部署与节点扩展。
--
-- 1. deployments.auto_standby jsonb（ADR-0017）：protojson(AutoStandbyPolicy)。
--    NULL = 未声明（默认关闭，行为与 M5 完全一致）。
-- 2. deployments.services jsonb（ADR-0022）：[{name, internal_port}]，主 service
--    为第一条。NULL/空 = 单端口（存量 port 列继续生效，零迁移成本）。
-- 3. deployments.strategy text（v1.1-F）：'bluegreen'（默认，现状全量切换）|
--    'rolling'（batch = max(1, 25%·目标副本) 逐批切换）。
-- 4. nodes.image_cache jsonb（ADR-0018）：节点本地镜像缓存 digest 列表
--    （LRU/创建序，agent ServiceInfo 20s sync 落库）。
-- 5. nodes.evacuate boolean（ADR-0021）：drain+evacuate 驱离意图标记。
--    仅标记运维意图（draining=true 且 evacuate=true 时 controller 编排驱离）；
--    驱离进度不持久化（由节点剩余 machine 数自然推导）。ready 复位时一并清除。

ALTER TABLE deployments ADD COLUMN IF NOT EXISTS auto_standby jsonb;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS services jsonb;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS strategy text NOT NULL DEFAULT 'bluegreen';

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS image_cache jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS evacuate boolean NOT NULL DEFAULT false;

-- strategy 合法性（存量行全部是默认 bluegreen）。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'deployments' AND column_name = 'strategy') THEN
        -- 仅约束未来写入；存量非法值（不应存在）由应用层校验兜底。
        EXECUTE 'ALTER TABLE deployments DROP CONSTRAINT IF EXISTS deployments_strategy_check';
        EXECUTE 'ALTER TABLE deployments ADD CONSTRAINT deployments_strategy_check '
                || 'CHECK (strategy IN (''bluegreen'', ''rolling''))';
    END IF;
END $$;
