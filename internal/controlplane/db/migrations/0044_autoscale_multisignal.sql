-- 0044_autoscale_multisignal.sql：Wave4 多信号——RPS/CPU/自定义 target 列。
--
-- 全部默认关闭（0/空 = off），零回归：既有策略行为逐字不变，新维度缺省
-- 不参与 max-of-wants。只加列 + 默认值（apps 行锁短，可在线执行）；
-- 无 CHECK 约束（沿 0039 纪律：读时钳制 + 写入前严格校验）。
-- 回滚：策略 PUT 回 0/空即关闭新维度；彻底删列需后续 migration
--（forward-only，已发布 migration 不重写）。
ALTER TABLE apps ADD COLUMN IF NOT EXISTS target_rps int NOT NULL DEFAULT 0;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS target_cpu_ratio double precision NOT NULL DEFAULT 0;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS custom_prom_query text NOT NULL DEFAULT '';
ALTER TABLE apps ADD COLUMN IF NOT EXISTS custom_target double precision NOT NULL DEFAULT 0;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS custom_mode text NOT NULL DEFAULT 'per_replica';
