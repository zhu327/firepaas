-- ADR-0041 并发自动弹性：app 级策略列（PG 权威，Redis 不保存策略）。
--
-- 只加列 + 默认值（默认即当前行为：关闭、min=1，零回归）；apps 行锁短，
-- 可在线执行。回滚：enabled=false + 停 ticker；彻底移除需后续 migration
-- 删列（forward-only，已发布 migration 不重写）。
ALTER TABLE apps ADD COLUMN IF NOT EXISTS autoscale_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS min_replicas int NOT NULL DEFAULT 1;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS max_replicas int NOT NULL DEFAULT 10;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS target_concurrency int NOT NULL DEFAULT 20;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS scale_down_delay_sec int NOT NULL DEFAULT 120;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS panic_threshold double precision NOT NULL DEFAULT 2.0;
