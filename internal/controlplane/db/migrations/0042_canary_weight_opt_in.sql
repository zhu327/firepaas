-- 0042_canary_weight_opt_in.sql：canary 权重改为显式 opt-in（0 = 不启用）。
--
-- 背景：0040 引入 canary_weight（DEFAULT 10，CHECK 1..99），配合 route publisher
-- 在 PREPARING 按代写权重，会把既有 rolling 部署在混合代窗口内的“按 ordinal
-- 切流”变成固定 10/90（ADR-0043 §1/§2 要求 bluegreen/rolling 行为不变）。
-- 本迁移把语义收敛为显式 opt-in：
--   canary_weight = 0     未启用：全部 backend 权重 100（历史行为，默认）；
--   canary_weight = 1..99 显式启用：to 代得该份额、from 代得 100-份额，
--                         且仅在 publisher 同时发布两代的可服务 backend 时生效。
-- 列默认值同步改为 0。存量行（0040 之后写入、值为 10 的 rollout）保持原值，
-- 只在自身发布窗口内按 10/90 分流；无需数据回填（发布结束即失效）。
-- 只改默认值与放宽约束（不增删列、不重写数据）；ADD CONSTRAINT 会做一次全表
-- 扫描并持有短暂 ShareUpdateExclusive 锁（rollouts 为低频追加写表）。
-- 回滚：约束恢复 1..99 前需先清 0 值行；代码回滚 = publisher 忽略该列。
ALTER TABLE rollouts ALTER COLUMN canary_weight SET DEFAULT 0;
ALTER TABLE rollouts DROP CONSTRAINT IF EXISTS rollouts_canary_weight_range;
ALTER TABLE rollouts ADD CONSTRAINT rollouts_canary_weight_range CHECK (canary_weight BETWEEN 0 AND 99);
