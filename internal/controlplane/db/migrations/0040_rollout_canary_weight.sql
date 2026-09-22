-- 0040_rollout_canary_weight.sql：P1 按权重灰度——rollout 行携带 canary 权重。
--
-- canary_weight 是 to-generation 在 PREPARING 期间的流量份额（1..99，默认
-- 10；from-generation 得 100-canary_weight）。route publisher 按它写
-- route_backends.weight，edge 按 weight 加权选择（此前 publisher 写死 100，
-- weight 列有模型无行为）。CUTOVER/ROLLING_BACK/无 rollout 时权重恒 100。
--
-- 只加列 + 默认值 + 范围约束（旧 rollout 回填默认 10，零回归）。
-- 锁说明（诚实口径）：ADD CONSTRAINT CHECK 对 rollouts 做一次全表扫描并持有
-- 短暂的 ShareUpdateExclusive 锁（阻塞并发写，不阻塞读）；rollouts 是低频
-- 追加写表（每次 deploy 一行），扫描代价可忽略。若未来该表膨胀，改用
-- NOT VALID + 独立 VALIDATE 分两步。回滚：publisher 忽略本列（全 100）+ deploy 停传参数；
-- 删列需后续 migration（forward-only，已发布 migration 不重写）。
ALTER TABLE rollouts ADD COLUMN IF NOT EXISTS canary_weight int NOT NULL DEFAULT 10;
ALTER TABLE rollouts DROP CONSTRAINT IF EXISTS rollouts_canary_weight_range;
ALTER TABLE rollouts ADD CONSTRAINT rollouts_canary_weight_range CHECK (canary_weight BETWEEN 1 AND 99);
