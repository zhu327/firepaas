-- 0006_m3_apps_rollouts.sql：M3 发布状态机（ADR-0015）+ 稳定 ordinal 模型。
--
-- 关键决策：
-- 1. machine 唯一键从 (app_id, replica_ordinal) 放宽为
--    (app_id, replica_ordinal, generation)：发布期间新旧 generation 的
--    同 ordinal 副本必须同时在 PG/agent 存在（旧代 draining，新代服务）。
-- 2. machine_id 采用 {app}-r{ordinal}-g{gen} 稳定推导（显式传入不受影响）。
-- 3. rollouts 每 app 至多一条活跃行（数据库级单 rollout 互斥）。

CREATE TABLE IF NOT EXISTS deployments (
    id            text PRIMARY KEY,
    app_id        text NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    generation    bigint NOT NULL,
    image_ref     text NOT NULL,
    vcpu          bigint NOT NULL DEFAULT 1,
    mem_mib       bigint NOT NULL DEFAULT 512,
    port          int NOT NULL DEFAULT 8080,
    env           jsonb NOT NULL DEFAULT '{}'::jsonb,
    placement     jsonb,
    health_check  jsonb,
    status        text NOT NULL DEFAULT 'PREPARING',  -- PREPARING|ACTIVE|SUPERSEDED|FAILED
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (app_id, generation)
);

CREATE TABLE IF NOT EXISTS rollouts (
    id              text PRIMARY KEY,
    app_id          text NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    from_generation bigint NOT NULL,
    to_generation   bigint NOT NULL,
    status          text NOT NULL DEFAULT 'PREPARING',  -- PREPARING|CUTOVER|ROLLING_BACK|COMPLETE
    failed          boolean NOT NULL DEFAULT false,
    cutover_at      timestamptz,
    drain_deadline  timestamptz,
    started_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS rollouts_one_active_per_app
    ON rollouts (app_id) WHERE status IN ('PREPARING','CUTOVER','ROLLING_BACK');

-- (app_id, replica_ordinal, deployment_id) 唯一：同 deployment 并发重试幂等键，
-- 跨 deployment（发布窗口新旧代）并行共存。注意不能用 generation：R3 换代
-- 重建会 machine.generation+1（fence 语义），同 ordinal 跨代会撞唯一键。
ALTER TABLE machines DROP CONSTRAINT IF EXISTS machines_app_id_replica_ordinal_key;
DROP INDEX IF EXISTS machines_app_ordinal_generation;
CREATE UNIQUE INDEX IF NOT EXISTS machines_app_ordinal_deployment
    ON machines (app_id, replica_ordinal, deployment_id);

CREATE INDEX IF NOT EXISTS machines_deployment
    ON machines (deployment_id);
