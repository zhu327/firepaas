-- 0020_v12_user_events_and_gc.sql：v1.2-F（v1.2-plan §9）用户事件与引用感知 GC。
--
-- 1. user_events：append-only 租户事件（machine/rollout/restart/expiry/
--    secret delivery/quota/rate-limit），与内部 scheduler_events 分离。
--    所有租户事件必须有 project attribution；details 只存脱敏摘要。
--    保留期默认 7 天、上限 30 天（controller 周期删除）。
-- 2. gc_seen_digests：引用感知镜像 GC 的 digest 首见时间（最小年龄的
--    权威来源；跨 controller 重启持久）。

CREATE TABLE IF NOT EXISTS user_events (
    id         bigserial PRIMARY KEY,
    at         timestamptz NOT NULL DEFAULT now(),
    project_id text NOT NULL,
    app_id     text NOT NULL DEFAULT '',
    machine_id text NOT NULL DEFAULT '',
    type       text NOT NULL,
    details    jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS user_events_project_id_desc
    ON user_events (project_id, id DESC);
CREATE INDEX IF NOT EXISTS user_events_machine
    ON user_events (machine_id) WHERE machine_id <> '';

CREATE TABLE IF NOT EXISTS gc_seen_digests (
    node_id    text NOT NULL,
    digest     text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id, digest)
);
