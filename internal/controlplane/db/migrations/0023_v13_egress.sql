-- 0023_v13_egress.sql：v1.3-A（ADR-0027）域名/SNI egress policy 与审计。
--
-- 1. deployments.egress_policy jsonb：policy 属于不可变 deployment；修改策略
--    走既有 rollout（新 generation 新机器）。NULL = 未声明（历史行为）。
--    policy_generation 随 deployment generation 单调递增。
-- 2. egress_deny_summaries：PG 只保存策略事实（deployment 行）与拒绝摘要
--    （本表，按 machine/execution/policy_generation 聚合）。明细审计走 agent
--    日志 sink；本表不含 Host/SNI 明细，不构成高基数标签。
-- 3. egress_policy_changes：策略变更事实审计（rollout 派生，保留策略 JSON
--    摘要供审计追溯，与 deployment 行一一对应）。

ALTER TABLE deployments ADD COLUMN IF NOT EXISTS egress_policy jsonb;

CREATE TABLE IF NOT EXISTS egress_deny_summaries (
    project_id         text NOT NULL,
    app_id             text NOT NULL,
    machine_id         text NOT NULL,
    execution_id       text NOT NULL,
    policy_generation  bigint NOT NULL,
    allowed_connections bigint NOT NULL DEFAULT 0,
    denied_connections  bigint NOT NULL DEFAULT 0,
    limit_rejections    bigint NOT NULL DEFAULT 0,
    deny_buckets        jsonb NOT NULL DEFAULT '[]'::jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (machine_id, execution_id, policy_generation)
);

CREATE INDEX IF NOT EXISTS egress_deny_summaries_project
    ON egress_deny_summaries (project_id);

CREATE TABLE IF NOT EXISTS egress_policy_changes (
    id                bigserial PRIMARY KEY,
    project_id        text NOT NULL,
    app_id            text NOT NULL,
    deployment_id     text NOT NULL,
    generation        bigint NOT NULL,
    policy            jsonb NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS egress_policy_changes_deployment
    ON egress_policy_changes (deployment_id);

CREATE INDEX IF NOT EXISTS egress_policy_changes_app
    ON egress_policy_changes (app_id, generation DESC);
