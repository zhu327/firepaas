-- 0019_v12_rate_limits.sql：v1.2-E（ADR-0035）API 限流配置。
--
-- 按 project + route_class（read / mutation / runtime-stream）的令牌桶参数。
-- 无行的 project 使用代码内默认值（同下方 DEFAULT）；rate = 每秒令牌，
-- burst = 桶容量。桶状态在 Redis（原子 Lua），本表只存配置（PG 权威）。

CREATE TABLE IF NOT EXISTS project_rate_limits (
    project_id    text PRIMARY KEY,
    read_rate     double precision NOT NULL DEFAULT 100,
    read_burst    double precision NOT NULL DEFAULT 200,
    mutation_rate double precision NOT NULL DEFAULT 20,
    mutation_burst double precision NOT NULL DEFAULT 40,
    stream_rate   double precision NOT NULL DEFAULT 5,
    stream_burst  double precision NOT NULL DEFAULT 10,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
