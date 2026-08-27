-- M5.1 内部生产就绪：API key 哈希存储 + 最小 scope + 按 project 限制（mvp-plan §9.1）。
-- 说明：项目实验室 DB 已先行落地同版本 schema（scopes text[]、project_id FK）；
-- 本文件与线上保持一致，name/expires_at/revoked_at/唯一 hash 索引由 0010 追加。
CREATE TABLE IF NOT EXISTS api_keys (
    id           text PRIMARY KEY,
    project_id   text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key_hash     text NOT NULL,
    scopes       text[] NOT NULL DEFAULT '{}'::text[],
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);
CREATE INDEX IF NOT EXISTS api_keys_project_created
    ON api_keys (project_id, created_at);
