-- M5.1 补充：把 api_keys 收敛为 Manager 形状。
--   - name/expires_at/revoked_at：软撤销 + 过期（manager 语义）。
--   - project_id 可空：NULL = 全部项目（受限键才填具体 project）。
--   - key_hash 唯一：防重复导入导致检索歧义。
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at timestamptz;
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS revoked_at timestamptz;
ALTER TABLE api_keys ALTER COLUMN project_id DROP NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS api_keys_key_hash_unique ON api_keys (key_hash);
