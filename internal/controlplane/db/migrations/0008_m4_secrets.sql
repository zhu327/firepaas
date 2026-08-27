-- 0008_m4_secrets.sql：M4 secrets v1（ADR-0010）。
--
-- 决策（ADR-0010 §1/§2）：
-- 1. 值与引用分离：secrets 表存信封加密后的密文；PG 中绝不出现明文。
--    - value_ciphertext = nonce(12B) || AES-256-GCM(DEK, plaintext)，AAD 绑定
--      (project, name, version) 防止行间密文互换；
--    - dek_wrapped       = wnonce(12B) || AES-256-GCM(master_key, DEK)。
-- 2. 引用按 project 内 name 解析，version 可选（缺省 = 最新版本）。
-- 3. deployments.secret_refs：app 级绑定随 deployment 固化（发布不可变语义，
--    ADR-0005），controller 在 create 时解析为 CreateMachineRequest.secret_env。
-- 4. 更新 secret 产生新版本；改 refs 走新 deployment 触发 rollout。

CREATE TABLE IF NOT EXISTS secrets (
    id               text PRIMARY KEY,
    project_id       text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name             text NOT NULL,
    version          bigint NOT NULL,
    value_ciphertext bytea NOT NULL,
    dek_wrapped      bytea NOT NULL,
    key_version      int NOT NULL DEFAULT 1,
    created_by       text NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project_id, name, version)
);

CREATE INDEX IF NOT EXISTS secrets_project_name_version
    ON secrets (project_id, name, version DESC);

ALTER TABLE deployments ADD COLUMN IF NOT EXISTS secret_refs jsonb NOT NULL DEFAULT '{}'::jsonb;
