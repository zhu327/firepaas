-- 0001_init.sql：M1 最小 PG 模型（architecture.md §4.2 裁剪）。
-- 状态权威：PG desired/business truth；agent observed；Redis 可重建投影。

CREATE TABLE IF NOT EXISTS projects (
    id           text PRIMARY KEY,
    name         text NOT NULL,
    vcpu_quota   bigint NOT NULL DEFAULT 64,
    mem_mib_quota bigint NOT NULL DEFAULT 32768,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id           text PRIMARY KEY,
    project_id   text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key_hash     text NOT NULL,
    scopes       text[] NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);

CREATE TABLE IF NOT EXISTS apps (
    id              text PRIMARY KEY,
    project_id      text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    hostname        text NOT NULL UNIQUE,
    image_ref       text NOT NULL DEFAULT '',
    vcpu            bigint NOT NULL DEFAULT 1,
    mem_mib         bigint NOT NULL DEFAULT 512,
    desired_replicas int NOT NULL DEFAULT 1,
    generation      bigint NOT NULL DEFAULT 1,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS machines (
    id                   text PRIMARY KEY,
    app_id               text NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    deployment_id        text NOT NULL DEFAULT '',
    replica_ordinal      int NOT NULL DEFAULT 0,
    hostname             text NOT NULL,
    desired_state        text NOT NULL DEFAULT 'CREATED',
    generation           bigint NOT NULL DEFAULT 1,
    current_execution_id text NOT NULL DEFAULT '',
    requested_vcpu       bigint NOT NULL DEFAULT 1,
    requested_mem_mib    bigint NOT NULL DEFAULT 512,
    image_ref            text NOT NULL,
    env                  jsonb NOT NULL DEFAULT '{}'::jsonb,
    node_id              text NOT NULL DEFAULT '',
    observed_state       text NOT NULL DEFAULT '',
    observed_slot_ip     text NOT NULL DEFAULT '',
    observed_readiness   text NOT NULL DEFAULT 'UNKNOWN',
    last_observed_at     timestamptz,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (app_id, replica_ordinal)
);

CREATE TABLE IF NOT EXISTS operations (
    id               text PRIMARY KEY,           -- operation_id
    project_id       text NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    machine_id       text NOT NULL,
    execution_id     text NOT NULL DEFAULT '',
    generation       bigint NOT NULL DEFAULT 1,
    kind             text NOT NULL,              -- create | delete
    idempotency_key  text NOT NULL,
    status           text NOT NULL DEFAULT 'PENDING',  -- PENDING | CLAIMED | SUCCEEDED | FAILED
    request          jsonb NOT NULL DEFAULT '{}'::jsonb,
    result           jsonb,
    error            text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    claimed_at       timestamptz,
    completed_at     timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS operations_project_idempotency
    ON operations (project_id, idempotency_key);
CREATE INDEX IF NOT EXISTS operations_status_created
    ON operations (status, created_at);

CREATE TABLE IF NOT EXISTS routes (
    id                text PRIMARY KEY,
    app_id            text NOT NULL UNIQUE REFERENCES apps(id) ON DELETE CASCADE,
    hostname          text NOT NULL UNIQUE,
    active_generation bigint NOT NULL DEFAULT 1,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS route_backends (
    route_id            text NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    generation          bigint NOT NULL,
    machine_id          text NOT NULL,
    execution_id        text NOT NULL,
    node_proxy_endpoint text NOT NULL DEFAULT '',
    app_port            int NOT NULL DEFAULT 8080,
    weight              int NOT NULL DEFAULT 100,
    readiness           text NOT NULL DEFAULT 'UNKNOWN',
    draining            boolean NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (route_id, generation, machine_id)
);

INSERT INTO projects (id, name) VALUES ('dev', 'development') ON CONFLICT (id) DO NOTHING;
