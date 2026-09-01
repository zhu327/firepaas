-- 0024_v13_snapshots.sql：v1.3-B（ADR-0028）node-local snapshot 资源。
--
-- 1. snapshots：PG 只拥有快照元数据与操作事实；v1.3 artifact 仍在 agent 本地。
--    状态机：CREATING → READY → DELETING → DELETED；
--    origin node 暂时不可达：READY → UNAVAILABLE ↔ READY；
--    LOST 只能由节点退役、人工确认或 agent inventory 权威证明不存在进入。
--    compression_* 是正交子状态，半成品不得置 READY。
-- 2. snapshot_schedules：checkpoint 调度（deterministic jitter；max_count/
--    max_age 只清理同 schedule 产物；手工 checkpoint 不绑定 schedule）。

CREATE TABLE IF NOT EXISTS snapshots (
    id                    text PRIMARY KEY,
    project_id            text NOT NULL,
    source_machine_id     text NOT NULL,
    source_execution_id   text NOT NULL,
    kind                  text NOT NULL DEFAULT 'MEMORY',   -- MEMORY | FILESYSTEM
    status                text NOT NULL DEFAULT 'CREATING', -- CREATING|READY|DELETING|DELETED|UNAVAILABLE|LOST
    node_id               text NOT NULL DEFAULT '',
    compatibility_key     text NOT NULL DEFAULT '',
    size_bytes            bigint NOT NULL DEFAULT 0,
    checksum              text NOT NULL DEFAULT '',
    compression           text NOT NULL DEFAULT 'none',     -- none|zstd|lz4
    compression_level     int,
    compression_state     text NOT NULL DEFAULT 'none',     -- none|compressing|compressed|error
    filesystem_consistency text NOT NULL DEFAULT '',        -- clean|crash-consistent
    retention_class       text NOT NULL DEFAULT '',
    schedule_id           text NOT NULL DEFAULT '',         -- 空 = 手工 checkpoint
    expires_at            timestamptz,
    lost_at               timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    CHECK (kind IN ('MEMORY','FILESYSTEM')),
    CHECK (status IN ('CREATING','READY','DELETING','DELETED','UNAVAILABLE','LOST')),
    CHECK (compression IN ('none','zstd','lz4')),
    CHECK (compression_state IN ('none','compressing','compressed','error')),
    CHECK (filesystem_consistency IN ('','clean','crash-consistent')),
    CHECK (status <> 'READY' OR (size_bytes > 0 AND checksum <> ''
        AND compression_state NOT IN ('compressing','error')))
);

CREATE INDEX IF NOT EXISTS snapshots_project ON snapshots (project_id);
CREATE INDEX IF NOT EXISTS snapshots_machine ON snapshots (source_machine_id);
CREATE INDEX IF NOT EXISTS snapshots_schedule ON snapshots (schedule_id) WHERE schedule_id <> '';
CREATE INDEX IF NOT EXISTS snapshots_retention ON snapshots (schedule_id, created_at)
    WHERE schedule_id <> '' AND status IN ('READY','UNAVAILABLE');

CREATE TABLE IF NOT EXISTS snapshot_references (
    snapshot_id    text NOT NULL REFERENCES snapshots(id),
    operation_id   text NOT NULL,
    kind           text NOT NULL CHECK (kind IN ('fork','restore')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    released_at    timestamptz,
    PRIMARY KEY (snapshot_id, operation_id)
);
CREATE INDEX IF NOT EXISTS snapshot_references_active ON snapshot_references(snapshot_id)
    WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS snapshot_schedules (
    id                text PRIMARY KEY,
    project_id        text NOT NULL,
    app_id            text NOT NULL,
    machine_id        text NOT NULL,
    interval_seconds  int NOT NULL DEFAULT 3600,
    jitter_seconds    int NOT NULL DEFAULT 0,
    max_count         int NOT NULL DEFAULT 10,
    max_age_seconds   int NOT NULL DEFAULT 0,  -- 0 = 不限
    compression       text NOT NULL DEFAULT 'none',
    compression_level int,
    enabled           boolean NOT NULL DEFAULT true,
    next_run_at       timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    CHECK (interval_seconds > 0),
    CHECK (jitter_seconds >= 0 AND jitter_seconds < interval_seconds),
    CHECK (max_count >= 0),
    CHECK (max_age_seconds >= 0),
    CHECK (compression IN ('none','zstd','lz4'))
);

CREATE INDEX IF NOT EXISTS snapshot_schedules_app ON snapshot_schedules (app_id);
