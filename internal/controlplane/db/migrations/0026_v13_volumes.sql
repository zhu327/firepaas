-- 0026_v13_volumes.sql：v1.3-D（ADR-0029）/ v1.3-E（ADR-0030）volume 控制面。
--
-- 1. volumes：PG 是 volume/attachment 业务事实权威；agent 是本地
--    materialization/attachment 观测权威。本迁移交付 LOCAL_RW（单写、硬钉
--    origin node）；DATASET_RO 的 import/seal 由 0026 启用。
-- 2. volume_attachments：绑定 machine/execution；旧 execution 的
--    attach/detach 不得影响新代（fencing 在操作层执行，表内保留历史行）。
-- 3. dataset seal：DATASET_RO 的 digest 密封事实（base 不可变）。
-- 4. source_generation added here to keep the already-published 0025 migration
--    immutable while ensuring snapshot deletion uses the captured fence.

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS source_generation bigint NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS volumes (
    id              text PRIMARY KEY,
    project_id      text NOT NULL,
    name            text NOT NULL,
    mode            text NOT NULL DEFAULT 'LOCAL_RW'
                    CHECK (mode IN ('LOCAL_RW', 'DATASET_RO')),
    node_id         text NOT NULL,                    -- origin node（硬钉）
    size_bytes      bigint NOT NULL DEFAULT 0,
    state           text NOT NULL DEFAULT 'CREATING'
                    CHECK (state IN ('CREATING','READY','UNAVAILABLE','DELETING','DELETED','LOST')),
    content_digest  text NOT NULL DEFAULT '',          -- DATASET_RO seal digest
    import_status   text NOT NULL DEFAULT '',          -- DATASET_RO: importing|sealed|failed
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deleted_at      timestamptz,
    UNIQUE (project_id, name)
);

CREATE INDEX IF NOT EXISTS volumes_project ON volumes (project_id);
CREATE UNIQUE INDEX IF NOT EXISTS volumes_project_dataset_digest
    ON volumes(project_id, content_digest)
    WHERE mode='DATASET_RO' AND content_digest<>'' AND state<>'DELETED';

CREATE TABLE IF NOT EXISTS volume_attachments (
    volume_id          text NOT NULL REFERENCES volumes(id) ON DELETE CASCADE,
    machine_id         text NOT NULL,
    execution_id       text NOT NULL,
    mount_path         text NOT NULL DEFAULT '',
    readonly           boolean NOT NULL DEFAULT false,
    overlay_size_bytes bigint NOT NULL DEFAULT 0, -- DATASET_RO per-execution CoW
    status             text NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN ('PENDING','ATTACHED','DETACHING','DETACHED')),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (volume_id, machine_id, execution_id),
    CHECK (overlay_size_bytes >= 0),
    CHECK (overlay_size_bytes = 0 OR readonly)
);

CREATE INDEX IF NOT EXISTS volume_attachments_machine ON volume_attachments (machine_id);

-- LOCAL_RW 首版的所有 volume 都是单写；DETACHING 仍持有 writer fence，只有
-- agent 确认 detach 后变成 DETACHED 才允许下一位 writer。
CREATE UNIQUE INDEX IF NOT EXISTS volume_attachments_single_writer
    ON volume_attachments (volume_id)
    WHERE status IN ('PENDING','ATTACHED','DETACHING')
      AND NOT readonly;
