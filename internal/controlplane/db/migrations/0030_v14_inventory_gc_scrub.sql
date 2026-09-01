-- v1.4 inventory ordering, scrub observations and fail-closed local GC claims.
-- Additive migration: old binaries ignore these tables/columns. New destructive
-- workers remain disabled unless FIREPAAS_LOCAL_GC_MODE=delete is explicitly set.

CREATE TABLE IF NOT EXISTS local_inventory_observations (
    id                  bigserial PRIMARY KEY,
    node_id             text NOT NULL,
    resource_type       text NOT NULL CHECK (resource_type IN ('snapshot','volume')),
    epoch               text NOT NULL,
    generation          bigint NOT NULL CHECK (generation > 0),
    agent_observed_at   timestamptz NOT NULL,
    received_at         timestamptz NOT NULL DEFAULT now(),
    item_count          integer NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    UNIQUE (node_id, resource_type, epoch, generation)
);
CREATE INDEX IF NOT EXISTS local_inventory_observations_latest
    ON local_inventory_observations(node_id, resource_type, received_at DESC);

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS inventory_epoch text;
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS inventory_generation bigint;
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS inventory_received_at timestamptz;
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS inventory_observation_id bigint;
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS integrity_reason text NOT NULL DEFAULT '';

ALTER TABLE volumes ADD COLUMN IF NOT EXISTS inventory_epoch text;
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS inventory_generation bigint;
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS inventory_received_at timestamptz;
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS inventory_observation_id bigint;
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS integrity_reason text NOT NULL DEFAULT '';
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS materialization_revision text NOT NULL DEFAULT '';
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS source_url_digest text NOT NULL DEFAULT '';
ALTER TABLE volumes ADD COLUMN IF NOT EXISTS rebuildable boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS local_scrub_jobs (
    id                  text PRIMARY KEY,
    project_id          text NOT NULL,
    node_id             text NOT NULL,
    resource_type       text NOT NULL CHECK (resource_type IN ('snapshot','volume')),
    resource_id         text NOT NULL,
    expected_revision   text NOT NULL DEFAULT '',
    status              text NOT NULL CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','STALE')),
    integrity           text NOT NULL DEFAULT 'UNKNOWN' CHECK (integrity IN ('UNKNOWN','METADATA_VERIFIED','CONTENT_VERIFIED','CORRUPT')),
    checksum            text NOT NULL DEFAULT '',
    bytes_read          bigint NOT NULL DEFAULT 0 CHECK (bytes_read >= 0),
    duration_ms         bigint NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    error               text NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE(resource_type, resource_id, expected_revision, status)
);

CREATE TABLE IF NOT EXISTS local_gc_claims (
    id                      text PRIMARY KEY,
    node_id                 text NOT NULL,
    artifact_type           text NOT NULL CHECK (artifact_type IN ('image','dataset','spool')),
    artifact_key            text NOT NULL,
    project_id              text NOT NULL DEFAULT '',
    status                  text NOT NULL CHECK (status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED','ROLLED_BACK','DELETED','ABORTED')),
    claim_token_hash        text NOT NULL,
    observation_epoch       text NOT NULL DEFAULT '',
    observation_generation  bigint NOT NULL DEFAULT 0,
    artifact_revision       text NOT NULL DEFAULT '',
    size_bytes              bigint NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    grace_until             timestamptz NOT NULL,
    reason                  text NOT NULL DEFAULT '',
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS local_gc_claims_active
    ON local_gc_claims(node_id, artifact_type, artifact_key)
    WHERE status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED');
CREATE INDEX IF NOT EXISTS local_gc_claims_due
    ON local_gc_claims(status, grace_until);
