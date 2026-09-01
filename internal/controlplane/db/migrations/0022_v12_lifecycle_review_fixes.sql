-- 0022_v12_lifecycle_review_fixes.sql: staged TTL detach and execution-bound restart delay.
-- Additive migration: 0016 may already be deployed and must not be rewritten.

ALTER TABLE machines ADD COLUMN IF NOT EXISTS lifecycle_delete_phase text NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE machines ADD COLUMN IF NOT EXISTS restart_failed_execution_id text;

ALTER TABLE machines DROP CONSTRAINT IF EXISTS machines_lifecycle_delete_phase_check;
ALTER TABLE machines ADD CONSTRAINT machines_lifecycle_delete_phase_check
    CHECK (lifecycle_delete_phase IN ('ACTIVE', 'ROUTE_DETACHED'));

CREATE INDEX IF NOT EXISTS machines_lifecycle_delete_phase
    ON machines (lifecycle_delete_phase) WHERE lifecycle_delete_phase <> 'ACTIVE';
