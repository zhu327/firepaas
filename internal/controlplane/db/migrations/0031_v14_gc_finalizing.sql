-- Keep the GC root-admission fence durable across an ambiguous finalize RPC.
ALTER TABLE local_gc_claims DROP CONSTRAINT IF EXISTS local_gc_claims_status_check;
ALTER TABLE local_gc_claims ADD CONSTRAINT local_gc_claims_status_check
    CHECK (status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED','ROLLED_BACK','DELETED','ABORTED'));
DROP INDEX IF EXISTS local_gc_claims_active;
CREATE UNIQUE INDEX local_gc_claims_active
    ON local_gc_claims(node_id, artifact_type, artifact_key)
    WHERE status IN ('CLAIMED','QUARANTINED','FINALIZING','ROLLBACK_REQUESTED');
