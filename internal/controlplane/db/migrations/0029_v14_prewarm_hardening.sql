-- 0029_v14_prewarm_hardening.sql: bounded prewarm attempts/deadlines.
-- Additive and mixed-version safe: old controllers ignore these columns.

ALTER TABLE image_prewarm_targets
    ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 5,
    ADD COLUMN IF NOT EXISTS deadline_at timestamptz NOT NULL DEFAULT (now() + interval '30 minutes');

ALTER TABLE image_prewarm_targets
    DROP CONSTRAINT IF EXISTS image_prewarm_targets_attempts_check;
ALTER TABLE image_prewarm_targets
    ADD CONSTRAINT image_prewarm_targets_attempts_check
    CHECK (attempts >= 0 AND max_attempts > 0 AND attempts <= max_attempts);

CREATE INDEX IF NOT EXISTS image_prewarm_targets_pending_deadline
    ON image_prewarm_targets (deadline_at)
    WHERE status = 'PENDING';

-- Immediate pin/unpin commands need their own durable idempotency ledger. The
-- result is recorded in the same transaction as the mutation so a retry never
-- extends a pin TTL or turns a completed unpin into a 404.
CREATE TABLE IF NOT EXISTS image_pin_idempotency (
    project_id      text NOT NULL,
    idempotency_key text NOT NULL,
    kind            text NOT NULL CHECK (kind IN ('pin', 'unpin')),
    request         jsonb NOT NULL,
    result          jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, idempotency_key)
);
