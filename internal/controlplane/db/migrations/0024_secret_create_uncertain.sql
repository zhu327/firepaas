-- Persist the conservative handoff for secret-bearing CreateMachine RPCs.
-- UNCERTAIN means the RPC may have crossed the agent boundary; that execution
-- must only be fenced/deleted and must never receive another secret create.
CREATE INDEX IF NOT EXISTS sdl_uncertain_cleanup
    ON secret_delivery_leases (state, updated_at)
    WHERE state = 'UNCERTAIN';
