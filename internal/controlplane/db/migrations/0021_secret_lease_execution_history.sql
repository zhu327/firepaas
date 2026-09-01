-- 0021_secret_lease_execution_history.sql：ADR-0024 的不可重签约束。
-- 同一 machine/execution 的任何历史 lease（包括终态）都会阻止再次签发。
-- 若旧版本已经产生重复历史，本 migration 明确失败，保留审计事实供人工处理，
-- 不静默删除或合并记录。

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM secret_delivery_leases
        GROUP BY machine_id, execution_id
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate secret delivery lease history exists for machine/execution';
    END IF;
END $$;

DROP INDEX IF EXISTS sdl_one_active_per_execution;

CREATE UNIQUE INDEX sdl_one_per_machine_execution
    ON secret_delivery_leases (machine_id, execution_id);
