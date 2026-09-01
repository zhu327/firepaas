-- 0017_v12_secret_leases.sql：v1.2-B（ADR-0024）one-shot secret delivery lease。
--
-- 状态机：ISSUED → CLAIMED → DELIVERED → ACKED | EXPIRED | REVOKED。
-- 1. PG 只持久化不含明文的 lease metadata；明文只在控制面/agent 内存短暂
--    存在，经 mTLS Create RPC 一次性下发。
-- 2. 每个 execution 至多一条非终态 lease（唯一部分索引）：二次签发必须换
--    execution；同一 op 的换节点重派复用同一 lease。
-- 3. 转换全部走 CAS（WHERE state IN ...），ACK 观测必须与 lease 行的
--    machine/execution/operation 匹配。
-- 4. 过期（ISSUED/CLAIMED/DELIVERED 超过 expires_at）→ EXPIRED；
--    execution 被替换或 machine 终态 → REVOKED。回收器在 controller 周期执行。

CREATE TABLE IF NOT EXISTS secret_delivery_leases (
    id            text PRIMARY KEY,
    project_id    text NOT NULL,
    machine_id    text NOT NULL,
    execution_id  text NOT NULL,
    generation    bigint NOT NULL,
    operation_id  text NOT NULL,
    request_hash  text NOT NULL,
    state         text NOT NULL,
    expires_at    timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sdl_machine_exec
    ON secret_delivery_leases (machine_id, execution_id);

CREATE UNIQUE INDEX IF NOT EXISTS sdl_one_active_per_execution
    ON secret_delivery_leases (execution_id)
    WHERE state IN ('ISSUED', 'CLAIMED', 'DELIVERED');

CREATE INDEX IF NOT EXISTS sdl_reap
    ON secret_delivery_leases (state, expires_at)
    WHERE state IN ('ISSUED', 'CLAIMED', 'DELIVERED');
