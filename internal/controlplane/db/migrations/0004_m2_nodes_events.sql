-- 0004_m2_nodes_events.sql：M2 节点投影、调度事件与操作派发节点。
-- nodes 是可重建的 observed projection（ADR-0014）；权威仍是 machines/operations。

CREATE TABLE IF NOT EXISTS nodes (
    id                text PRIMARY KEY,          -- agent node_id（ServiceInfo.node_id）
    nomad_node_id     text NOT NULL UNIQUE,      -- Nomad 节点 ID
    node_pool         text NOT NULL DEFAULT 'compute',
    status            text NOT NULL DEFAULT 'UNKNOWN',  -- HEALTHY|DRAINING|UNHEALTHY|UNKNOWN
    labels            jsonb NOT NULL DEFAULT '{}'::jsonb,
    vcpu_total        bigint NOT NULL DEFAULT 0,
    mem_total_mib     bigint NOT NULL DEFAULT 0,
    disk_total_mib    bigint NOT NULL DEFAULT 0,
    cpu_percent       double precision NOT NULL DEFAULT 0,
    mem_used_mib      bigint NOT NULL DEFAULT 0,
    mem_allocated_mib bigint NOT NULL DEFAULT 0,
    disk_used_mib     bigint NOT NULL DEFAULT 0,
    grpc_addr         text NOT NULL DEFAULT '',
    proxy_addr        text NOT NULL DEFAULT '',
    last_seen_at      timestamptz NOT NULL DEFAULT now(),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scheduler_events (
    id           bigserial PRIMARY KEY,
    at           timestamptz NOT NULL DEFAULT now(),
    kind         text NOT NULL,                  -- placement|filter_rejection|reconcile|reservation
    machine_id   text NOT NULL DEFAULT '',
    operation_id text NOT NULL DEFAULT '',
    node_id      text NOT NULL DEFAULT '',
    reason       text NOT NULL DEFAULT '',
    details      jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS scheduler_events_at ON scheduler_events (at DESC);
CREATE INDEX IF NOT EXISTS scheduler_events_machine ON scheduler_events (machine_id);

ALTER TABLE operations ADD COLUMN IF NOT EXISTS dispatch_node_id text NOT NULL DEFAULT '';
