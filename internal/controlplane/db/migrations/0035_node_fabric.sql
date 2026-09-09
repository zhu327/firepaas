-- ADR-0040 §18（T4b）：节点级 fabric 推送水位（desired 侧权威）。
-- fabric_versions：控制面 reconciler 的 per-node 推送水位与快照内容哈希；
-- 只升不降，与 agent 侧节点级 fencing（node_id + fabric_generation +
-- operation_id）配套。
CREATE TABLE IF NOT EXISTS fabric_versions (
    node_id       text PRIMARY KEY,
    generation    bigint NOT NULL DEFAULT 0,
    snapshot_hash text NOT NULL DEFAULT '',
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- node /64 全局唯一：跨节点聚合路由（peer AllowedIPs）不得重叠。
CREATE UNIQUE INDEX IF NOT EXISTS wg_peers_node_prefix_active_idx
    ON wg_peers (node_prefix);
