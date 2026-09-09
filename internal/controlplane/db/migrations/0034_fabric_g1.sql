-- ADR-0040 G1/G2：网络 fabric 的 desired 事实表（§18）。
-- 全部为控制面权威；Redis 只存可重建投影。本 migration 只加新表，不回填、
-- 不收紧既有约束。回滚方案：全新表无既有数据依赖，DROP TABLE + DROP SEQUENCE
-- 即回滚（若未来已有业务行，须先按行级迁移导出）。

-- 稳定层 service 身份（ADR-0040 §6）：identity_id 由控制面分配（PG 唯一），
-- 是 eBPF policy map 的唯一 key；跨 execution 稳定，不随 ULA 轮换。
CREATE TABLE IF NOT EXISTS workload_identities (
    identity_id  integer PRIMARY KEY,
    trust_domain text NOT NULL,
    project_id   text NOT NULL,
    app_id       text NOT NULL,
    service      text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project_id, app_id, service)
);

CREATE SEQUENCE IF NOT EXISTS workload_identity_ids AS integer START WITH 1;

-- ULA /128 分配（ADR-0040 §7/§8）：execution 生命周期内稳定、单节点内有效；
-- 跨节点迁移 = 新 execution + 重编号。行保留不删（release 只置 released_at，
-- 支撑回滚 §3“identity/ipam/peer/policy 行保留”与重放审计）；地址可回收：
-- 唯一性由 active-only 部分索引保证，释放后可重新分配给新 execution。
CREATE TABLE IF NOT EXISTS ipam_allocations (
    ula          inet NOT NULL,            -- /128 workload 地址（唯一权威分配）
    cell_prefix  inet NOT NULL,             -- cell /40（G3 联邦前为单 cell 前缀）
    project_id   text NOT NULL,
    node_id      text NOT NULL,
    machine_id   text NOT NULL,
    execution_id text NOT NULL,
    generation   bigint NOT NULL,           -- machine generation（fence 水位）
    allocated_at timestamptz NOT NULL DEFAULT now(),
    released_at  timestamptz                -- NULL = 在役
);

CREATE UNIQUE INDEX IF NOT EXISTS ipam_allocations_ula_active_idx
    ON ipam_allocations (ula) WHERE released_at IS NULL;
-- 同一 machine+execution 在役唯一：并发重放/重试只能收敛到一条 /128
--（check-then-act 的插入冲突由此索引在事务层仲裁）。
CREATE UNIQUE INDEX IF NOT EXISTS ipam_allocations_machine_exec_active_idx
    ON ipam_allocations (machine_id, execution_id) WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS ipam_allocations_project_idx
    ON ipam_allocations (project_id);
CREATE INDEX IF NOT EXISTS ipam_allocations_node_active_idx
    ON ipam_allocations (node_id) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS ipam_allocations_machine_exec_idx
    ON ipam_allocations (machine_id, execution_id);

-- WG peer（ADR-0040 §9）：节点级，AllowedIPs 按 node /64 聚合，不逐实例建
-- peer；fabric_generation 只升不降，旧 generation 重放由 store 与 agent
-- 节点级 fencing 双重拒绝。
CREATE TABLE IF NOT EXISTS wg_peers (
    node_id           text PRIMARY KEY,
    pubkey            text NOT NULL,
    endpoint          text NOT NULL,
    node_prefix       inet NOT NULL,        -- 节点 /64（peer 路由聚合）
    fabric_generation bigint NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- EastWestPolicy（ADR-0040 §15）：契约先行，行在 G2 数据面执行落地前不产生。
-- 只表达“谁能连谁的哪个服务端口”；与 ServiceSpec.mesh_direct 缺一不可。
CREATE TABLE IF NOT EXISTS eastwest_policies (
    src_project text NOT NULL,
    src_app     text NOT NULL,
    dst_project text NOT NULL,
    dst_app     text NOT NULL,
    dst_service text NOT NULL,
    ports       integer[] NOT NULL,
    generation  bigint NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (src_project, src_app, dst_project, dst_app, dst_service)
);
