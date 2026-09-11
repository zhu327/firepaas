-- v1.5 热路径索引补齐（review 2026-09-10）。
--
-- 均为 additive：旧二进制不使用这些索引也不受影响；新二进制避免全表扫描。
-- 注意：迁移在事务内执行，这里不用 CREATE INDEX CONCURRENTLY（会加短暂
-- 写锁；当前表规模可接受）。
--
--   user_events(at)   —— 保留期清理 DELETE ... WHERE at < cutoff
--   machines(node_id) —— ListMachinesOnNode（controller 节点对账）
--   volumes(node_id)  —— ListVolumesOnNode（volume 对账/失效标记）
--   snapshots(node_id)—— ListSnapshotsOnNode（snapshot 对账/失效标记）
--   apps(project_id)  —— ListAppsFiltered（租户列表）
CREATE INDEX IF NOT EXISTS user_events_at ON user_events (at);

CREATE INDEX IF NOT EXISTS machines_node_id ON machines (node_id) WHERE node_id <> '';

CREATE INDEX IF NOT EXISTS volumes_node_id ON volumes (node_id);

CREATE INDEX IF NOT EXISTS snapshots_node_id ON snapshots (node_id);

CREATE INDEX IF NOT EXISTS apps_project_id ON apps (project_id);
