-- 0003_route_sync.sql：routes/route_backends 从死 schema 转为 controller 写入的
-- 已发布路由权威（ADR-0005；M1 评审 P2-6）。
-- M1 语义：每 hostname 一个活跃端口；id = "{hostname}:{port}"。
-- app_id 放开唯一约束（同一 app 未来可拥有多个 hostname 的 route）；
-- hostname 唯一约束升级为 (hostname, port) 唯一。

ALTER TABLE routes DROP CONSTRAINT IF EXISTS routes_app_id_key;
ALTER TABLE routes DROP CONSTRAINT IF EXISTS routes_hostname_key;
ALTER TABLE routes ADD COLUMN IF NOT EXISTS port int NOT NULL DEFAULT 8080;
ALTER TABLE routes DROP CONSTRAINT IF EXISTS routes_hostname_port_key;
ALTER TABLE routes ADD CONSTRAINT routes_hostname_port_key UNIQUE (hostname, port);
