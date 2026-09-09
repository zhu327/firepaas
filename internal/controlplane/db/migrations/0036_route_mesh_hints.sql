-- ADR-0040 §17（W3）：route_backends 持久化 mesh 直连提示（ULA/identity/
-- fabric generation），使 PG 成为“已发布 backend set（含提示）”的完整权威。
-- 此前提示只走内存 Projection 进 Redis，PG 读回重建与审计排障都看不到提示。
-- 纯加列（nullable→带默认，零回填）：旧二进制/旧查询不受影响；回滚 DROP COLUMN。
ALTER TABLE route_backends
    ADD COLUMN IF NOT EXISTS mesh_ula text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS mesh_identity_id integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS mesh_generation bigint NOT NULL DEFAULT 0;
