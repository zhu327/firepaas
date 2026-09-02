-- R2 加固（Task J / D-2）：route 发布的单调 revision。publisher 在与
-- SyncRoutes 相同的 PG 事务内对每个 hostname 做 insert-on-conflict-increment
-- 并带回 RETURNING；Redis catalog 的 ReplaceHostRoutes Lua 以该 revision 为高
-- 水位拒绝乱序/重放的旧快照（miss 视为 0 直接生效）。hostname 级粒度（多端口
-- route 共享同一 revision），PG 是唯一权威——leader 换届时新进程从本表继续
-- 分配，绝不回退。
CREATE TABLE IF NOT EXISTS route_publication_revisions (
    hostname text PRIMARY KEY,
    revision bigint NOT NULL
);
