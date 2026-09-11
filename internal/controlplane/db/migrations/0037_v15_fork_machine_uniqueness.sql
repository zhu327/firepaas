-- 0037 v1.5 debug fork：machine 唯一键放宽到正式副本槽位。
--
-- fork（snapshot debug pin）机器固定使用 replica_ordinal=-1、deployment_id=''
-- （internal/controlplane/store/snapshots.go），而 0006 的唯一索引
-- (app_id, replica_ordinal, deployment_id) 会让同一 app 的第二次 fork 命中
-- 23505 并对外表现为 500。fork 的 machine_id 由 API 用 id.New() 生成、PK 已
-- 全局唯一；正式副本（replica_ordinal >= 0）的唯一性语义保持不变。
--
-- Additive migration：旧二进制看到更宽松的索引不会改变行为（它只在正式
-- 副本路径依赖该唯一性）；新二进制在旧库上会被本 migration 修正。
DROP INDEX IF EXISTS machines_app_ordinal_deployment;
CREATE UNIQUE INDEX IF NOT EXISTS machines_app_ordinal_deployment
    ON machines (app_id, replica_ordinal, deployment_id)
    WHERE replica_ordinal >= 0;
