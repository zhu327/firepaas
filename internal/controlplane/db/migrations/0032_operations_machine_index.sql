-- R2 加固（Task G-6）：operations 按 machine_id 的查询路径（LatestCreateAttempt /
-- FailedCreateAttempts / PendingOperationForMachine / R 路径对账）此前全表顺序
-- 扫；加普通（非 CONCURRENTLY，迁移器在事务内执行）索引。
CREATE INDEX IF NOT EXISTS operations_machine ON operations (machine_id);
