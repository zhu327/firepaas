-- 0041_machines_created_id_index.sql：P1 listMachines keyset 分页的排序/过滤支撑。
--
-- ListMachinesPaged 按 (created_at, id) 稳定升序 + keyset 过滤
-- `(created_at, id) > ($ts, $id)`；无索引时大租户每次翻页全表排序。
-- 只加索引（读优化，零行为变更）；在线执行，CREATE INDEX 取短暂
-- ShareLock（与 0038 热路径索引同策略；表大时由运维窗口执行）。
-- 回滚：DROP INDEX（forward-only，已发布 migration 不重写）。
CREATE INDEX IF NOT EXISTS machines_created_id ON machines (created_at, id);
