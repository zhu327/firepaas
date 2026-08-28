-- 0014_evacuation_steps.sql：ADR-0021 驱离互斥与可恢复步骤状态。
-- 只允许一个 active evacuate 节点；当前 machine/开始时间持久化，以便
-- leader 切换后继续等待同一 replacement，而不推进下一台。
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS evacuation_machine_id text;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS evacuation_started_at timestamptz;

CREATE UNIQUE INDEX IF NOT EXISTS nodes_one_active_evacuation
    ON nodes ((evacuate)) WHERE draining AND evacuate;
