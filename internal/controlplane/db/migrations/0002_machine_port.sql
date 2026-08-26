-- 0002_machine_port.sql：machines 增加 ingress_port（route catalog key 的一部分）。
ALTER TABLE machines ADD COLUMN IF NOT EXISTS ingress_port int NOT NULL DEFAULT 8080;
