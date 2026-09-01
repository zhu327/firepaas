-- 0027_v14_image_prewarm.sql：v1.4-C（docs/v1.4-plan.md §7）显式镜像预热、
-- 覆盖率观测与有界 cache pin。
--
-- 1. image_prewarm_targets：持久 prewarm operation 的逐节点结果
--    （coverage 查询的观测来源；重复 prewarm 只补未完成节点）。
-- 2. image_pins：短期 cache pin。主键作用域 = project_id + image_digest +
--    target selector；GC 按节点计算 roots，单个 pin 不得隐式保护全集群。
-- 3. image_sizes：digest 观测大小（pin bytes 记账的权威来源；无观测大小
--    时拒绝新建 pin——先 prewarm 再 pin，fail closed）。
--
-- 回滚兼容：全部为新增表；回滚二进制保留数据即可（列均可空/有默认值）。

CREATE TABLE IF NOT EXISTS image_prewarm_targets (
    operation_id text NOT NULL,
    node_id      text NOT NULL,
    status       text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','SUCCEEDED','FAILED')),
    error        text NOT NULL DEFAULT '',
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (operation_id, node_id)
);

CREATE INDEX IF NOT EXISTS image_prewarm_targets_node
    ON image_prewarm_targets (node_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS image_pins (
    id           text PRIMARY KEY,
    project_id   text NOT NULL,
    image_digest text NOT NULL CHECK (image_digest LIKE 'sha256:%'),
    selector     text NOT NULL CHECK (selector LIKE 'node_pool:%' OR selector LIKE 'node:%'),
    owner        text NOT NULL,
    reason       text NOT NULL DEFAULT '',
    expires_at   timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project_id, image_digest, selector)
);

CREATE INDEX IF NOT EXISTS image_pins_expiry ON image_pins (expires_at);
CREATE INDEX IF NOT EXISTS image_pins_project ON image_pins (project_id, expires_at);

CREATE TABLE IF NOT EXISTS image_sizes (
    digest      text PRIMARY KEY CHECK (digest LIKE 'sha256:%'),
    size_mib    bigint NOT NULL CHECK (size_mib > 0),
    observed_at timestamptz NOT NULL DEFAULT now()
);
