# M5 单机执行计划：内部生产就绪（安全/稳定性/可观测/可靠性/升级）

日期：2026-08-27 ｜ 基线：M4 已提交（3f6abc6）｜ 运行基线不变（单机、Nomad system job agentd、slot 数据面）

## 目标（mvp-plan §9）

M5 = 内部生产就绪的最后一块。单机实验室形态下逐项收口，GA 出口六项内能在单机验证的
在下文切片中实现并挂 e2e；72h 浸泡以 runner 脚本交付（后台运行，不占排期）。
DEFERRED-MULTI-NODE 项继续累积到双机清单。

## 切片

### M5.1 安全硬化（API key + 最小 scope + 镜像准入 + host hardening）

- migration `0009_m5_api_keys.sql`：`api_keys(id, name, key_hash, scopes, project_id,
  created_at, expires_at, last_used_at, revoked_at)`；key_hash = SHA-256(key)（**绝不明文
  存储**）；key 生成 'fp_'+hex(crypto/rand 32B)，创建响应仅回显一次。
- 认证链：Bearer → 常量时间匹配 `FIREPAAS_API_TOKEN`（bootstrap root，兼容不变）；
  未命中再查 api_keys（未撤销且未过期）。**最小 scope**：`read`（GET 观测/列表）、
  `write`（机器/应用/secrets 变更）、`admin`（系统端点：重投影、节点 drain、key 管理）。
  范围：`scopes` jsonb + `project_id`（空 = 全部项目）。
- 镜像准入收口：`imagepolicy.Policy` 增 `RequireDigest`（`FIREPAAS_IMAGE_REQUIRE_DIGEST`
  未开启时仅告警?否——开启即拒绝 mutable tag）；**解包大小上限**
  `FIREPAAS_IMAGE_MAX_UNPACK_MIB`（默认 4096）在 agent `ensureImageReady` 就绪后校验
  `meta.SizeBytes`，超限 → 永久 InvalidArgument（不进重试）。
- host hardening：`scripts/lab/host-hardening-check.sh` **只读审计**（sysctl/sshd/suid/
  明文凭证扫描/firecracker 权限），逐项输出 PASS/FAIL/NA 与修复 runbook 链接；不写 `/etc`。
- 验收：错 key/撤销后/过 scope/跨 project 全部 401/403；审计黑名单覆盖新端点。

### M5.2 运行时稳定性（时钟/entropy/OOM/资源上限 → 阈值与告警）

- e2e 实测并记录：100 次 pause/resume 后 guest 时钟漂移（FC snapshot 不保 wall clock，
  预期倒退；记录数值与 chrony 补救 runbook）；200 次冷启动期间宿主 entropy、
  FD 数与 inode 数、conntrack 计数、TAP 数的变化曲线 → 写入 capacity model 阈值。
- host OOM：agentd cgroup 16GiB admission 已有（M3）；补 OOM 指标抓取（`/metrics` 暴露
  `firepaas_host_*`：mem available、FD、inode、conntrack；由 info provider 读取）。
- 告警：`iac/observability/prometheus-alerts.yml`（示例规则：PENDING 积压、节点
  UNKNOWN、投影滞后、FD 逼近上限）+ runbook 条目；lab compose 增 prometheus/grafana
  **optional profile**（默认不启动，文档说明）。

### M5.3 可观测（operation trace + 容量/调度/route 可视化）

- `GET /v1/operations?machine_id=&kind=&limit=` 与 `GET /v1/operations/{id}`（PG operations
  表已含 created/claimed/completed/attempts/error）；`fpctl ops ls/show`。压测复盘 =
  按 operation_id/machine_id 关联各阶段时间戳。
- controller sync 增 gauge 快照（machines 按 observed_state、pending ops、routes 数）；
  调度器复用 `scheduler_events` 事件流暴露。
- Grafana provisioning（datasource + 1 个 dashboard JSON：容量/调度/路由/operation 四版块）
  入库 iac/observability/，lab 可选启动。

### M5.4 可靠性（PG 备份恢复、Redis 重投影、对象存储恢复、节点替换 runbook）

- `scripts/lab/pg-backup.sh`（pg_dump → gzip → `backups/`，保留最近 7 份）与
  `pg-restore-rehearsal.sh`（scratch 库 → migrate → 行数断言）。e2e 演练真实跑一遍。
- **显式重投影**：`POST /v1/system/reprojections`（admin scope）：清 Redis route/location
  key → 从 PG 全量重建，返回耗时；e2e 注入 flushall 后调它，≤75s 内 edge 回 200。
  顺手修 M4 遗留：projection rebuild 由 30s ticker 触发改为**可被显式调用抢占**。
- `scripts/lab/minio-backup-rehearsal.sh`（mc mirror 对象存储备份演练）+ 恢复 runbook。
- `docs/runbook-node-replacement.md`：节点替换全流程（单机形态轮到双机执行）。

### M5.5 升级（drain/rebuild 承诺落地）

- `POST /v1/nodes/{id}/drain`（admin）→ nodes.status=DRAINING：调度不再放置新车
  （placement 过滤），已有流量继续；`POST /v1/nodes/{id}/ready` 复原。**首版承诺
  drain/rebuild，不承诺零中断**（mvp-plan §9.5 原话进 runbook）。
- `scripts/lab/upgrade-agentd.sh`：rebuild agentd → drain node → nomad job restart →
  ready → 对账收敛（无孤儿 VM/lease）；e2e 演练。

### M5.6 e2e-m5 收敛验收 + 72h 浸泡 runner

- `scripts/lab/e2e-m5.sh` 六段（后台运行、日志轮询）：
  A 安全负路径（错 key/撤销/scope/跨 project/image 准入）
  B 稳定性采样（entropy/FD/inode/conntrack 曲线 + 100 循环 pause/resume 时钟记录）
  C 可观测（/metrics 抓取 + operation trace 关联）
  D 可靠性（pg backup+restore 演练、flushall→重投影 ≤75s）
  E 升级（node drain → restart → ready → 对账）
  F 终态零泄漏
- `scripts/lab/soak-m5.sh`：72h runner（周期 create/publish/scale/pause/delete + 泄漏快照
  入 `results/soak-m5/`），期间崩溃即非零退出；本里程碑交付脚本并在真机跑 60 分钟
  排练（结果入 results/），72h 全程后台留给用户。

## 验收映射（mvp-plan §9 → 切片）

| §9 条目 | 切片 | 单机可达性 |
|---|---|---|
| 1 安全（key 哈希/scope/准入/hardening） | M5.1 | 全量可验证 |
| 2 运行时稳定性（时钟/entropy/OOM/上限） | M5.2 | 测量+阈值记录；告警模板 |
| 3 可观测（dashboard/告警/trace + 复盘） | M5.3 | 全量（lab 可选栈） |
| 4 可靠性（备份/重投影/对象存储/替换） | M5.4 | 全量演练；节点替换 runbook 双机执行 |
| 5 升级（drain/rebuild） | M5.5 | 全量可验证（承诺语义进 runbook） |
| 6 浸泡/DR（72h） | M5.6 | runner + 60min 排练；72h 后台 |

## DEFERRED-MULTI-NODE（累积）

- 双节点 drain 演练与 VIP 漂移（keepalived 模板已就位）、3-server quorum、
- 多 edge 凭证缓存一致性验证、跨节点快照兼容（快照 node-local）、节点替换 runbook 实战。

## 风险表（增量）

| 触发 | 决策 |
|---|---|
| FC 快照时钟漂移不可接受 | runbook 强制 guest 内 chrony/一次性唤后重启；指标记录漂移分布进 capacity model |
| 镜像解包上限误杀正常镜像 | 默认 4096MiB 且仅 agent 侧告警+API 提示；阈值可配 |
| Redis flushall 重投影耗时超 75s | 保留 serve-stale 兜底；优化 publish 批量（pipeline）已内置 |
| 多 scope 路由判定复杂化 | scope 三档封顶（read/write/admin），超出记 v1.1 |

## 执行记录

（2026-08-27 全部切片落地，详见各切片：）

- **M5.1 安全**：api_keys 哈希存储 + 三档 scope + 跨 project 防线；
  镜像准入 require-digest/allowlist/解包上限三层；host hardening 只读审计。
  实验室 DB 预置 0009（scopes text[]）→ 仓内 0009 对齐 + 0010 收敛，双轨一致。
  单位测试：apikeys（PG 集成 4 例）+ imagepolicy RequireDigest + adapter
  解包上限（ErrImageTooBig 永久错误路径）。
- **M5.2 稳定性**：宿主 gauge 采样 15s 入 /metrics；告警规则 YAML；
  实测 20 循环 pause/resume guest 时钟漂移 **-5ms**、entropy 256、
  conntrack 725→649（results/m5/e2e-m5-run.log）。FC snapshot 在实验节奏下
  未回拨 wall clock；长 pause 场景仍需 guest chrony（runbook-capacity）。
- **M5.3 可观测**：operation trace 端点（全树脱敏）与 fpctl ops；
  调度/机器/积压 gauge；Grafana provisioning + compose observability profile。
- **M5.4 可靠性**：PG 备份/恢复行数断言演练 PASS；显式重投影
  （flushall→≤15s 回源，e2e D 段）；minio 双拷贝清单一致演练 PASS；
  节点替换 runbook（双机实施）。
- **M5.5 升级**：drain→rebuild→ready 全链路 e2e E 段 PASS（upgrade-agentd.sh）。
- **M5.6 e2e-m5** FULL PASS（~4.5min；results/m5/e2e-m5-run.log）；
  soak runner 交付，60min 排练后台运行中，72h 正式运行见 runner 说明。

### 风险表实际命中

| 风险 | 实际 |
|---|---|
| FC 快照时钟漂移不可接受 | 未命中：20 循环漂移 -5ms（记录在案；长 pause 留 chrony 建议） |
| 镜像解包上限误杀 | 未命中：默认 4096MiB 只拒绝超限 |
| flushall 重建超 75s | 未命中：≤15s（5s route 同步周期） |
| 多 scope 判定复杂化 | 未命中：三档封顶 |

### 新增偏差（进 v1.1 / 上游）

1. hypeman `GetImage` 对 OCI index digest 的寻址问题（firepaas 已用
   ListImages 绕行；上游修复提交另开）。
2. `FROM scratch`/无发行版 init 的镜像不写 hypeman boot marker → 界面
   image 要求：必须有基础发行版 init（文档化于 runbook-capacity）。
3. 自动 idle 检测（per-VM usage 管道）v1.1（同 M4 记录）。
4. 多 edge/双节点项 DEFERRED-MULTI-NODE（不变）。
