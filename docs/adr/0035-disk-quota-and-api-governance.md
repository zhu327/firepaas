# ADR-0035：磁盘资源治理、项目配额与 API 限流

状态：已接受（v1.2-E，2026-08-31）
关联：ADR-0002（Best-of-K 硬过滤）、ADR-0023（能力协商）、v1.2-plan §8。

## 背景

v1.2-E 之前，资源治理只有 CPU（R=4 超售）与内存（不超售）两个维度：磁盘
不参与调度、预约与硬准入；项目配额只有 vcpu/mem 两列且无管理 API；控制面
API 无限流，单个 project 可以耗尽控制面容量或 runtime-stream 并发。

## 决策

### 1. 磁盘维度（requested 驱动调度，水位做最终准入）

- `disk_mib`（requested）贯穿 scheduler Request → Redis reservation →
  agent admission；超售比 R=1.0（与内存一致，磁盘不能靠压缩回收）。
- scheduler `canFit` 在打分前硬过滤节点 `disk_allocated + disk_pending +
  disk_requested ≤ disk_total`；`NodeUsage` 增加 `disk_allocated_mib`。
- agent admission 是最终防线：节点磁盘水位（`disk_used/data_total`）超过
  `FIREPAAS_ADMISSION_DISK_WATERMARK`（默认 0.9）时拒绝 create（
  `ResourceExhausted`，控制面换节点重试），无论控制面投影怎么说。
- 磁盘 accounting 偏差的兜底顺序：requested（调度）→ reserved（Lua）→
  free-watermark（agent 真实文件系统）。三层任一拒绝都不得被评分覆盖。

### 2. 项目配额（revision 并发更新，降低不驱逐）

- `projects` 增加 `disk_mib_quota`、`machine_concurrency`、
  `runtime_session_concurrency`；沿用 `vcpu_quota`/`mem_mib_quota`。
- 配额更新走 admin API（`PUT /v1/projects/{id}/quota`，admin scope），
  携带 `revision`（乐观锁，冲突 409）；GET 返回 `ETag`。
- **配额降低不驱逐已有 machine/会话**，只拒绝新的 create/restart/
  exec/cp；项目用量在声明窗口内收敛。
- machine 并发在 create 入队时检查（PG 权威 + Redis Lua 双层，与 vcpu/
  mem 同模式）；runtime-session 并发在 API 层（debug scope 端点）按
  project 计数检查，超限 429。

### 3. API 限流（project × route_class，Redis 原子 token bucket）

- route_class 三类：`read`（GET 非流式）、`mutation`（POST/PUT/DELETE）、
  `runtime-stream`（logs/exec/files 与 wait 长轮询）。
- 配置存 PG（`project_rate_limits` 表，默认 fallback 行），API 进程内存
  缓存 10s；桶算法在 Redis（单脚本原子取令牌），未取到令牌返回 429 +
  `Retry-After`。
- admin 与 /metrics 端点不受 project 限流（它们有自己的认证边界）。
- **Redis 故障分级**：read 类 fail-open（记 `firepaas_api_ratelimit_
  degraded_total`）；mutation 与 runtime-stream 类 fail-closed（503，明
  确 `rate_limiter_unavailable`）——高成本或敏感操作不得在限流失效时
  绕过配额保护。

### 4. 网络带宽与 IOPS

本版只进 `NodeCapacity` 上报与指标，不进入调度硬过滤或项目配额；数据
充分后再评估（与 v1.2-plan §8.6 一致）。

## 理由

- 磁盘 requested 与水位双层：控制面投影会滞后/泄漏（崩溃恢复），agent
  真实水位是唯一可信的最终防线；与 ADR-0002"agent 最终准入"一致。
- 配额"降低不驱逐"：驱逐正在服务的 workload 比短暂超配额伤害更大；
  收敛靠自然终态（TTL/delete/rollout）。
- 限流桶放 Redis：控制面多实例（leader+standby）共享计数；Redis 轻故障
  不能放大为控制面只读——read fail-open 是可用性取舍，mutation
  fail-closed 是配额完整性取舍，两者风险分级显式声明。

## 后果

- reservation Lua 键布局扩展（v3 脚本，向后兼容旧键）；node/projection
  同步带 disk_allocated；`make sim` 增加磁盘维度。
- API 中间件链新增 rate-limit 层（auth 之后、handler 之前），需要
  project 归属已解析（read 类无 project 查询参数时按 caller 的 project
  集合取第一优先级——apikey 场景为 key 的 project；root 为 "__root__"）。
- e2e-v12 增加 E 段：配额满拒绝、限流 429 与 fail 分级行为。
