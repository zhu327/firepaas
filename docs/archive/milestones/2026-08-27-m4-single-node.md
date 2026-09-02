# M4 单机执行计划：安全入口、proxy hardening 与条件弹性

日期：2026-08-26 ｜ 基线：M3 已提交（75f60c1）｜ 运行基线不变（单机、Nomad system job agentd、slot 数据面）

## 目标（mvp-plan §8）

在 M3 正式 slot 数据面上补齐：secrets v1（ADR-0010）、proxy hardening（ADR-0006 收口）、
edge TLS/限流/serve-stale（ADR-0011 实验室形态）、Redis 可用性验收，以及基于 M0 snapshot
spike（restore p95 95ms ✅）的 scale-to-zero。

## 切片

### M4.1 secrets v1（ADR-0010）

- migration 0008：`secrets(id, project_id, name, version, value_ciphertext, dek_wrapped,
  nonce, key_version, created_at)`，unique(project_id, name, version)；值 = 信封加密
  （主密钥 `FIREPAAS_SECRETS_MASTER_KEY` base64 32B 包裹随机 DEK，AES-256-GCM），
  PG 绝无明文。
- store：PutSecret（新版本追加）、GetSecretValue（解密）、ListSecrets（仅元数据）、
  ResolveSecretRefs（app 的 {var:{name,version}} → 明文 env，仅内存）。
- API：`POST/GET/DELETE /v1/secrets`；app 维度 `PUT /v1/apps/{id}/secret-refs`；
  **修改 refs 触发新 deployment（走现有 rollout 状态机）**。
- 下发链路：EnsureCreateExecution 时解析 refs → `CreateMachineRequest.secret_env`
  （字段已存在）；agent 侧注入/脱敏/ledger 不落盘已有（M1），零改动验证。
- 审计只记 secret_id/version；api+agent 增加**敏感字段黑名单中间件**
  （secret_env/value/ciphertext/proxy_credential/authorization），测试断言。
- CLI：`fpctl secrets set/rm/ls`、`fpctl apps env set --secret K=NAME[@v]`。

### M4.2 agent proxy hardening（ADR-0006 收口）

- 控制面为每次 create execution 生成 128-bit `proxy_credential`：
  - 经 `CreateMachineRequest.proxy_credential` 单向下发进 agent（保存 SHA-256 摘要 +
    execution 绑定，不回显不持久化——ledger 已天然不含）；
  - 同值经 **专用认证端点** `GET /v1/machines/{id}/traffic-token`（bearer+审计黑名单）
    交给 edge 内存缓存；**绝不进 Redis/Machine/ListMachines/operation result**。
- agent proxy 校验链：mTLS CN=edge-proxy（已有）→ `X-Firepaas-Credential` 摘要匹配该
  machine 当前 execution → 仅转发 machine 声明端口（ingress_port）。
- 撤销语义：delete 成功或换代 create 落地瞬间丢弃验证材料 → stale 流量立即 fail-closed
  （补齐 M3 遗留的"不向 stale execution 转发"硬校验）。
- 兼容开关 `FIREPAAS_PROXY_CREDENTIAL_REQUIRED`（默认 true）。

### M4.3 edge TLS、限流与本地缓存（ADR-0011 实验室形态）

- gen-certs.sh 增签 `*.firepaas.local` 服务端证书（复用实验室内部 CA）；edge 新增 :443
  TLS 终止，:80 → 308 https；客户端信任 = 预置 ca.crt（runbook 文档化，curl 用法入 e2e）。
- step-ca ACME 集成记录为生产 runbook 前置（实验室静态 CA 足够；偏差写入计划执行记录）；
  keepalived VIP 为节点层配置模板（iac/keepalived/），单机保持 DNS 形态（ADR-0011 分层）。
- edge 本地 route 缓存：fresh TTL 5s；Redis 失联时在 stale 窗口内（默认 120s，可配）继续
  serve last-known-good，超窗 503；counters：stale serves / redis errors。
- 每 hostname 令牌桶限流（默认 100rps/burst200）→ 429。

### M4.4 Redis 可用性验收（chaos-m4.sh）

- 注入：停止 dev-redis → 断言声明窗口内数据面继续服务（stale 计数增长）；恢复 → 投影在
  RebuildInterval 内重建（≤60s）；期间发布操作受控失败不悬挂。
- 凭证撤销负路径：delete 后旧 credential 必须被 agent proxy 拒绝（403）。
- 结论写入 mvp-plan §8：sentinel 是否引入（预期否——stale 窗口足够）。

### M4.5 scale-to-zero（依赖 M0 snapshot spike ✅）

- proto v1 增量：PauseMachine/ResumeMachine RPC（append 字段编号，契约冻结破例记录于
  执行计划，M5 升级策略仍 drain/rebuild）；observed 状态增 PAUSED。
- agent adapter 包装 hypeman StandbyInstance/RestoreInstance；恢复错误 → 上报失败由 R3
  cold-start 重建（天然降级，符合风险表）。
- controller idle 检测（usage CPU 阈值 × 时长，app 级开关）→ 入队 pause op；
  **autoresume 在 agent proxy 同步完成**（首个请求触发 restore，<5s SLO 来自 M0 基准，
  超时 503+Retry-After）。
- 验收：50 次 pause/resume 无 netns/TAP/slot 泄漏；autoresume 边界记录。

## 验收映射（mvp-plan §8）

- credential 不出现在 Machine/ListMachines/Redis/日志/operation result；错误身份或
  credential 不能过 agent proxy：e2e-m4 步骤 C/D + 黑名单单测 ✅
- 两节点多副本同 hostname 稳定服务：DEFERRED-MULTI-NODE（单机降级放置已有事件审计）
- Redis 宕机注入 stale 窗口 + 恢复重建时限：e2e-m4 步骤 F（含 FLUSHALL 真测投影
  重建；chaos 场景折叠进 e2e-m4，未另建 chaos-m4.sh）✅
- scale-to-zero 50 次 pause/resume 无泄漏 + autoresume SLO：e2e-m4 步骤 H ✅

## 执行记录（2026-08-27，含评审修复）

全部切片交付并重跑验收（详细见 mvp-plan §8 单机执行记录）。评审修复：

- **P0**：agent/machine 测试替身补 StandbyInstance/RestoreInstance（接口新增
  后编译失败）；补 server 层 Pause/Resume 单测（幂等重放/fencing/execution 绑定）。
- **P1-2/P1-3/P2-5**：edge TokenClient 重写——缓存键 (machine, execution)、
  agent 403 → Invalidate+重取一次（ModifyResponse 哨兵拦截，响应不半写）、
  API 不可达时 token serve-stale（execution 匹配才复用，同源 120s 窗口）、
  回源锁外执行 + 同 key single-flight。
- **P1-4**：e2e-m1/m2/m3/chaos-m2 补 FIREPAAS_TRAFFIC_TOKEN_KEY 与 edge 的
  API_ADDR/TOKEN（agent 默认强制校验后旧脚本会全量 create 失败）。
- **P2-7/P2-8**：edge counters + FIREPAAS_EDGE_METRICS_PORT /metrics；路由缓存
  区分权威 miss（立即 404 + 5s 负缓存）与回源失败（才 serve-stale），已删除
  路由不再被 stale 复活。
- **P2-9/P3-18**：resolveInstanceID 实例不存在改 ErrMachineNotFound（不再
  误判镜像不可拉取的终态失败）；Pause/Resume 补 execution 绑定校验；
  processLifecycle 死变量清理、observed 按实际状态写入。
- **P2-10**：e2e-m4 F 步：Redis 恢复后 FLUSHALL（AOF 持久化使旧实现未真测
  重建）→ 404 → ≤75s 重建断言；宕机窗口 deploy 受理不悬挂断言；
  credentials.json 路径修正（data_dir/agent 下，原路径恒不存在）+ 0600 断言。
- **P3**：--secret VAR= 移除绑定语义打通（服务端过滤 null 条目）；
  fpctl secrets set 支持 stdin；并发 PUT secret 版本冲突 409+Retry-After；
  308 重定向保留非标 TLS 端口；redact 接入 auditMiddleware（深度防御）；
  RouteCache.Invalidate 在 403 重试路径接线。
- **P2-6**：iac/keepalived/ 配置模板 + README（双节点漂移演练
  DEFERRED-MULTI-NODE；DNS 轮询降级路径已文档化）。
- **P2-11**：mvp-plan §8 执行记录 + sentinel 结论（不引入）+ 偏差清单；
  idle 检测明确列入 v1.1（风险表已加条目）。

### 真机验收追加修复（评审时未发现、e2e 重跑暴露）

- **R-1（P0 级）processLifecycle agent opID 撞固定后缀**：
  `op-pause-{machine}-{exec8}` 固定后缀使同 execution 的后续 pause/resume
  全部命中 agent ledger 重放——VM 从未真正 standby/resume，sync 循环又把
  observed 回写 RUNNING，50 循环在第 N 次撞输竞态。改用控制面 op.ID
  （每次 API 调用唯一）；同 op 重试仍命中重放（幂等语义不变）。
- **R-2（P0 级）autoresume 传 machineID 而非内部 ID**：hypeman
  RestoreInstance 按内部 ID（目录名）加载，传 firepaas 名字直接
  ErrNotFound → 502。改用 GetInstance 已解析的 inst.Id；测试替身同步
  收紧为仅接受内部 ID（原替身两者都收，把 bug 藏进单测盲区）。
- **R-3（P0 级）restore 后 slot 未重挂**：hypeman standby 释放网络、
  restore 在 root ns 重建 TAP，但 slot 后端要求 TAP 在 slot netns 内——
  resume/autoresume 后 VM Running 但流量 502。新增 reattachSlot
  （Attach 幂等，ensureKernel 补移 root ns TAP）。
- **R-4（P1 级）ensureKernel 撞 netns 残留 TAP**：standby 时 root 删除
  沉默跳过（TAP 已移入 netns），restore 后 netns 内旧 TAP 残留 + root 新
  TAP → move 报 File exists。移入前先清理 netns 内同名残留。
- **R-5（P1 级）fresh 命中误报 stale**：RouteCache.Get 的第二返回值把
  "来自缓存"与"last-known-good 降级"混为一谈，fresh 命中（正常缓存）
  也打 X-Firepaas-Stale 头，e2e 的"重建后回源"断言永远不满足。语义
  收敛为仅降级路径 servedStale=true。
- e2e-m3 7.6 断言适配 fresh TTL：catalog 删除后允许快速重建路径（fresh
  窗口内投影已重建 → 无 miss 窗口）；slot 泄漏测试前环境残留 VM 会被
  误报泄漏（cleanupStaleTestNetns 跳过带 TAP 的 live slot）——先清理再跑。

重跑验收（全部 PASS）：e2e-m4（secrets/credential 正负路径与撤销/TLS/限流
/serve-stale/FLUSHALL 重建 6s/50 次 pause-resume/autoresume 159ms/零泄漏）、
e2e-m3（U1-U3/隔离/发布回滚/1000 次 slot 无泄漏/7.5-7.7 回归/零泄漏）、
e2e-m2（1000 并发幂等/20 轮无泄漏）、chaos-m2（ACK 丢失 33s/agent crash
16s/Redis 清空/API crash 与在途 crash 均 <120s 收敛）；单测/PG-gated/sim
10 万次全绿。

## 风险与降级

- snapshot 稳定性不足 → scale-to-zero 移至 v1.1，保留 cold-start（§11 已预留）。
- edge 内存凭证缓存使多 edge 实例需要各自拉取 —— 多 edge 验证 DEFERRED-MULTI-NODE。
- 主密钥管理仅环境变量注入，轮换/KMS 是 M5 工作项（ADR-0010 已切分）。
