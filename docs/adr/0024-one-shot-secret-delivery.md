# ADR-0024：Execution-bound one-shot secret delivery

状态：已接受（v1.2，2026-08-31 实现）
替代/收敛：ADR-0010 中 `unsafe-persisted-env` 降级路径。实现决策：
- 上游采用 release-gate 方案——hypeman `SecretDelivery` policy（init 在
  guest agent 启动前挂 tmpfs、entrypoint 等 marker 放行，tag
  `firepaas-v1.2.0-secretgate`）；secret 明文永不落宿主盘。
- 投递随 Create RPC 内联：agent 在 Create 内同步完成 vsock 投递后返回，
  DELIVERED 由响应推进，ACKED 由 observed `ProgramStartedAt` 推进；
  独立 DeliverSecret RPC 不引入。

## 背景

当前 hypeman 会把普通环境变量写入 `metadata.json`。因此 firepaas 的 `secret_env` 只能默认拒绝或以明确不安全开关启用，与“secret 不落节点持久状态”的目标冲突。

## 决策

1. PG 继续保存信封加密 secret；deployment 只保存 `{secret_id, version}` 引用。
2. PG 持久化不含明文的 lease metadata，状态机为
   `ISSUED → CLAIMED → DELIVERED → ACKED | EXPIRED | REVOKED`；状态转换使用 CAS/唯一约束。Redis 只能缓存，不能成为 lease 权威。
3. 每个 execution 创建随机、短 TTL、最多成功消费一次的 delivery lease，绑定 project、machine、execution、generation、operation 和 request hash；ACK 结果可幂等重放。
4. 控制面解析密文后经 mTLS 发给 agent；agent 只在内存持有，并通过 vsock guest channel 写入 `/run/firepaas/secrets` tmpfs。
5. guest ACK 后 agent清除明文，PG 将 lease 置 ACKED；回收器处理 EXPIRED/REVOKED。二次消费、过期 lease、旧 execution、身份不匹配全部拒绝。
6. guest 已写入但 ACK 丢失属于不确定结果：必须先 fence 并销毁旧 execution，再以新 execution 和新 lease 重试；禁止向同一 execution 二次签发。
7. secret 不得进入 MachineSpec 回显、operation request/result 持久化、Redis、日志、trace 或 hypeman metadata/config disk。
8. 环境变量兼容由 guest init 在启动目标进程时从 tmpfs 构造，不写回持久配置。
9. **接收过 secret 的 execution 禁止 memory snapshot/checkpoint/fork。** tmpfs、进程环境和应用内存都会进入 Firecracker memory snapshot，扫描 canary 不能证明没有副本。只允许 filesystem-only checkpoint，冷启动后重新授权 secret。
10. `unsafe-persisted-env` 保留一个版本用于实验兼容，默认关闭、启动告警，随后删除。

## 理由

只缩短 secret 在普通 env 中的停留时间不能满足不落盘要求；必须把 secret delivery 与 execution 身份及 guest 启动顺序绑定。

## 后果

- 需要 hypeman 上游提供通用 guest secret channel 并发布不可变 tag；
- 增加 lease 状态机、安全 canary 和 crash-point 测试；
- secret 更新仍通过新 deployment，不提供在线热更新；
- guest 内有权限的应用仍可主动读取自己的 secret，这是产品授权边界而非泄漏。
