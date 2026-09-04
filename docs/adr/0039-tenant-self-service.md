# ADR-0039：租户自助运营（项目 CRUD、自助 key、RBAC 角色、预热自助、CLI 补齐）

状态：已接受（v1.5，2026-09-04）
关联：ADR-0035、ADR-0037、ADR-0038；`docs/architecture.md`。

## 背景

v1.4 交付后三块租户运营断层：

1. 无 `POST /v1/projects` CRUD（仅 quota/rate-limit GET/PUT），无 team/member 模型；
   `POST /v1/apikeys` 仅全局身份，租户不能自助发 key/轮换；scope 只有
   `read<write=debug<admin`，`debug` 与 `write` 同 rank，做不到"可 exec 不可 deploy"。
2. `POST /v1/images/prewarm`、coverage、pin 全 admin（ADR-0037 临时治理），
   普通 key 预期 403；租户发版冷启动依赖运维手工预热。
3. quota/rate-limit、nodes/drain、events、wait/ttl、snapshot/fork/preflight/rescue、
   volume、egress-audit 全只有 REST；`fpctl images` 不发 `Idempotency-Key`，
   与服务端幂等设计矛盾。

## 决策

1. **项目 CRUD（最小可用，不引入 team/member 表）**。新增 `POST/GET /v1/projects`、
   `GET/DELETE /v1/projects/{id}`；创建/删除仅全局身份（admin），读为 `read`
  （受限身份仅见本项目，handler 层过滤）。删除仅当无 apps、未删除 volumes、
   snapshots、未过期 pins 时执行（`ErrProjectNotEmpty` → 409），拒绝删除默认
   `dev` 项目。配额/限流仍走既有 governance 端点。协作边界 = project 绑定的
   apikey + 下述 RBAC 角色；`team/member` 表延期（见后果）。
2. **scope 能力化拆分（向后兼容）**。授权从 rank 比较改为能力授予
   （`scopeGrants`）：`exec`/`debug` 授予 logs/exec/cp，`deploy` 授予 app
   创建/部署/扩缩/回滚/删除，历史 `write` 授予全部非 admin 能力，`admin` 授予
   一切。路由表：app 发布链 `write`→`deploy`，logs/exec/cp `debug`→`exec`，
   其余 `write` 路由不动——历史 `write`/`debug` key 行为不变，新 key 可按最小
   权限申请 `deploy`/`exec`。未知 scope 永不授予。
3. **RBAC 角色 = scope 束（不落库）**。`viewer=[read]`、`operator=[read,exec]`
  （可 exec 不可 deploy）、`deployer=[read,deploy]`、`maintainer=[read,deploy,exec]`、
   `owner=[admin]`；`POST /v1/apikeys` 支持 `role` 参数（与 `scopes` 二选一），
   `fpctl apikey create --role` 透出。落库仍是 scopes，无 migration。
4. **项目内自助 key（修订 ADR-0038 §1）**。中间件只做 `admin` scope 门槛；
   越权边界下沉到 handler：全局身份不受限；受限 project admin 仅可管理本项目
   key（`project_id` 留空归一到自身、跨项目 403），且申请 scopes 的能力 ⊆ 自身
   （`scopesSubset` 按能力语义，防提权；`admin` 覆盖一切）。`list` 按项目过滤，
   `revoke` 跨项目返回统一口径（防 id oracle）。新增
   `POST /v1/apikeys/{id}/rotate`（同名同 scopes 同项目发新 key 后撤销旧 key；
   撤销失败返回新 key + 明确告警，不回滚已签发的明文）。
5. **租户自助预热（修订 ADR-0037 §1，配额与脱敏代替一刀切）**。`prewarm`/`pins`
   写降为 `write`，`coverage`/`pins` 读降为 `read`；约束：受限身份禁止
   `node_ids`（只许 `node_pool`，拓扑由运维管理），`coverage` 对受限身份只返回
   `summary` + 按 pool 聚合（不暴露 `node_id`/单节点观测），全局身份保留全量
   per-node 视图。配额（active prewarm、pin count/bytes）、幂等、watermark、
   GC 锁语义沿用 ADR-0037 不变。
6. **CLI 全量补齐 + 幂等头**。新增 `fpctl project`（含 quota/ratelimits get/set）、
   `nodes`（ls/drain/ready/capabilities）、`events`（ls/scheduler）、`machines`、
   `wait`、`ttl`、`snapshot`（含 schedule/fork/preflight/rescue）、`volume`、
   `app egress-audit`、`apikey rotate`/`--role`/`ls --project`；mutation 子命令
   接受 `--idempotency-key`（为空自动生成并打印到 stderr，超时重试复用同一 key
   即安全重放；亦可用 `FP_IDEMPOTENCY_KEY` 预置）。positional-first、flags-after
   的参数顺序与既有 `app deploy` 模式一致（Go flag 遇 positional 即停）。

## 理由

- 项目删除"非空拒绝"而非级联：级联删除是破坏性操作，与"配额降低不驱逐"
  （ADR-0035）同理——显式清理顺序优于隐式销毁。
- 能力子集而非字符串子集：否则 `admin` 身份反而签发不出 `deploy` key
  （实现中已踩中并修正，见 `scopesSubset`）。
- coverage 脱敏保留 pool 聚合：租户按 `node_pool` 自助预热必须知道 pool 粒度
  进度；`node_id` 与单节点观测时间是拓扑信息，不给。
- 无 migration：全部变更是 API/CLI/授权语义，`projects`/`api_keys` 表结构不动；
  回滚只需回滚二进制（新路由 404、新 scope 在旧二进制上视为非法 scope 创建失败）。

## 后果

- ADR-0038 §1"受限 admin 一律 403"被本 ADR §4 取代（全局 key 管理仍需全局身份）。
- ADR-0037 §1"prewarm 系全 admin"被本 ADR §5 取代（其余 8 条不变）。
- `team/member` 表、node-pool ACL 数据模型仍延期：当前"受限禁 node_ids +
  coverage 脱敏"是过渡治理；引入 pool 授权表时另立 ADR。
- `rotate` 非幂等（每次调用签发新 key）：重试轮换前先 `ls` 确认，避免 key 增殖。
- e2e 需补：自助 key 越权矩阵、operator/deployer 互斥、coverage 脱敏、
  quota set revision 冲突 409。

## 回滚

停止新项目创建/自助 key/prewarm，等待在途 prewarm 终态；回滚二进制后旧行为
恢复（受限自助 403、自助预热 403）。PG 中 projects/keys/pins/operations 行保留；
旧二进制忽略新增路由与未知 scope。
