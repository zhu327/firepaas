# 并发自动弹性（ADR-0041）实施计划

日期：2026-09-11 ｜ ADR：`docs/adr/0041-concurrency-autoscaling.md`（v2，待接受）
模式：High-risk（公开 API、migration、并发写者、Redis 契约）→ 设计先行 + 回滚方案 + 末尾一次独立 `code-reviewer`。

## Goal

按 ADR-0041 落地 KPA 式基于并发的 App 自动弹性：策略 PG 权威、edge 推信号、Redis 聚合、leader 决策只写 `desired_replicas`，默认关闭零回归。

## Assumptions

- 落地时下一个空闲 migration 号为 `0039`（工作树已占用到 `0038`，已核实无 `0039`）。
- `apps.hostname` 单列 UNIQUE（App 模型未放开多 hostname；决策函数按多 hostname 求和预留形状）。
- per-edge hard 默认 256 不变；`target_concurrency` 默认 20。
- controller 侧 Redis 读经 `catalog` 包边界（edge 不连 PG、controller 不直连裸 redis client 除外：controller 已有 `cat *catalog.Catalog`，加方法）。
- Store/API 单测需 `FIREPAAS_TEST_POSTGRES`，catalog/edge 上报单测需 Redis；缺失时跳过并如实报告。

## Architecture

- ① `internal/controlplane/db/migrations/0039_autoscale.sql` + `store`（`App` 字段、`appColumns/scanApp`、策略 CRUD、CAS 条件 UPDATE、接管原子 UPDATE、纯校验函数）。
- ② `cmd/api`（`PUT/GET /v1/apps/{id}/autoscale`、`scale` 接管、`authm5.go` 两表登记、`user_events`）。
- ③ `internal/edge/autoscale.go`（per-hostname 有界记账 + 5s reporter + Lua 原子 HSET/EXPIRE）+ `handler.go` 记账点 + `/metrics` + `cmd/edge-proxy/main.go` 装配。
- ④ `internal/controlplane/controller/autoscale.go`（纯决策函数 + 有界内存状态 + 10s ticker）+ `catalog.GetAutoscaleFields` + 配额/资源冻结钩子（`processCreate` 内）+ 预取独立去重。
- ⑤ 文档（architecture §5 一句、两包 README、README 计数 40→41、runbook 归因树）+ `scripts/lab/e2e-autoscale.sh`。
- 回滚：逐 app `enabled=false` → 停 ticker（二进制回滚，键 20s 过期）→ 后续 migration 删列。每步可独立回滚（ADR §后果顺序①→⑤）。

## Validation

`make build` / `make vet` / `go test ./internal/edge/... ./internal/controlplane/controller/... ./cmd/api/... ./internal/controlplane/catalog/...` / `bash -n scripts/lab/e2e-autoscale.sh`；`make check` 全量；有 PG/Redis 时 `make check-lab`。`make proto` 不需要（无 protobuf 变更）。

| Task | Type | Blocked by | 可并行 |
|---|---|---|---|
| 1. migration+store | HITL? 否，AFK | — | — |
| 2. API+鉴权 | AFK | 1（store 签名） | 与 3 可并行（写集不交） |
| 3. edge 记账+reporter | AFK | — | 与 2 可并行 |
| 4. controller 决策循环 | AFK | 1、3（信号 JSON 契约） | — |
| 5. 文档+e2e+metrics 事件收尾 | AFK | 2、3、4 | — |
| 6. 集成验证 + 独立 code-reviewer | AFK | 1–5 | — |

并行说明：写集虽不交，但 4 依赖 1+3 的精确声明；为省上下文传输成本，本次顺序执行，不派 subagent（skill 允许）。

### Task 1：migration + store

Goal：策略列与三类写入（策略 CRUD、CAS、接管）+ 纯校验。
Acceptance criteria：
- `0039_autoscale.sql` 只加列+默认值（`enabled=false,min=1,max=10,target=20,delay=120,panic=2.0`），在线可执行。
- `App` 新增 6 字段；`appColumns/scanApp` 同步；既有 INSERT 不指定新列（走 DB 默认）。
- `ValidateAutoscalePolicy` 纯函数：`0<=min<=max<=100`、`max>=1`、`1<=target<=256`、`30<=delay<=600`、`1.5<=panic<=5.0`；store 写入侧同样 clamp。
- `SetAutoscalePolicy`（全量替换）、`SetAppReplicasCAS(id,want,expect) (bool,error)`（`AND autoscale_enabled AND desired_replicas=expect`，`RowsAffected=0`→false,nil）、`TakeoverScale(id,replicas)`（单条 UPDATE 写 desired+关 enabled）。
Files：Create `internal/controlplane/db/migrations/0039_autoscale.sql`；Modify `internal/controlplane/store/apps.go`；Create `internal/controlplane/store/autoscale.go`（策略类型+校验+CAS/接管方法）；Create `internal/controlplane/store/autoscale_test.go`（纯校验单测，无需 PG）。
Contracts：`type AutoscalePolicy{Enabled,MinReplicas,MaxReplicas,TargetConcurrency,ScaleDownDelaySec,PanicThreshold}`；`ValidateAutoscalePolicy(Policy) error`；`SetAutoscalePolicy(ctx,id,Policy)`；`SetAppReplicasCAS(ctx,id,want,expect)(bool,error)`；`TakeoverScale(ctx,id,replicas) error`。
Tests：校验边界全覆盖（min>max、max=0、target 越界、delay/panic 越界）；CAS/接管语义跑 PG（有环境时）。
Validation：`go test ./internal/controlplane/store/...` + `go test ./internal/controlplane/db/...`（migration 号唯一）。

### Task 2：API + 鉴权

Goal：`PUT/GET autoscale` 与手动接管。
Acceptance criteria：
- `PUT /v1/apps/{id}/autoscale`（deploy scope，全量替换，不改 desired）400 覆盖全部越界；`GET`（read scope）回策略；404 未找到。
- `POST /scale` 在启用中隐含接管：单条 UPDATE（desired+`enabled=false`）+ `user_events(autoscale.takeover)`；策略 PUT/开关写 `user_events(autoscale.policy)`。
- `authm5.go` 的 `routeScope` 与 `projectGated` 两表同时登记新路由；未登记默认拒绝。
Files：Modify `cmd/api/apps.go`（或新建 `cmd/api/autoscale.go`，优先新建减少冲突）、`cmd/api/authm5.go`、`cmd/api/main.go`（路由注册）。
Tests：handler 校验单测（400 矩阵）；接管原子性（store CAS 交错单测，有 PG 时）。
Validation：`go test ./cmd/api/...`。

### Task 3：edge 记账 + reporter

Goal：per-hostname 四计数 + 5s 上报 `autoscale:{host}`。
Acceptance criteria：
- 记账点：served（`proxiedReqs.Add` 同判定点 +1/出口 -1，retry 不重复）、served_rps、hard_rejected（`errHardLimit` 分支）、unserved（零容量：`ErrNotFound`/空 Backends/`errNoEligible`；排除 pin-miss/hard/429/`ErrBeyondStale`/catalog/token 错误）。
- 有界表（复用 `lru.go`，独立上限 env 可配）；空闲零值不按时间淘汰，仅 LRU 压力逐出。
- reporter 5s：`HSET autoscale:{host} {edgeID} {ewma,rps,hard_rejected,unserved,ts_ms,win_ms}` + `EXPIRE 20s` 经 Lua 原子；短窗计数上报约 2×poll 滚动和；失败只记 `redisErrors`。
- edgeID：`FIREPAAS_EDGE_ID` → `FIREPAAS_MESH_EDGE_ID` → hostname 回退。
- `/metrics` 新增 `firepaas_edge_autoscale_reports_total`、`_report_errors_total`、`firepaas_edge_unserved_requests_total`（均不带 hostname label）。
Files：Create `internal/edge/autoscale.go`；Modify `internal/edge/handler.go`（Config 加 Tracker、记账点）、`cmd/edge-proxy/main.go`（装配 reporter）；Create `internal/edge/autoscale_test.go`（EWMA 数学、unserved 分类、滚动和、有界）。
Validation：`go test ./internal/edge/...`。

### Task 4：controller 决策循环

Goal：leader 内 10s ticker + 纯决策函数，只经 CAS 写 desired。
Acceptance criteria（与 ADR 伪代码同序）：
- `ListApps` 失败→全量跳过；未启用/已删/无 target/有活跃 rollout（含查询失败）→跳过。
- 聚合只用 fresh（`-5s<=now-ts<=20s`）；无 fresh→hold；过期非零→禁缩容；零容量≠信号丢失（不读 route 投影，容量来自 PG machines）。
- `eff=ready+warming`（目标代；ready=serving 且节点非 draining；warming=非 serving、desired≠DELETED、创建证据在 `AgentRPCTimeout` 内或有在途 create）；`load=served+unserved`；`want=ceil(load/target)` clamp[min,max]；panic 双路只放大。
- 扩立即（CAS+独立有界预取去重+清缩时器）；缩要求稳定窗全程成立、`want==0` 需 `rps==0`、直接缩到 want；`want>=desired`/hold/策略变/接管/rollout 出現结束清零。
- 配额/资源冻结只冻扩不冻缩，默认 100s 超时自解冻，leader 内存态，事件 `autoscale.decision{freeze/unfreeze}`；状态变化落 `user_events`（from/to/reason/sig），hold 只指标 `firepaas_autoscale_decisions_total{result}`（result∈scale_up|scale_down|hold_signal|hold_rollout|hold_quota|conflict|error）。
- `catalog.GetAutoscaleFields(ctx,hostname)` 供 controller 读（HGETALL）。
- `processCreate` 配额失败→`noteQuotaRejection`；placement 失败且该 op 有 `filter_rejection:resources:*`→连续 3 次冻结；成功→清 streak。
Files：Create `internal/controlplane/controller/autoscale.go` + `autoscale_test.go`（验收矩阵决策侧：panic 双路、稳定窗、信号丢/过期非零、零容量冷起、发布互斥、配额冻结/超时、warming 超龄、clamp、CAS 冲突）；Modify `controller.go`（Config+New+ticker+hooks）、`internal/controlplane/catalog/catalog.go`。
Validation：`go test ./internal/controlplane/controller/... ./internal/controlplane/catalog/...`。

### Task 5：文档 + e2e + 收尾

Goal：architecture §5 一句、两包 README、README 计数、runbook 归因树、`e2e-autoscale.sh`（阶跃/归零冷起/信号丢失/发布冻结/配额冻结/手动接管，lab 门控）。
Acceptance criteria：`bash -n` 通过；脚本头含 `LAYER/PREREQ/DESTRUCTIVE/FROZEN`；事件类型常量 `autoscale.policy/takeover/decision` 入 store。
Files：Modify `docs/architecture.md`、`internal/edge/README.md`、`internal/controlplane/README.md`、`README.md`、`docs/runbook-operations.md`；Create `scripts/lab/e2e-autoscale.sh`。
Validation：`bash -n scripts/lab/*.sh` 相关项。

### Task 6：集成验证 + 独立审查

Goal：`make build/vet/check`（或风险适当子集）+ 缺失工具如实报告 + 一次 `code-reviewer`。
Acceptance criteria：高风险 changeset 完整独立审查恰一次；实质重塑才重审。

## 验收结果（2026-09-11）

环境：铲掉旧 lab 状态（Nomad job/agentd/hypeman/VM/netns/guest 数据）后重建；DB 重建（migration 0001-0039 全量重放）。

- `bash scripts/lab/e2e-autoscale.sh` PASS（阶跃扩、信号丢失不缩、过期非零禁缩、稳定窗缩容、min=0 缩零+unserved 冷起、发布冻结含 panic、配额冻结/解冻、手动接管；断言含 `hold_rollout` 指标计数，非空过）。
- `bash scripts/lab/verify.sh --layer l2` PASS（steps=4 failed=0）：smoke-p0、e2e-m3、e2e-m4、e2e-m5（A-F 段全绿，含 PG 备份恢复、Redis flushall 重投影、drain→rebuild 升级演练、终态零泄漏）。

验收中发现并修复：

- 已删 machine 的存量 create 无限重试（poison op 挤占派发队列）：`processCreate` 在 placement/RPC 前按 `desired_state=DELETED` 终态收敛为 `SUPERSEDED`（`controller.go` + `dispatch_r2_test.go` 回归）。
- `e2e-autoscale.sh`：信号 keeper（长等待持续刷新）、`FIREPAAS_ROLLOUT_DRAIN=90s`（发布冻结断言窗口）、断言时校验 rollout 仍活跃 + `hold_rollout` 计数、配额恢复后再验 unfreeze、JSON 助手容错。
- lab 脚本健壮性：`smoke-p0` 排除 `fp-*` 基础设施 netns；`e2e-m3` Go 路径回退（`FIREPAAS_GO`）+ 预清理孤儿 slot netns/veth + 前置无 live VM 断言；`e2e-m4` API 端口 `FIREPAAS_LAB_API_PORT`、`e2e-m3` edge 端口 `FIREPAAS_LAB_EDGE_PORT` 可覆盖（宿主常驻进程占用 8081）；`run-agentd.sh` 忽略终态 alloc（多 client 单机宿主的 failed alloc 不再阻塞）；`upgrade-agentd.sh` 只重启 running alloc（dead 节点 alloc 会让 `nomad job restart` 整体失败）；`verify.sh` 白名单透传新变量。
