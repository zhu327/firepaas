# ADR-0015：M3 发布状态机冻结（rollout/drain/回滚 + 组合场景决策表）

状态：已接受（2026-08-26）
依据：ADR-0005 后果项「发布与 scale/节点故障/再次发布的组合场景决策表在 M3
冻结；MVP 至少实现同一 app 同时只允许一个 rollout 的互斥」；mvp-plan §7.3。

## 决策

### 1. 实体与状态

- **deployment**：app 的一次不可变发布目标（image_ref/vcpu/mem_mib/port/env/
  generation）。机器创建后其 `deployment_id/generation` 不再改变；修正只能
  产生新 deployment。
- **rollout**：`(app_id, from_generation → to_generation)` 的一次发布过程。
  PG `rollouts` 表唯一活跃约束（`app_id` 上最多一条 status ∈
  PREPARING/CUTOVER/ROLLING_BACK）保证单 rollout 互斥。
- 状态机：

```
            deploy
  (无活跃 rollout) ──────► PREPARING ──(全部新 backend RUNNING+READY)──► CUTOVER
                              │                                                │
                              │ 超时/新代 create 终态 FAILED                    │ drain 期限到：回收旧 execution
                              ▼                                                ▼
                          ROLLING_BACK ──(旧 generation 重新发布完成)──► COMPLETE(failed=true)
```

- 发布中再次 deploy：409 拒绝（MVP 互斥，复杂组合延后）。
- 发布中 scale：scale 只作用于**目标 generation**（PREPARING/CUTOVER 期间
  desired_replicas 变化直接对目标 ordinal 集对账）；发布中 scale down 到 0
  等组合场景不在 MVP（明确延后，API 拒绝或文档记录）。
- 发布中节点故障：机器级决策仍走 M2 R1-R8（该换代的机器按自己的 generation
  重建）；rollout 只关心「目标 generation 全部 ordinal 是否 READY」。

### 2. 发布与 route 投影（ADR-0005 顺序落地）

1. controller 每轮 route 重建读 PG `rollouts` 活跃状态：
   - PREPARING：active generation = from；新 backend 以 readiness 实际值写入
     `route_backends`（generation=to），但不在 active backend set；edge 不感知。
   - CUTOVER：active generation = to；to-generation 的 READY backend 进 active
     set；from-generation backend 保留但 `draining=true`。
   - ROLLING_BACK：active generation = from；to backend 摘除；from backend
     解除 draining。
   - COMPLETE：active generation = to（failed=true 时为 from）；旧代 backend
     已删除。
2. 切流条件：**全部**目标 ordinal 的 machine 均 `observed_state=RUNNING` 且
   `readiness=READY`（无 health_check 时 UNCONFIGURED 等价 READY，ADR-0008）。
   任一失败/超时（默认 300s，可配）→ 自动 ROLLING_BACK。
3. drain：CUTOVER 后旧 backend 只服务已建连请求；`drain_deadline`（默认 30s，
   可配）后 controller 下发旧 execution 的 reap delete 并置 rollout COMPLETE。
4. edge 契约不变：只读 route projection 的 backend set，按
   `draining=false && readiness ∈ {READY, UNCONFIGURED}` 选择；edge 永不读
   slot IP（ADR-0005）。

### 3. 决策表（组合场景冻结）

| # | 场景 | 决策 |
|---|---|---|
| S1 | PREPARING 中新代 ordinal 缺失/被杀 | AppController 按目标 generation 补建（M2 R1-R8 收敛） |
| S2 | PREPARING 中新代 create 终态 FAILED 且退避重试耗尽（3 次） | ROLLING_BACK |
| S3 | PREPARING 超时（300s）未全 READY | ROLLING_BACK |
| S4 | CUTOVER 中 from 代机器死亡 | 不重建 from 代（已 draining），提前回收 |
| S5 | CUTOVER 中 to 代机器死亡 | 按 to generation 重建；重建期间该 backend 不进 active set，其余 to backend 继续服务 |
| S6 | ROLLING_BACK 中 | from generation 机器保持/补建；to 代机器删除；完成后 COMPLETE(failed) |
| S7 | 发布中再次 deploy | 409（MVP 互斥） |
| S8 | 发布中 scale | 作用于目标 generation ordinal 集；不重置 rollout 计时 |
| S9 | 发布中 app 删除 | rollout 级联删除；所有机器按 ordinal 下发 delete |

## 理由

- 发布状态机放控制面（PG 权威、Redis 投影）与 ADR-0003/0005 一致；agent 无
  发布概念，只上报 observed state。
- 「全部 READY 才切流」与「失败自动回滚」把 U2 验收变成可判定条件，避免
  部分发布的人工判断。
- 单 rollout 互斥符合风险表降级项，且 rollouts 表唯一约束是数据库级互斥。

## 后果

- migration 0006：deployments、rollouts 表 + 唯一部分索引。
- controller 新增 AppController/rollout reconcile 阶段（在 machine reconcile
  之后、buildRoutes 之前）。
- buildRoutes 改读 rollout 状态决定 active generation 与 draining 集合。
- API：deploy/scale 端点 + 409 互斥；fpctl 最小 CLI。
- e2e-m3 断言 S1-S9 的 U2 子集（超时/失败回滚、drain 后旧代回收、409 互斥）。
