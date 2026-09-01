# ADR-0021:节点排水驱离(drain evacuate)——零停机节点维护

状态:已接受(2026-08-28)
依据:M5.5 交付的 drain 只停止新放置(`nodes.draining` + scheduler 过滤),节点
升级走 `drain → nomad job restart → agentd SIGTERM 带走全部 VM → 对账重建`,
存在停机窗口(M3 已知行为:agentd SIGTERM teardown VM,重建由 R1–R8 收敛)。
对照评审(docs/fp.md §6.4)提出 evacuate 语义:drain 后逐实例受控迁移。

## 决策

### 1. drain API 语义扩展(向后兼容)

`POST /v1/nodes/{id}/drain` 请求体增加 `{evacuate: bool}`(默认 false):
- `evacuate=false`:M5.5 行为不变(停新放置,存量不动);
- `evacuate=true`:进入驱离编排(下述),驱离完成后节点 fp machine 归零,
  状态可由运维置 maintenance/ready。

### 2. evacuate 编排(复用既有机制,不新增状态机)

全局同时只允许一个 draining+evacuate 节点；该互斥由数据库部分唯一索引保证。
对节点上的 machine 按 ordinal 序、**一次一个**执行，当前 machine 与步骤开始时间
持久化在 `nodes`，所以 leader 切换后继续等待同一步骤：

```text
持久化 claim 当前 machine
  → delete source execution → R3 换代重建
  → 调度选点（避开 draining source）
  → replacement RUNNING/PAUSED + READY/UNCONFIGURED，且 route projection 可服务
  → 清持久步骤 → 下一个 machine
```

实现沿用 delete-then-recreate：同一 machine_id 的旧/新 execution 不会同时被
agent observed 去重逻辑混淆；副本数至少 2 时其它副本维持服务。

关键点:
- **standby 实例直接重建,不先唤醒**(快照本就节点本地,跨节点恢复不在承诺内,
  architecture §7:node-local 快照仅作加速);
- **与 rollout 互斥**:app 存在 active rollout 时,该 app 的 machine 跳过本轮
  (复用 `rolloutHoldsRecreate` 语义),记录调度事件,下一轮再迁;evacuate 不与
  rollout 并发编排;
- 编排器在 controller 内以独立 reconcile 分支实现（事件与审计进
  scheduler_events,kind=`evacuate_*`）。节点归零仍是完成判定；但当前步骤
  `machine_id/started_at` 是必须的持久恢复状态，防止重启或 leader 切换推进错序。
  `EvacuateStepTimeout`（默认 5m）到期会记告警并保持同一持久步骤，绝不跳到下一台。

### 3. 升级路径的标准形态

`scripts/lab/upgrade-agentd.sh` 升级为 evacuate 版:`drain {evacuate:true}` →
等归零 → nomad job restart(此时无 VM 可带走)→ ready → 对账确认。SIGTERM
带走 VM 的已知行为影响面收敛为"未 evacuate 的异常路径"。

### 4. 边界与文档化限制

- 单副本 app evacuate 存在重建窗口(切流前新代未 READY):runbook 明确
  **建议副本 ≥2 时执行 evacuate**;单副本场景接受短暂不可用(与 R4 节点失联
  重建同量级);
- evacuate 期间节点失联(R4 抢跑)以 R4 为准(先到先得,换代幂等);
- 不做并发多节点 evacuate、不做 evacuate 进度 API(进度从
  `GET /v1/nodes/{id}` + machine 列表推导)。

## 理由

1. 全部原语已存在:R3 换代重建、调度过滤 draining、readiness 门控、路由投影、
   delete 收尾——evacuate 只是"按序触发 + 等待收敛"的编排,无新协议;
2. 镜像亲和(ADR-0018)让重建落点优先已缓存节点,配合 prefetch 语义使 evacuate
   的重建时间可控(已缓存冷启动 p95 <5s);
3. 关闭"agentd 升级必然带停机窗口"这一运维债务,且是后续 agent 原地升级、
   内核维护等一切节点级操作的标准前置动作。

## 后果

- controller 增加 evacuate 编排分支与配置(concurrency、每步超时);
- e2e 新增:drain+evacuate 全程并发 curl 0 失败(副本 ≥2)、驱离后节点 machine
  归零、与 active rollout 组合不破坏单 rollout 互斥(ADR-0015);
- runbook-node-replacement/runbook-operations 更新为 evacuate 流程;
- upgrade-agentd 演练脚本重写并纳入 M 级验收复用。
