# ADR-0009:放置约束过滤器与副本反亲和(先过滤后打分)

状态:已接受(2026-08-25)
依据:骨架评审补充——M3 用户故事 U3 "scale 3 在可用节点间放置"在契约与
scheduler 骨架中没有任何约束表达;e2b 的 Best-of-K 单副本场景原生不需要反亲和,
PaaS 多副本必须显式补齐。

## 决策

### 1. 调度管线分两层:硬过滤在前,Best-of-K 打分在后

```
候选节点 = 全量节点
  → 状态过滤(Healthy、CanAcceptNewRequests)
  → 架构/label/node_pool 过滤(PlacementConstraints)
  → 反亲和过滤(同 deployment 已有副本所在节点,尽力而为)
  → 资源过滤(剩余可售容量 ≥ 请求)
  → 随机抽 K → Best-of-K 打分 → 最低分
```

- `PlacementConstraints`(proto,`MachineSpec.placement`):
  `node_pool`(默认 compute)、`labels`(map,key=value 硬匹配,覆盖 arch/GPU/池)、
  `anti_affinity`(NONE / DEPLOYMENT)。
- **DEPLOYMENT 反亲和为尽力而为(soft)**:过滤后候选为空时允许在排除集内重新
  打分并记录调度事件,不为反亲和牺牲可用性;`NONE` 不做排除。
- 排除集由控制面从 PG 现存活 machine 的 `node_id` 推导,不依赖 agent 上报;
  resume/重建回 origin node 的优先级高于反亲和(ADR-0002 的顺序保持:
  origin 优先 → 过滤 → 打分)。

### 2. 实现位置与里程碑

- 过滤器在控制面 scheduler(`internal/scheduler`)实现,agent 不参与决策;
  agent 的硬准入仍只校验本机资源(ADR-0002 的双保险语义不变)。
- M2 落地 Best-of-K 时实现状态与资源过滤、预留约束接口;**M3 交付 U3 前实现
  label/node_pool 过滤与 DEPLOYMENT 反亲和**。
- 反亲和只作用于同一 `(deployment_id)` 的新副本;跨 app 反亲和、跨可用区
  spread、节点均衡再平衡(rebalance)均明确移出 MVP。

## 理由

1. 没有反亲和,Best-of-K 在小集群(2 节点)会把同一 app 的两个副本放上同一节点,
   节点故障即 app 整体下线——这是 PaaS 与代码沙箱场景的本质差异。
2. "先过滤后打分"与 e2b placement 的现有结构(不可接受节点先于采样剔除)一致,
   不改动打分公式,移植成本最低。
3. 反亲和做成"尽力而为 + 调度事件"而非硬约束,避免小集群(1 个健康节点)下
   scale 永远失败,与降级表"失联节点重建"语义自洽。

## 后果

- proto:`MachineSpec` 增加 `PlacementConstraints placement`;M1 契约冻结项更新。
- scheduler 实现位于 `internal/scheduler/`，控制面 placement 层负责提供候选与
  约束；仿真器断言增加"DEPLOYMENT 反亲和下副本落点 distinct,
  除非候选不足"。
- 调度事件(跳过原因、降级打分)需纳入 M2 最低可观测项(mvp-plan §6.5)。
