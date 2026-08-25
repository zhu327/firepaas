# ADR-0007：控制面写者数量随里程碑演进（M1 单实例 → M2a leader → M2b 多写）

状态：已接受（2026-08-25，骨架评审补充）
依据：ADR-0002、架构评审；`iac/nomad/control-plane.hcl` 的副本数与单写者约束存在冲突

## 决策

控制面 API 的**部署副本数**与**写者数量**绑定，按里程碑演进，不由部署参数自由决定：

| 阶段 | Nomad job `count` | reconcile / 放置写者 | 依据 |
|---|---|---|---|
| M1 | 1 | 单实例即单写者 | 无 leader election、无 Redis 预约；controller 是唯一调和 PG desired ↔ agent observed 的组件 |
| M2a | ≤2 | PG advisory lock（短租约）选主，备实例只读待命 | 消灭双记账主体（ADR-0002 的"单写者先行"） |
| M2b | 多实例 | 创建路径多写（Redis Lua 预约 + PG 幂等键）；reconcile 仍单写者（或按 app 分区） | ADR-0002 的完整方案 |

- `count` 的提升是**代码能力交付的结果**：未具备对应机制的阶段，job 副本数必须保持为机制允许的上限；`control-plane.hcl` 中的注释与本表同步。
- M1/M2a 单写者期间，API 实例故障由 Nomad restart 兜底。该窗口内控制面不可用（创建/发布/对账暂停），但**数据面流量不经 API**（架构 §2），在运 app 不受影响——这是接受单写者的前提。

## 理由

1. 多个 API 实例并发 reconcile 同一 desired state，会在没有任何互斥机制时产生重复下单、route generation 互覆写、orphan 误判；这正是 ADR-0002 要消灭的双记账场景。
2. leader election（PG advisory lock）实现成本远低于完整的多写路径，且保留 Best-of-K/记账逻辑不变，风险最小。
3. 把副本数写进 ADR 而非留作部署细节，是因为它直接决定 `control-plane.hcl` 的形态与升级路径，评审时需要单一事实来源。

## 后果

- M1 的控制面 HA 被有意推迟；单实例重启窗口（秒级）计入 M1 验收观察项，不算故障。
- M2a 需要定义租约时长与切换语义（备实例提升时如何衔接 in-flight operation），进入 M2 工作项。
- M2b 之后 reconcile 若仍单写，需说明 leader 故障时对账循环的恢复时间对 SLO 的影响。
