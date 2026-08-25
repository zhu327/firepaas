# ADR-0008:guest 交互契约与 readiness 信号归属(agent host 侧探针)

状态:已接受(2026-08-25)
依据:骨架评审补充——`MachineSpec.health_check` 已定义但没有执行者归属,`Machine`
消息里没有 readiness 字段,M3 的"readiness 后才发布 route generation"(ADR-0005)
缺少信号来源。

## 决策

### 1. agent↔guest 的运行时通道(沿用 hypeman,不改语义)

- guest 内运行 hypeman 注入的 guest agent(`lib/system`/`lib/guest`),控制面
  `Exec` 流式 RPC 由 agent 经 vsock 转发给 guest agent;MVP 不引入第二套 in-guest 协议。
- `/dev/kvm`、vsock CID 由 agent 在节点范围分配并上报(architecture.md §4.2),
  控制面不做全局分配。
- guest 镜像基件的 boot marker / init 完成信号维持 hypeman 现有语义;控制面只消费
  `MachineState`(proto),不直接解释 guest 内部事件。

### 2. 健康/就绪信号:agent 在 host 侧执行探针,作为 observed state 上报

- **执行者是 agent**:agent 经内部 workload endpoint（M1 bridge 后端时为 guest TAP IP，M3 slot 后端时为 slot IP）对
  `MachineSpec.health_check` 声明的 target 执行 HTTP/TCP/EXEC 探针,复用 hypeman
  `lib/healthcheck`;不在控制面、不在 edge、不在 guest 内做全局健康判断。
- **`Machine` 消息携带 `MachineReadiness readiness` 字段**(UNKNOWN / NOT_READY /
  READY / UNCONFIGURED)与 `last_readiness_change`,随 `CreateMachine` 响应与
  `ListMachines` 上报;这是 controller 更新 `route_backends.readiness` 的唯一来源。
- controller 只在 `state=RUNNING && readiness=READY` 时把 backend 纳入新 route
  generation(ADR-0005 的顺序第 2-3 步);readiness 抖动策略(连续 N 次才翻转)
  属 agent 本地参数,不进契约。
- readiness 是 **observed state**,不写回 PG machines 表作为权威;PG 的
  `route_backends.readiness` 是 controller 依据观测投影出的路由决策(ADR-0003)。

### 3. 边界

- agent 失联时,其节点上所有 backend 的 readiness 按 UNKNOWN 处理,controller 按
  对账决策表摘除或重建(ADR-0003)。
- edge 不执行应用层健康检查,只消费 route projection;跨副本摘除是 controller 职责。

## 理由

1. 没有归属的健康检查必然导致 M3 临时造信号(常见反模式:edge 自己做探针、控制面
   直连 VM、把 RUNNING 当 READY),事后 breaking change 代价高。
2. agent host 侧探针与 hypeman 单机语义一致,复用面最大;同时满足"控制面不直连
   VM"(架构 §2)与"edge 不猜后端"(ADR-0005)。
3. proto 在 M1.2 冻结前补齐字段,避免契约事后演进。

## 后果

- proto:`Machine` 增加 `readiness`/`last_readiness_change`;M1 契约冻结项同步更新。
- agent 需在 M1 实现最小就绪上报(无 health_check 声明时返回 UNCONFIGURED,等价
  RUNNING 即 READY,保证 M1 vertical slice 不被探针阻塞);探针执行器在 M3 前补齐。
- hypeman `lib/healthcheck` 的迁移列入 agent internal/health 工作包(M3)。
