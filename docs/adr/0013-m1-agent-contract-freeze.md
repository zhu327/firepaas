# ADR-0013:M1.2 agent gRPC 契约稳定子集冻结

状态:已接受(2026-08-25)
依据:`docs/mvp-plan.md` §5.2、`protos/agent/v1/agent.proto` 草案、
ADR-0003/0006/0008/0010。

## 决策

M1 只对以下 agent 契约子集给出稳定性承诺，其余方法/字段保持实验状态：

| 服务 | 稳定方法 |
|---|---|
| InfoService | `ServiceInfo` |
| MachineService | `CreateMachine`、`ListMachines`、`DeleteMachine` |
| ImageService | 无（实验） |

稳定类型：`Machine`、`MachineSpec`（含 `PlacementConstraints`/`SecretRef`/
`HealthCheckSpec`/`NetworkSpec` 的字段编号）、`MachineState`、
`MachineReadiness`、`CreateMachineRequest`、`DeleteMachineRequest`、
`ListMachines*`、`MachineOperationRequest` 及 Start/Stop/Pause/Resume/
Checkpoint 请求包装（字段编号冻结，但 Pause/Resume/Checkpoint 的
**运行时语义仍为实验状态**）。

以下为实验状态，不承诺兼容：`Exec`、`StreamLogs` 的帧细节、
`UpdateMachine`、`ImageService` 全部方法、`CheckpointMachine` 语义。

## 不可破坏的契约不变量（有编译期测试）

1. 所有状态变更请求必须带 `machine_id + execution_id + generation +
   operation_id`。Create 的 `execution_id` 位于 `MachineSpec` 内（machine 自身的
   execution 代），其余顶层字段在 `CreateMachineRequest`；其他状态变更走
   `MachineOperationRequest`。
2. `MachineSpec` 与 `Machine` 回显结构中**不得出现** secret 值、代理凭证或
   traffic token 字段（ADR-0010；`secret_refs` 只引用 ID/version）。
3. `proxy_credential` 与 `secret_env` 只存在于 `CreateMachineRequest`，
   单向、请求内；不进入任何响应/列表/投影（ADR-0006/ADR-0010）。
4. `Machine.slot_ip` 只允许 control-plane 的 observed state 消费；
   edge/catalog 永不读取（ADR-0004/0005，代码评审检查）。
5. `MachineReadiness` 四值语义冻结：UNKNOWN / NOT_READY / READY /
   UNCONFIGURED；未声明 health_check 的 machine 上报 UNCONFIGURED，
   M1 降级为 RUNNING 即 READY（ADR-0008）。
6. 同一 `operation_id` 重试返回已记录结果；同 ID 不同 request hash 被拒绝
   （agent operation ledger 语义，见 internal/agent/state）。

## 变更流程

- 稳定子集任何字段重命名、改号、删除，或语义变更：先 ADR + 兼容性评审。
- 实验字段可演进，但不得复用已冻结字段编号；删除字段用 `reserved`。
- proto 每次生成后运行 `go test ./internal/contracts/...` 作为 breaking check。

## 后果

- `internal/contracts/agentv1` 新增基于 protoreflect 的契约测试，接入 `make test`。
- agent/control-plane 实现必须引用生成包，不得手写同结构体。
- M3 引入 slot 数据面时若需要改 `NetworkSpec`/`Machine`，按稳定流程走 ADR。
