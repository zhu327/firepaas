# 单机版 M1 执行计划（vertical slice）

> 状态：草案 v1（2026-08-25）
> 依据：`docs/mvp-plan.md` §5、ADR-0012（单机基线）。M0 数据面验证已在单机完成
> （见 `docs/benchmarks.md`），本计划定义 M1 在单机上的最小闭环与任务切分。

## Goal

在本机跑通 M1 vertical slice：

```text
authenticated API → PG desired state + operation → controller
→ mTLS agent Create → observed state → Redis route projection → edge → HTTP 200
```

M1 只承诺 `Info`、`CreateMachine/ListMachines/DeleteMachine` 与最小 proxy 路径；
Pause/Resume/Checkpoint/Image/Exec 保持实验状态（mvp-plan §5.2）。

## 单机部署形态（M1 决策，落实 ADR-0012）

| 组件 | 单机形态 | 说明 |
|---|---|---|
| agentd | Nomad system job（raw_exec/root，node_pool=compute） | M1.4 起替代 hypeman-p0 job |
| control-plane | 独立进程（root，scripts/lab 启停） | 单机不跑 control 池 service job |
| edge-proxy | 独立进程（root，scripts/lab 启停） | 同上；HTTP 80 满足 U1，TLS 属 M4 |
| PG/Redis/MinIO/registry | Docker（iac/dev/docker-compose.yaml） | 已运行 |
| Nomad/Consul | scripts/lab 单机配置 | 已运行 |

## 依赖与顺序

```text
① M1.1 工程基线（根 go.mod + proto 生成 + dev/test 命令）
→ ② M1.2 契约冻结（proto fencing/readiness/proxy + shared 类型）
→ ③ M1.4 agentd 最小能力（包 hypeman Manager + operation ledger）
→ ④ M1.5 controller（PG desired/observed/operation + outbox + 单 replica reconcile）
→ ⑤ M1.6 agent proxy v0 + 最小 edge（route projection 查询）
→ ⑥ M1.3 身份收尾（mTLS 静态证书降级路径，ADR-0006）
→ ⑦ e2e harness 一键冒烟
```

### M1.1 工程基线

- 收敛为**一个根 `go.mod` + 多个 `cmd/*`**（mvp-plan 默认策略）：
  `cmd/agentd`、`cmd/api`、`cmd/edge-proxy`；`internal/*` 按层分包。
  删除 agent/control-plane/edge/shared 四个子 go.mod（保留历史提交）。
- 根 go.work 继续 `use ../hypeman`；根 go.mod 用
  `replace github.com/kernel/hypeman => ../hypeman` 供 GOWORK=off 构建。
- proto 生成接入：安装 protoc/protoc-gen-go/protoc-gen-go-grpc 到
  `~/.local/firepaas-lab/bin`，`make proto` 生成到 `shared/gen/agent/v1`。
- 验收：`make build test proto` 全绿；CI 骨架（后续补）。

### M1.2 契约冻结

- 以 `protos/agent/v1/agent.proto` 现有草案为基线，冻结稳定子集：
  `InfoService.ServiceInfo`、`MachineService.Create/List/Delete`、
  `MachineReadiness`、最小 `proxy` 字段。
- 生成代码后给 control-plane/agent 添加编译期约束测试（fencing 字段必填、
  MachineSpec 不含敏感字段）。
- 验收：proto lint/breaking 检查可执行；agentd/api 均引用同一生成包。

### M1.4 agentd 最小能力

- `cmd/agentd` 用 hypeman `lib/instances.Manager`（**不 import
  cmd/api/config 与 providers**，见 agent/internal/README.md spike 结论）。
- 实现 `internal/state` operation ledger：原子持久化 request hash/result、
  重启重放、同 operation_id 重试返回原结果、不同 hash 拒绝。
- observed state 上报：重启扫描 + 周期上报 ServiceInfo/Create/List/Delete。
- 验收：spike 级别的 Create/List/Delete 通过；ledger 单测覆盖重放/拒绝。

### M1.5 controller

- PG migrations：projects/api_keys/quotas（M2 依赖，现在建表）、
  machines（desired/observed/operation/route/backend）。
- 单实例 controller：`CreateMachine` 写 desired + operation → 调 agent →
  记 observed → Redis route projection。
- 验收：PG 数据可查；Redis 删除后可由 PG+agent 重建 projection。

### M1.6 proxy v0 + edge

- agent proxy：edge mTLS 校验 + execution 校验 + 仅转发声明端口
  （bridge workload endpoint adapter）。
- edge：单 hostname、单 backend、从 Redis route catalog 查询。
- 验收：单机 nginx 经 hostname 返回 200；删除后 route 消失。

### M1.3 身份收尾

- 用足 ADR-0006 降级路径：静态证书 + 主机端口 ACL；step-ca 选型记录。
- 验收：无 mTLS identity 的请求不能访问 5108/5107。

### ⑦ e2e harness

- `scripts/lab/e2e-m1.sh`：一键起 dev 依赖 + agentd + api + edge，跑 U1 冒烟。
- 验收：脚本重复执行结果一致；作为后续里程碑复用入口。

## 本轮可并行/串行策略

- 单机、单人执行，**不并行写共享文件**；顺序 ①→②→③→④→⑤→⑥→⑦。
- 每步先落小提交再前进；环境已跑通的 M0 脚本不重写，新增 M1 脚本。

## 风险

- 根 module 收敛会短暂影响 `make build` 的四个子目录；用一次提交完成并立即验证。
- agentd 若直接 import `lib/providers` 会引入 cmd/api/config 红线依赖；坚持
  lib/* + 自建装配（spike 已证明可行）。
- 本机 k8s 与桌面负载可能让 p95 抖动；M1 验收以功能正确性为主，性能数据只做参考。

## 执行记录（2026-08-26）

- M1.1 完成：单根 module（`github.com/example/firepaas`）+ `cmd/*`/`internal/*`；
  proto 生成接入 `make proto`（protoc 29.3 + go 插件）。
- M1.2 完成：ADR-0013 稳定子集冻结；`internal/contracts/agentv1` 基于
  protoreflect 的契约不变量测试接入 `make test`。
- M1.4 完成：agentd 只 import hypeman `lib/*`（`lib/config` 别名入口已 upstream
  到 hypeman lab 分支）；operation ledger 真机验证幂等重放与冲突拒绝；
  Info/Create/List/Delete 经 Nomad system job 运行。
- M1.5 完成：PG migrations（projects/api_keys/apps/machines/operations/routes/
  route_backends）、controller reconcile、Redis route/location 投影；删除后
  stale route 清理已验证。
- M1.6 完成：agent proxy（execution 校验 + bridge endpoint）与最小 edge；
  U1 通过：hostname → edge → Redis catalog → proxy → Firecracker nginx → HTTP 200。
- M1.3 完成：静态 mTLS（scripts/lab/gen-certs.sh + internal/security/mtls）；
  无证书访问 5108/5107 拒绝，持证书 API/agentctl/edge 全链路可用。
- ⑦ 完成：`sudo bash scripts/lab/e2e-m1.sh` 一键 PASS。

## 已知遗留（进入 M2 前记录）

1. agentd 进程重启会带走其子进程（raw_exec 任务终止杀进程组），VM 丢失后
   observed state 变 UNSPECIFIED；controller 已不再把非 RUNNING 放入 route。
   M2 需要实现 orphan 收养/重建决策表（mvp-plan §6.4）。
2. Nomad 2.0 system job 更新后 Latest Deployment 仍显示历史 failed；当前 alloc
   healthy 且 service checks success，属展示层脏状态，M2 前清理或升级 Nomad 版本验证。
3. PG operations 的 CLAIMED 状态在进程崩溃后不会自动回 PENDING（当前单实例
   可接受）；M2 加 lease/超时回收。
4. hostname 与 ingress_port 为 M1 实验字段（proto 编号 19 / NetworkSpec.4），
   M3 route 冻结时转正。
