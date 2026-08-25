# firepaas

基于 Firecracker 的私有 PaaS 平台(目标形态:私有化 Fly.io)。

- 数据面复用 [hypeman](https://github.com/kernel/hypeman) 的 VM/镜像/快照能力
- 管控面与调度模式参考 [e2b-dev/infra](https://github.com/e2b-dev/infra):控制面/数据面分离、Best-of-K 自研调度、Nomad 只编排基础设施作业
- 当前状态:**M0 验证准备阶段（MVP 开发尚未开始）**；在真实 Nomad job、自动基准与 hypeman adapter spike 通过前，不进入 M1

## 文档

| 文档 | 说明 |
|---|---|
| [docs/mvp-plan.md](docs/mvp-plan.md) | **修订 MVP 计划**：范围、阶段、出口和降级策略 |
| [docs/architecture.md](docs/architecture.md) | 目标架构、状态权威、路由与 fencing 契约 |
| [docs/adr/](docs/adr/) | 关键决策：Nomad、调度、状态、网络、route catalog、内部身份、写者数量演进、readiness 信号、放置约束/反亲和、secret 路径、edge 入口 |
| [../docs/paas-feasibility-analysis.md](../docs/paas-feasibility-analysis.md) | 基于 hypeman 与 e2b-dev/infra 的完整可行性分析 |

## 仓库布局

```
firepaas/
├── control-plane/       # 新写:控制面 API + 调度器 + 节点管理 + 预约
├── agent/               # 从 hypeman 抽取:每节点 gRPC agent(VM/镜像/网络数据面)
├── edge/                # 新写:边缘路由(TLS + catalog 路由 + 自动唤醒)
├── shared/              # proto 生成代码、ID/错误/存储客户端等公共库（M1 默认并入单根 module）
├── protos/agent/v1/     # agent gRPC 契约(唯一数据面契约)
├── iac/                 # Nomad jobs + Terraform(参考 e2b-dev/infra 裁剪)
├── tools/               # CLI、调度仿真器
├── scripts/             # 实验室搭建、基准测试脚本
└── docs/                # 架构、MVP 方案、ADR
```

## 快速开始(实验室)

```bash
# 1. 准备 3 台 server(兼任 control 池)+ 2 台 compute 节点(Ubuntu 24.04,KVM 必需)
sudo bash scripts/bootstrap-lab.sh server                 # 在 3 台 server 节点执行
sudo bash scripts/bootstrap-lab.sh compute <server-ip>    # 在 2 台 compute 节点执行

# 2. 集群就绪后创建节点池(任一 server)
nomad node pool create iac/nomad/pools/control.hcl
nomad node pool create iac/nomad/pools/compute.hcl

# 3. 阅读 iac/README.md；P0 使用 iac/nomad/hypeman-p0.hcl，不能使用未来 agentd job

# 4. 构建各组件(要求 firepaas 与 hypeman 同级 checkout,见 go.work)
make build

# 5. 从 P0 验证开始,详见 docs/mvp-plan.md
```

### 单机实验室(当前开发基准,ADR-0012)

只有一台机器时,使用折叠版实验室(不写 /etc、不启巨页,与 k8s 共存):

```bash
source scripts/lab/env.sh
bash scripts/lab/build-hypeman.sh     # 构建嵌有 Firecracker 的 hypeman
bash scripts/lab/start.sh             # 单节点 Nomad + compute 池
sudo bash scripts/lab/root-setup.sh   # root 准备并切换 Nomad 到 root 运行
sudo bash scripts/lab/run-p0.sh       # 部署 P0 job 并等 /health
sudo bash scripts/lab/smoke-p0.sh     # P0 冒烟
```

开发依赖（PG/Redis/MinIO/registry）：受限网络先经 `docker.m.daocloud.io` 拉取并
retag，再 `docker compose -f iac/dev/docker-compose.yaml up -d`。

详见 [scripts/lab/README.md](scripts/lab/README.md) 与 [docs/plans/2026-08-25-single-node-m0.md](docs/plans/2026-08-25-single-node-m0.md)。

## Go 工程策略

当前使用占位 module path `github.com/example/firepaas/*`。M1 工程基线默认收敛为**一个根 `go.mod` + 多个 `cmd/*`**；如确有独立版本/依赖隔离需求才保留多 module，并以 ADR 记录。正式建仓时替换组织路径。

本地 `go.work` 可引用同级 `../hypeman` 做联调；CI/release 必须以 pin 到具体 commit/tag 的 hypeman 依赖执行 `GOWORK=off` 构建，避免发布结果随工作区漂移。

## 核心原则(来自可行性分析)

1. **Nomad 不调度用户 VM**。它只部署基础设施作业(agent system job 等)、提供服务发现、对齐作业副本与节点池。
2. **VM 放置由控制面自研 Best-of-K 调度器决定**,API 决定"放哪",agent 决定"怎么跑"。
3. **流量永不经过控制面**：M1 起固定为 edge → Redis routing catalog → 节点 agent proxy → VM；edge/catalog 不感知 slot IP。
4. **状态分层**:Postgres 是期望状态/业务事实，agent 是观测运行态，Redis 是可重建 route/lease 投影。
5. **hostname 路由到版本化 backend set**，而不是直接路由单台 machine；所有 VM 操作均由 execution/generation/operation fencing。
