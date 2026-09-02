# firepaas

基于 Firecracker 的私有 PaaS 平台（目标形态：私有化 Fly.io）。

- 数据面复用 [hypeman](https://github.com/zhu327/hypeman) 的 VM/镜像/快照能力（`firepaas-lib` 分支，tag `v0.4.0-firepaas`，作为 Go module 直接消费）
- 管控面与调度模式参考 [e2b-dev/infra](https://github.com/e2b-dev/infra)：控制面/数据面分离、Best-of-K 自研调度、Nomad 只编排基础设施作业
- 当前状态：MVP 主体（M1–M5）及 v1.1–v1.4 的主要代码路径已实现；单机 smoke 覆盖部分能力，但版本发布门禁、标准多节点故障矩阵与长期观测尚未全部满足。当前发布证据以 [GA observation scorecard](docs/ga-observation-scorecard.md) 和各版本记录为准。

## 文档

| 文档 | 说明 |
|---|---|
| [docs/architecture.md](docs/architecture.md) | 目标架构、状态权威、路由与 fencing 契约 |
| [docs/adr/](docs/adr/) | 关键设计决策（Nomad 边界、调度、状态分层、网络、route catalog、内部身份、secret 路径、edge 入口等 38 篇） |
| [docs/releases/README.md](docs/releases/README.md) | MVP–v1.4 的范围、实现记录与证据状态索引 |
| [docs/mvp-plan.md](docs/mvp-plan.md) | MVP 范围、实现记录、出口和降级策略 |
| [docs/v1.1-plan.md](docs/v1.1-plan.md) | v1.1 范围与验收契约；实现状态另见同版本 implementation notes |
| [docs/v1.2-plan.md](docs/v1.2-plan.md) | v1.2 范围与验收契约；当前发布门禁尚未全部满足 |
| [docs/v1.3-plan.md](docs/v1.3-plan.md) | v1.3 范围、实现状态与验证边界 |
| [docs/v1.4-plan.md](docs/v1.4-plan.md) | v1.4 方向性范围；当前仍未完成版本级验收 |
| [docs/ga-observation-scorecard.md](docs/ga-observation-scorecard.md) | GA 证据状态与尚未评估项 |
| [docs/runbook-*.md](docs/runbook-*.md) | 运维流程：soak、备份恢复、容量、HA 验证、节点替换等 |

## 仓库布局

```
firepaas/
├── cmd/agentd/          # 每节点 gRPC agent（VM/镜像/网络数据面）
├── cmd/api/             # 控制面 API + 调度器 + 节点管理 + 预约
├── cmd/edge-proxy/      # 边缘路由（TLS + catalog 路由 + 自动唤醒）
├── cmd/fpctl/           # 运维 CLI
├── cmd/agentctl/        # agent 侧运维 CLI
├── internal/agent/      # agent 实现（server/machine/network/proxy/state）
├── internal/controlplane/ # 控制面实现（api/db/store/controllers/...）
├── internal/edge/       # edge 实现（router/catalog/autoresume/tls）
├── internal/scheduler/  # Best-of-K 放置算法
├── shared/pkg/          # ID/错误等公共库
├── shared/gen/          # proto 生成代码（已跟踪入库；make proto 重新生成后提交。
│                         # CI 门禁：重新生成后 git diff 必须为空且无 untracked 文件）
├── protos/agent/v1/     # agent gRPC 契约（唯一数据面契约）
├── iac/                 # Nomad jobs + Terraform + 可观测性配置
├── scripts/             # 实验室搭建、e2e/混沌/soak 脚本
└── docs/                # 架构、计划、ADR、runbook
```

## 快速开始

依赖：Go 1.25、Linux（KVM）、Nomad 1.10+、PostgreSQL、Redis、容器 registry。

```bash
git clone https://github.com/zhu327/firepaas.git
cd firepaas
make build   # go build ./...（hypeman 作为远程 module 经 go.mod replace 拉取）
make test    # go test ./...
make check   # build + vet + test + tidy-check（本地 CI 等价）
```

### 单机实验室（ADR-0012）

只有一台机器时，使用折叠版实验室（不写 /etc、不启巨页，可与 k8s 共存）：

```bash
# 开发依赖（PG/Redis/MinIO/registry）
docker compose -f iac/dev/docker-compose.yaml up -d

source scripts/lab/env.sh
bash scripts/lab/start.sh             # 单节点 Nomad + compute 池
sudo bash scripts/lab/root-setup.sh   # root 准备并切换 Nomad 到 root 运行
sudo bash scripts/lab/run-agentd.sh   # 部署 agentd system job
sudo bash scripts/lab/e2e-m1.sh       # M1 一键验证（API→agent→edge→VM→HTTP 200）
```

完整验收矩阵与脚本说明见 [scripts/lab/README.md](scripts/lab/README.md)。

### 多节点实验室

```bash
# 3 台 server（兼任 control 池）+ 2 台 compute 节点（Ubuntu 24.04，KVM 必需）
sudo bash scripts/bootstrap-lab.sh server                 # 在 3 台 server 节点执行
sudo bash scripts/bootstrap-lab.sh compute <server-ip>    # 在 2 台 compute 节点执行

# 集群就绪后创建节点池（任一 server）
nomad node pool create iac/nomad/pools/control.hcl
nomad node pool create iac/nomad/pools/compute.hcl
```

## Go 工程策略

单根 module（`github.com/zhu327/firepaas`）：`go.mod` + 多个 `cmd/*` + `internal/*`。

hypeman 依赖经 `go.mod` replace 指向公开 fork 的 `v0.4.0-firepaas` tag（`github.com/zhu327/hypeman`，`firepaas-lib` 分支）：该 tag 提交了 go:embed 必需的 firecracker/guest-agent/init 二进制，可远程作为 module 消费，clone 后无需 sibling checkout。上游 [kernel/hypeman](https://github.com/kernel/hypeman) 发布包含所需 API 的正式 tag 后，可切换 require 并删除 replace。

本地如需联调未发布的 hypeman 改动，可创建不入库的 `go.work.local` 覆盖 replace；CI 与 release 始终以 `GOWORK=off` 走根 `go.mod`。

## 核心原则

1. **Nomad 不调度用户 VM**。它只部署基础设施作业（agent system job 等）、提供服务发现、对齐作业副本与节点池。
2. **VM 放置由控制面自研 Best-of-K 调度器决定**，API 决定"放哪"，agent 决定"怎么跑"。
3. **流量永不经过控制面**：固定为 edge → Redis routing catalog → 节点 agent proxy → VM；edge/catalog 不感知 slot IP。
4. **状态分层**：PostgreSQL 是期望状态/业务事实的唯一权威，agent 是观测运行态，Redis 是可重建 route/lease 投影。
5. **hostname 路由到版本化 backend set**，而不是直接路由单台 machine；所有 VM 操作均由 execution/generation/operation fencing。

## License

[MIT](LICENSE)
