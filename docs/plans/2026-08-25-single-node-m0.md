# 单机版 M0 验证环境实施计划

> 状态：执行中（2026-08-25）
> 依据：ADR-0012（单机实验室基线）。本计划是 `docs/mvp-plan.md` §4 M0 在
> “唯一一台物理机”约束下的落地版；标准 3 server + 2 compute 实验室仍是
> 后续多机硬件的目标形态，本计划不推翻它，只定义单机基线上先验什么。

## Goal

在本机（Ryzen 7 8700G / 60GiB / KVM，已承载单节点 k8s + 桌面）建立**可重复、
非破坏性**的单机版 M0 实验室：1 个 Nomad server+client（node_pool=compute）、
本机 KVM 跑 hypeman + Firecracker，并用真实 job / 真实基准 runner 产出
M0 数据面 go/no-go 输入。

## Assumptions

1. 本机只有普通用户 zty 可被本会话直接使用；sudo/root 步骤是 HITL，由操作者执行。
2. 不得破坏现有 k8s/桌面：不写系统 sysctl（ip_forward 已开启）、不启用巨页、
   不改 systemd-resolved、不用 `/etc` 系统级服务；所有工具装在
   `/home/zty/.local/firepaas-lab`。
3. 网络可访问 GitHub、HashiCorp releases、Go 官方下载（已验证连通）。
4. hypeman 与 firepaas 同级 checkout（`/home/zty/Learn/hypeman`）不变；
   M0 数据面用**原版 hypeman 二进制**，不接 agentd。
5. M0 基准以 Firecracker 为默认 hypervisor；Cloud Hypervisor 仅作为对照采样。
6. 单机意味着双节点/三节点行为（quorum、跨节点放置、跨节点快照兼容）无法在本机
   验证，单机结论不得外推为多机结论。

## Architecture（单机折叠拓扑）

```text
本机 (Ubuntu 24.04, KVM)
├── /home/zty/.local/firepaas-lab/
│   ├── go/                # Go 1.25.4（用户态）
│   ├── bin/{nomad,consul} # Nomad 2.0.x / Consul 2.0.x
│   ├── bin/hypeman        # 构建产物（嵌入 Firecracker v1.14.2）
│   └── run/{nomad,consul} # 数据目录 + 日志
├── /var/lib/firepaas-p0/hypeman  # root 运行的 hypeman 数据目录（Nomad job）
├── Docker: postgres/redis/minio/registry（iac/dev/docker-compose.yaml）
└── Nomad(单节点, bootstrap_expect=1)
    └── client node_pool=compute（raw_exec, root）
        └── job firepaas-hypeman-p0 (type=system)
```

- Nomad/Consul 端口全默认（4646-4648 / 8300-8302 / 8500 / 8600），已确认与本机
  现有监听不冲突。
- M0 阶段只创建 `compute` 节点池；`control` 池在 M1 需要跑 control-plane/edge
  service job 时再处理（单机方案：control 组件直接以 systemd/docker/进程运行，
  或临时放宽 job 到 compute 池，见 M1 计划）。
- 所有写盘路径：工具链与代码在 `/home/zty/Learn`，运行时数据在
  `/var/lib/firepaas-p0`（root）与 Docker volume（user 加入 docker 组后由
  compose 管理）。

## Validation

```bash
# 工具链
~/.local/firepaas-lab/go/bin/go version
~/.local/firepaas-lab/bin/nomad version
~/.local/firepaas-lab/bin/hypeman 2>&1 | head -1        # 确认能启动/报配置错误即算产物可用

# 配置与 job（用户态即可执行）
~/.local/firepaas-lab/bin/nomad agent -config=scripts/lab/nomad-single.hcl &   # 后台
~/.local/firepaas-lab/bin/nomad node pool apply iac/nomad/pools/compute.hcl
~/.local/firepaas-lab/bin/nomad fmt -check iac/nomad/hypeman-p0.hcl
~/.local/firepaas-lab/bin/nomad job validate iac/nomad/hypeman-p0.hcl
~/.local/firepaas-lab/bin/nomad job plan iac/nomad/hypeman-p0.hcl

# 冒烟（root / HITL）
sudo ~/.local/firepaas-lab/bin/nomad job run iac/nomad/hypeman-p0.hcl   # 或 root 下重跑 agent
bash scripts/bench-hypeman.sh
```

## 依赖表

| Task | Type | Blocked by | 说明 |
|---|---|---|---|
| T1 计划与 ADR-0012 | agent | - | 本文档 + ADR |
| T2 git 基线 | agent | T1 | firepaas 本地 git init + baseline commit |
| T3 工具链安装 | agent | T1 | Go/Nomad/Consul 下载到用户目录 |
| T4 hypeman 构建 | agent | T3 | `make build-linux`（下载 CH/FC/Caddy，编译 Caddy） |
| T5 单机 Nomad/Consul 配置 | agent | T3 | 配置、启停脚本、pools |
| T6 hypeman-p0 job + config | agent | T4, T5 | 真实 artifact、真实 config、health |
| T7 job fmt/validate/plan | agent | T6 | 用户态 Nomad agent 即可 |
| T8 bench runner | agent | T6 | 把 TODO 脚本改成真实 runner |
| T9 Docker 依赖 | HITL | T1 | 需要把 zty 加入 docker 组（sudo） |
| T10 M0 冒烟与基准 | HITL | T7, T8, T9 | root 运行 raw_exec + KVM；产出 benchmarks 原始样本 |
| T11 agent adapter spike | agent | T10 | 依赖真实 Create/List/Delete 结果 |
| T12 M0 出口文档 | agent | T10, T11 | benchmarks.md / capacity-model.md / go-no-go |

## Tasks

### T1 单机基线与文档

- 新增 `docs/adr/0012-single-node-lab-baseline.md`：单机实验室的边界、单机结论
  外推限制、与标准 3+2 实验室的差异。
- 新增本计划文档。

### T3 工具链（用户态）

- Go 1.25.4 → `~/.local/firepaas-lab/go`（不动 `/usr/local`）。
- Nomad 2.0.x、Consul 2.0.x zip → `~/.local/firepaas-lab/bin`。
- `~/.local/firepaas-lab/env.sh` 统一 PATH。

### T4 hypeman 构建

- 在 `/home/zty/Learn/hypeman` 执行 `make build-linux`（嵌入式 CH v49/v51.1、
  Firecracker v1.14.2、Caddy v2.10.2、guest init/agent）。
- 产物复制到 `~/.local/firepaas-lab/bin/hypeman`，记录 `git rev-parse HEAD` 与
  `go version` 作为 provenance。

### T5 单机 Nomad/Consul 配置

- `scripts/lab/nomad-single.hcl`：`bootstrap_expect=1`、client `node_pool=compute`、
  raw_exec 开启、data_dir 指向用户目录。
- `scripts/lab/consul-single.hcl`：单节点 consul（M0 不强制启动，M1 服务发现用）。
- `scripts/lab/start.sh` / `stop.sh`：nohup 启停、日志落盘、幂等。
- `scripts/lab/README.md`：与 `bootstrap-lab.sh`（多机）的分工说明。

### T6 hypeman-p0 job 与 hypeman 配置

- `iac/nomad/hypeman-p0.hcl`：artifact 改为 `file:///home/zty/.local/firepaas-lab/bin/hypeman`，
  `HYPEMAN_PORT`、`CONFIG_PATH` 指向 `scripts/lab/hypeman-p0.yaml`；health 保持 `/health`；
  资源 memory 降到 768MB（微 VM 基准专用）；job 注释写明单机版，多机版由变量覆盖。
- `scripts/lab/hypeman-p0.yaml`：`hypervisor.default: firecracker`、无 ingress、
  `data_dir: /var/lib/firepaas-p0/hypeman`、网络 `10.100.0.0/16`。
- `scripts/lab/smoke-p0.sh`：pull/run/exec/logs/stop/delete + 残留检查骨架。

### T7 job 验证

用户态启动 Nomad 单节点 agent，执行 pool create + fmt/validate/plan；
plan 结果允许“raw_exec 需要 root”的放置说明，但 HCL 必须全部通过。

### T8 bench runner

- `scripts/bench-hypeman.sh` 实现：参数校验、固定样本数、JSON/CSV 原始样本、
  p50/p95 计算、进程/TAP/netns/磁盘残留检查、失败即非零退出。
- 冷启动/未缓存冷启动/standby-restore/density 四个子命令。

### T10 冒烟与基准（HITL）

操作者以 root 执行 start + job run + smoke；基准跑完整样本，原始样本保存到
`scripts/lab/results/`，汇总进 `docs/benchmarks.md`。

### T11 agent adapter spike（M0.4）

- 在 `agent/internal/machine` 做最小 adapter：直接 import
  `github.com/kernel/hypeman/lib/instances`（经 go.work），完成 Create/List/Delete
  的最小调用路径与测试桩；
- 输出 `docs/adr/` 或 `agent/internal/README.md` 的耦合点清单与“直接依赖 /
  fork / runtime core”三选一建议。

### T12 出口文档

- `docs/benchmarks.md`：环境、命令、样本、p50/p95。
- `docs/capacity-model.md`：单机实测可售容量、快照/镜像预算、Firecracker 版本
  pin 与 compatibility key。
- `docs/mvp-plan.md` §4 追加“单机版执行记录”，明确哪些出口在单机上降级验收。

## 执行记录（2026-08-25）

- T1–T8、T11 已完成；T9 Docker 依赖已用 `docker.m.daocloud.io` 镜像拉起。
- T10 完成：P0 job 部署/冒烟/kill -9 恢复/全套基准见 `docs/benchmarks.md`。
- 环境适配发现并落地：Nomad 2.0 `file://` artifact 不支持、`nomad fmt` 改名、
  `node pool apply` 语法、hypeman 需要 `erofs-utils`、Docker Hub 需镜像站补丁。
- 剩余：`DEFERRED-MULTI-NODE` 项（quorum/双节点/跨机快照）待第二台机器；
  镜像大小/allowlist/凭证边界待 M1.3 前冻结；hypeman 镜像站补丁需上游化。
