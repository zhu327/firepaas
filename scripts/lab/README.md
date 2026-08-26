# firepaas 单机实验室（M0）

> 状态：M0 单机基线（ADR-0012）。与 `scripts/bootstrap-lab.sh`（3 server + 2 compute
> 标准实验室）互斥，不得混用。所有工具装在 `/home/zty/.local/firepaas-lab`，
> 不改 `/etc`、不写系统 sysctl、不启巨页，避免影响本机已有 k8s。

## 目录

```
scripts/lab/
├── README.md           # 本文件
├── env.sh              # PATH / NOMAD_ADDR 等环境变量
├── nomad-single.hcl    # 单节点 Nomad：bootstrap_expect=1, client node_pool=compute
├── consul-single.hcl   # 单节点 Consul（M0 不强制启动，M1 服务发现用）
├── hypeman-p0.yaml     # hypeman P0 配置：firecracker、无 ingress
├── start.sh            # 启动 Nomad（可选 Consul）+ 建 compute 池
├── stop.sh             # 幂等停止
├── status.sh           # 状态检查
├── build-hypeman.sh    # 构建 hypeman/CLI/token 工具（用户态，无 make/gcc 也可）
├── root-setup.sh       # root 一次性准备（数据目录/docker/kvm 组），sudo 执行
├── run-p0.sh           # root 部署 P0 job 并等 /health，sudo 执行
├── smoke-p0.sh         # P0 冒烟：pull/run/exec/logs/stop/delete + 残留检查
└── results/            # 基准原始样本（JSON/CSV，gitignore 之外人工保留）
```

## 快速开始

```bash
# 0. 环境变量（每次新终端）
source scripts/lab/env.sh

# 1. 启动单节点 Nomad（用户态即可；job 实际运行需要 root，见下）
bash scripts/lab/start.sh

# 2. root 一次性准备（数据目录、docker/kvm 组）
sudo bash scripts/lab/root-setup.sh

# 3. 校验 P0 job
nomad fmt -check iac/nomad/hypeman-p0.hcl
nomad job validate iac/nomad/hypeman-p0.hcl
nomad job plan iac/nomad/hypeman-p0.hcl

# 4. root 部署 P0 job（raw_exec + KVM/TAP 需要 root）
sudo bash scripts/lab/run-p0.sh

# 5. 冒烟 + 基准
sudo bash scripts/lab/smoke-p0.sh
sudo bash scripts/bench-hypeman.sh cold 10
sudo bash scripts/bench-hypeman.sh standby 10
sudo bash scripts/bench-hypeman.sh fork 5
sudo bash scripts/bench-hypeman.sh uncached 3
sudo bash scripts/bench-hypeman.sh density 16
```

## root 与当前用户说明

- Nomad agent 可以用普通用户跑（本目录启动脚本即如此），但 `raw_exec` driver 的
  任务在非 root 客户端上会被拒绝启动；因此 **job run 与冒烟/基准必须 root**。
- `root-setup.sh` 会把 Nomad 切换到 root 运行（data_dir `/var/lib/firepaas-p0/nomad`），
  用户仍可用 `NOMAD_ADDR=http://127.0.0.1:4646` 操作集群。
- hypeman 需要 `/dev/kvm`（root:kvm 660）与 CAP_NET_ADMIN（bridge/TAP/iptables）。
- root 跑 job 前请确认执行路径与 config 路径对 root 可读：
  `/home/zty/.local/firepaas-lab/bin/hypeman` 与
  `/home/zty/Learn/firepaas/scripts/lab/hypeman-p0.yaml`。
- hypeman 的 `data_dir` 是 `/var/lib/firepaas-p0/hypeman`（root 所有）。

## M1（当前开发）

M0 数据面验证已通过；M1 起 agentd 替代 hypeman-p0 job：

```bash
sudo bash scripts/lab/run-agentd.sh   # 部署 firepaas-agentd（system job，root）
agentctl info                          # gRPC ServiceInfo
agentctl create -machine-id m1 -execution e1 -operation op1
agentctl list
agentctl delete -machine-id m1 -execution e1 -operation op2
```

`firepaas-agentd` 与 `firepaas-hypeman-p0` 互斥（共享 data_dir），需要回退 P0 时先
`nomad job stop firepaas-agentd` 再运行 `scripts/lab/run-p0.sh`。

## 受限网络（Docker Hub 被劫持/超时）

- Docker 镜像经 `docker.m.daocloud.io` 拉取后 retag 为官方名再 compose up。
- hypeman 直连 `index.docker.io` 拉 alpine initrd 基件，本机需给 P0 job 设置
  `HYPEMAN_DOCKER_HUB_MIRROR=docker.m.daocloud.io`（已在 hypeman-p0.hcl 中默认配置；
  补丁在 hypeman 的 `lab/docker-hub-mirror-env` 分支，见 capacity-model.md）。

## 基准结果

首轮结果与原始样本：`docs/benchmarks.md`、`scripts/lab/results/`（meta.json +
raw.csv/raw.jsonl）。冷启动 p95 2.17s、restore p95 95ms、未缓存 p95 7.6s、
fork p95 660ms、micro 密度 32（网络带宽准入上限）。

## 端口

| 组件 | 端口 |
|---|---|
| Nomad HTTP/RPC/Serf | 4646 / 4647 / 4648 |
| Consul DNS/HTTP/RPC/Serf | 8600 / 8500 / 8300-8302 |
| hypeman API | 4973 |
| Docker dev deps | 5432 / 6379 / 9000-9001 / 5000 |

均已确认与本机现有 k8s/桌面服务不冲突。
