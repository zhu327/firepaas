# ADR-0012:M0 单机实验室基线(3+2 标准实验室的降级执行)

状态:已接受(2026-08-25)
依据:`docs/mvp-plan.md` §4 与单机硬件约束(唯一可用物理机:
Ryzen 7 8700G / 8C16T / 60GiB / KVM,同时承载单节点 k8s 与桌面负载)。

## 决策

M0 数据面验证以**单机折叠实验室**为基线执行:

```text
1 台物理机 = 1 个 Nomad server+client(bootstrap_expect=1,node_pool=compute)
           + 1 个 Consul(M0 不强制启动)
           + 本机 KVM(Firecracker,由 hypeman 管理)
           + Docker 承载 PG/Redis/MinIO/registry
```

- 工具链全部安装到 `~/.local/firepaas-lab`,不改 `/etc`、不动系统
  sysctl、不启巨页、不装 systemd 系统服务;runtime 数据(root)在
  `/var/lib/firepaas-p0`。
- `scripts/bootstrap-lab.sh` 保留为**多机标准实验室**入口;新增
  `scripts/lab/` 作为单机入口,两者不得混用。
- 单机只创建 `compute` 节点池;`control` 池与 control-plane/edge 的 Nomad
  service job 推迟到 M1,单机方案届时另定(进程直跑或放宽到 compute 池)。

## 理由

1. 唯一可用硬件无法同时提供 3 server + 2 compute;用嵌套虚拟机硬凑 5 节点,
   benchmark 数据(尤其是 Firecracker 冷启动/密度)不具备代表性,且 60GiB
   内存无法给出 2 个有意义的 compute 配额。
2. M0 的核心 go/no-go 是**数据面是否成立**(OCI → Firecracker VM →
   exec/logs → snapshot/restore),这部分在单机上可以完整验证。
3. 用户态工具链 + root-only runtime 的最小化设计,避免与现网 k8s 节点互相
   破坏;单机实验室可随时拆除。

## 单机结论的外推边界(重要)

以下 M0 出口在单机上**不完整或不可验证**,不得外推:

| 原 M0 出口 | 单机状态 |
|---|---|
| 3 server quorum / server 故障转移 | 不验证 |
| 两 compute 节点分别冒烟、重启与 kill -9 恢复 | 只验单节点,`bootstrap_expect=3` 相关结论无效 |
| 双节点密度/放置/反亲和 | 不验证 |
| 跨节点 snapshot compatibility | 只记录本机 key,不验证不兼容降级路径 |
| 基准与 k8s/桌面共存 | 数据为参考值;正式容量承诺需多机复测 |

标准 3+2 实验室仍是 M0 go/no-go 的**完整形态**;在获得第二台机器前,M0 的
数据面类出口以单机结果先行决策,集群类出口标记为 `DEFERRED-MULTI-NODE`。

## 后果

- `iac/nomad/hypeman-p0.hcl` 的 artifact/config 使用单机真实路径;多机复用时
  以变量覆盖,不删除单机默认值(见 job 注释)。
- `scripts/bench-hypeman.sh` 的密度基准必须记录“与 k8s 共存”的干扰声明。
- M1 计划需先给出单机 control-plane/edge 部署形态(进程直跑 or job 放宽),
  再冻结 M1 工程基线。
- 获得第二台物理机后,按 `scripts/bootstrap-lab.sh` 重建 3+2 标准实验室,
  并对 `DEFERRED-MULTI-NODE` 出口补测。
