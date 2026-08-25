# 容量模型

> P0.5 冻结。公式 + 实测依据 + 调度器参数。

## 每节点可售容量

```text
可售 vcpu = 物理 vcpu × cpu_overcommit(R)
可售内存 = (总内存 - 系统/hugepage 预留) × mem_overcommit
预留 = max(4GiB, 16% 内存)  # 参考 e2b start-client.sh 的巨页策略
可售磁盘 = 总磁盘 - 系统预留 - 快照预算 - 镜像缓存预算
快照预算(P0 测定单副本 standby 快照体积 S 后更新)
  = max(节点 mem 可售上限 × 0.5, 64GiB)   # 全节点副本同时 standby 的最坏情形按半内存估
镜像缓存预算 = min(总磁盘 × 30%, 100GiB) # LRU 驱逐的配额上限
```

单机首轮实测（2026-08-25，见 benchmarks.md）：
- 1vCPU/512MiB micro VM 密度达到 32 个后被**网络带宽准入**拦截（默认
  7.5MB/s/VM，bridge 有效上限 238.4MB/s），CPU/内存尚未触顶。
- 因此“可售容量”公式必须加入网络维度：`可售带宽 = bridge 有效上限 ×
  bandwidth_overcommit`，放置打分目前仍按 ADR-0002 只用 CPU/内存，但
  agent 硬准入需同时校验收带宽/磁盘。
- 本机为共享 k8s 节点，密度数据不用于生产容量承诺。

## 磁盘水位与回收(M3 依赖,agent 守护职责)

- **镜像缓存 GC**:节点磁盘使用率 ≥ 70% 触发 LRU 驱逐(从未被任何在用 machine 引用
  的镜像开始);≥ 85% 拒绝新拉取并上报 `InfoService` 降权标签;缓存占用不超过上表预算。
- **快照预算守门**:standby 前检查剩余快照预算,不足则拒绝 Pause 并返回明确错误
  (控制面转为 cold-start 路径);快照体积随 `Machine` observed state 上报,计入对账。
- 节点本地数据不作跨节点持久承诺(ADR-0003 不变),磁盘预算只服务本节点可用性。

- 初始参数:R=4、α=0.5、K=3、mem_overcommit=1.0
- 实测依据:见 benchmarks.md(单机首轮)
- 硬件选型建议:本机(Ryzen 7 8700G / 60GiB / KVM)仅作单机验证基准,
  生产 compute 节点需多机复测后才能给选型

## Firecracker 二进制、内核与兼容性(P0.5 冻结)

agentd 依赖 firecracker 二进制、内核与 guest rootfs 基件;分发与版本 pin 在 M0 决定(mvp-plan §4),不达标不得进入 M1:

| 项 | 决策 |
|---|---|
| 分发渠道 | 单机实验室:构建产物经 Nomad raw_exec 绝对路径执行(`scripts/lab/build-hypeman.sh`);多机形态:http(s) artifact + checksum(待实现 `hypeman-p0-remote.hcl`) |
| 版本 pin | hypeman git `72440f5`(含 lab 镜像站补丁);Firecracker v1.14.2(嵌入);CH v49.0/v51.1(嵌入);Caddy v2.10.2(嵌入) |
| 升级路径 | (待填:与 agent drain/rebuild 升级的先后关系) |
| snapshot compatibility key | Firecracker v1.14.2 + ch-6.12.8-kernel-3.0-202605291 + hypeman 默认 snapshot 格式 + CPU vendor AuthenticAMD/model 25/SVM + host KVM 特性;不兼容时 cold-start 降级 |
| 不兼容降级 | 禁止 restore，回退到 digest-pinned image cold-start |

## Host/runtime 容量与稳定性边界（M0 采样，M5 soak 冻结）

| 项 | 基线/上限 | 告警与降级 |
|---|---|---|
| host NTP / guest resume clock drift | (待填) | 超阈值禁用 snapshot resume |
| entropy | (待填) | guest 启动超时并告警 |
| host OOM / cgroup memory.events | (待填) | 节点停止准入并 drain |
| inode / file descriptor | (待填) | 高水位停止拉取/创建 |
| conntrack / TAP / netns 数量 | (待填) | 高水位停止准入并回收 |

## OCI 镜像边界（M0 冻结中）

- 部署时解析 tag 并持久化 digest；machine 只运行 digest-pinned image。
- 受限网络基线:`HYPEMAN_DOCKER_HUB_MIRROR=docker.m.daocloud.io`(hypeman
  lab 分支补丁,仅重写 docker.io 网络访问,不改存储命名);生产环境使用
  registry allowlist,不依赖公共镜像站。
- registry allowlist、最大镜像压缩体积、最大解包体积与磁盘预检查:（待填）。
- registry 使用短期 scoped credential 或 agent credential provider，不持久化长期密码。

## 规格示例

| 规格 | vcpu | mem | disk | 单节点上限(本机,共享 k8s) |
|---|---|---|---|---|
| micro | 1 | 512MiB | 5GiB(默认 10GiB) | 32(网络带宽准入上限) |
| small | 2 | 2GiB | 10GiB | (待填) |

## 变更记录

- (冻结日期 + 依据)
