# 容量模型

> P0.5 冻结。公式 + 实测依据 + 调度器参数。

## 每节点可售容量

> 权威来源：agent 本机采集并上报的容量（`internal/agent/info/info.go`）与
> create/volume 硬准入；控制面调度器投影只是软决策，不得覆盖 agent 的真实
> 资源判定（ADR-0002）。下列公式即当前实现。

```text
可售 vcpu = min(host vcpu, cgroup cpu.max)        # agent 上报 CPUTotal
调度 CPU 上限 = 可售 vcpu × R（R=4，调度器超售比）
可售内存 = min(host MemTotal, cgroup memory.max) - 预留   # 内存不超售（MemR=1.0）
预留 = max(512MiB, 总量 × 1/32 ≈ 3%)   # Nomad/agentd/hypeman/页缓存安全余量

磁盘：requested 驱动 + 水位硬准入（ADR-0035，无固定系统/快照/镜像预算公式）
  scheduler 硬过滤：disk_allocated + disk_pending + disk_requested ≤ disk_total
  agent 最终防线：disk_used / disk_total ≥ FIREPAAS_ADMISSION_DISK_WATERMARK（默认 0.9）
```

单机首轮实测（2026-08-25，见 benchmarks.md）：
- 1vCPU/512MiB micro VM 密度达到 32 个后被**网络带宽准入**拦截（默认
  7.5MB/s/VM，bridge 有效上限 238.4MB/s），CPU/内存尚未触顶。
- 因此“可售容量”公式必须加入网络维度：`可售带宽 = bridge 有效上限 ×
  bandwidth_overcommit`，放置打分目前仍按 ADR-0002 只用 CPU/内存，但
  agent 硬准入需同时校验收带宽/磁盘。
- 本机为共享 k8s 节点，密度数据不用于生产容量承诺。

> ⚠️ ADR-0040 后网络路径已变更（eBPF 数据面 + WG mesh 东西向，bridge 已
> 删除）：上表及公式中的 bridge 上限口径不再对应现行数据面；eBPF/WG 路径
> 的吞吐与密度上限需重新标定（含 mesh 加密开销与 MTU 1280），标定前上述
> 数值仅作历史基线，不用于生产容量承诺。

## 磁盘水位与回收（ADR-0035）

- **create/volume 硬准入（权威）**：requested 驱动调度（scheduler 硬过滤
  `disk_allocated + disk_pending + disk_requested ≤ disk_total`），agent 以真实
  文件系统水位做最终防线：已用比 ≥ `FIREPAAS_ADMISSION_DISK_WATERMARK`（默认 0.9）
  时拒绝 create/volume（`ResourceExhausted`，控制面换节点重试）。
- **预取水位**：`PullImage` 是可让步优化，已用比 ≥ `FIREPAAS_PREFETCH_DISK_WATERMARK`
  （默认 0.9）时拒绝预取，不影响已有负载。
- **镜像缓存 GC / scrub**：由控制面 `FIREPAAS_LOCAL_GC_MODE`（默认 off）与
  `FIREPAAS_GC_LOW/HIGH_WATERMARK`（默认 0.70/0.85）配置，不再使用固定
  系统/快照/镜像预算公式；`FIREPAAS_SCRUB_*` 默认关闭。
- 节点本地数据不作跨节点持久承诺（ADR-0003 不变）。

- 初始参数（scheduler.DefaultBestOfKConfig）:R=4、MemR=1.0、DiskR=1.0、α=0.5、K=3、WeightImage=0.5
- 实测依据:见 benchmarks.md(单机首轮)
- 硬件选型建议:本机(Ryzen 7 8700G / 60GiB / KVM)仅作单机验证基准,
  生产 compute 节点需多机复测后才能给选型

## Firecracker 二进制、内核与兼容性(P0.5 冻结)

agentd 依赖 firecracker 二进制、内核与 guest rootfs 基件;分发与版本 pin 在 M0 决定(mvp-plan §4),不达标不得进入 M1:

| 项 | 决策 |
|---|---|
| 分发渠道 | 正式构建通过 `github.com/zhu327/hypeman v0.4.2-firepaas` Go module 消费嵌入式 runtime；Nomad raw_exec 执行已构建的 agentd。`build-hypeman.sh` 仅保留为历史 P0 复现工具，不是发布链路。 |
| 版本 pin | module/tag 和 `go.sum` 固定 hypeman；Firecracker compatibility key 由 agentd 从实际嵌入 runtime 检测，不再以本文历史版本常量上报。 |
| 升级路径 | 先 drain 节点，替换并校验 agent artifact，再恢复调度；实验室入口见 `scripts/lab/upgrade-agentd.sh`。 |
| snapshot compatibility key | 实际 Firecracker/runtime 版本 + kernel/rootfs/snapshot 格式 + CPU/KVM 特征；不兼容时禁止 restore，并回退到 digest-pinned image cold-start。 |
| 不兼容降级 | 禁止 restore，回退到 digest-pinned image cold-start |

## Host/runtime 容量与稳定性边界（M0 采样，M5 实测回填 2026-08-27）

| 项 | 基线/上限 | 告警与降级 |
|---|---|---|
| host NTP / guest resume clock drift | 实测 20 次 pause/resume 后 guest-host 偏差 -4~-6ms（远低于 10min 闸门）；FC snapshot 不保存 wall clock，长时间 standby 后偏差 ≈ 暂停时长，恢复后由 guest 内 NTP/chrony 补救 | > 10min 禁用 snapshot resume（e2e-m5 B 段断言）；runbook 建议 guest 内 chrony |
| entropy | 实测 256（满档，现代内核 getrandom 不阻塞） | < 128 告警（prometheus-alerts FirePaasEntropyLow） |
| host OOM / cgroup memory.events | agentd cgroup 16GiB；宿主可用内存告警阈 4GiB | < 4GiB 持续 5min 告警（FirePaasHostMemLow）；节点 drain |
| inode / file descriptor | 实验室 fs.file-max 巨大（不被约束）；采样基线 FD 10432，20 次 pause/resume 后无增长（零 FD 泄漏） | FD > max×80% 持续 5min 告警（FirePaasFDPressure）；高水位停止拉取/创建 |
| conntrack / TAP / netns 数量 | 实测基线 576，20 次 pause/resume 后 519（波动无增长）；上限 nf_conntrack_max=524288；每 VM 净占 ≈ 3-5 conntrack + 1 netns + 1 TAP + 2 veth | > max×70% 告警（FirePaasConntrackPressure critical）；高水位停止准入 |

实测样本：e2e-m5 B 段（20 循环 pause/resume + FD/conntrack/entropy 采样）
与 soak-m5 10 轮排练（fc/netns/machines 终态归零）；72h 正式浸泡待用户后台执行。

## OCI 镜像边界（M0 冻结中）

- 部署时解析 tag 并持久化 digest；machine 只运行 digest-pinned image。
- 受限网络基线:`HYPEMAN_DOCKER_HUB_MIRROR=docker.m.daocloud.io`(hypeman
  lab 分支补丁,仅重写 docker.io 网络访问,不改存储命名);生产环境使用
  registry allowlist,不依赖公共镜像站。
- registry allowlist、镜像压缩/解包限制和磁盘准入由当前 image policy、disk quota 与 inventory 实现共同约束；具体阈值以部署配置和 ADR-0035/0036/0037 为准，本文不复制易漂移的默认值。
- registry 使用短期 scoped credential 或 agent credential provider，不持久化长期密码。

## 规格示例

| 规格 | vcpu | mem | disk | 单节点上限(本机,共享 k8s) |
|---|---|---|---|---|
| micro | 1 | 512MiB | 5GiB(默认 10GiB) | 32(网络带宽准入上限) |
| small | 2 | 2GiB | 10GiB | (待填) |

## 生产发布与 Nomad 资源边界

生产 Nomad spec 不得使用 `latest`、`REPLACE-ME`、示例 registry 或无校验 artifact。
`iac/nomad/*.hcl` 要求调用方提供 digest-pinned 镜像、版本化 agent artifact 和 SHA-256；
漏传变量必须令 `nomad job validate/plan` 失败。API 的实际 health endpoint 是
`/v1/health`，edge 是其 HTTP 监听器上的 `/healthz`；不能按过期 `/health` 或不存在的
独立 health port 配置健康检查。

## 变更记录

- 2026-08-27：Task 3 统一 production agent job/service 名为 `firepaas-agentd`，并记录
  必填、digest/checksum 发布输入和真实 health/env 契约。

## M5 实测校准（2026-08-27，results/m5/）

20 次 pause/resume 循环（ontime 探针，slot 后端）：

| 采样点 | FD(file-nr) | conntrack | entropy | guest 时钟漂移 |
|---|---|---|---|---|
| 循环前 | 10592/9223372036854775807 | 725 | 256 | — |
| 循环后 | 10592/9223372036854775807 | 649 | 符合 | **-5ms**（4 次采样全一致）|

- 结论 1：FC snapshot 短 pause 不丢 wall clock；长 pause 建议 guest chrony
  （一次性 -5ms 级校准即可，属预防性建议）。
- 结论 2：pause/resume 无 FD/conntrack 漂移——M4.5 的恢复路径不占新资源面。
- 结论 3：镜像必须带发行版 init（scratch 不用），解包上限默认 4096MiB。
- 阈值进 `iac/observability/prometheus-alerts.yml` 与 docs/runbook-capacity.md。
