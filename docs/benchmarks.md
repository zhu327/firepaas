# 基准与验证记录

> 状态：M0 单机首轮实测完成（2026-08-25）。原始样本在
> `scripts/lab/results/<run-id>/`（raw.jsonl / raw.csv / meta.json），
> 全部由 `scripts/bench-hypeman.sh` 生成，未手工粘贴。
> 本机同时承载单节点 k8s 与桌面负载，数据为**参考值**；正式容量承诺需
> 多机复测（ADR-0012）。

## 硬件清单

| 节点 | 角色 | CPU | 内存 | 磁盘 | 网络 | KVM |
|---|---|---|---|---|---|---|
| node1 | 单机折叠(server+client+compute) | Ryzen 7 8700G 8C/16T | 60GiB(63373092 kB) | 771GB 可用 | 10G 内网, 与 k8s 共存 | /dev/kvm,SVM,nested=1 |

软件：Ubuntu 24.04.3、Go 1.25.4、Nomad 2.0.5、Consul 2.0.3、
hypeman `72440f5`（含 lab 补丁 `HYPEMAN_DOCKER_HUB_MIRROR`）、
Firecracker v1.14.2（hypeman 嵌入）、镜像 `docker.io/library/nginx:alpine`
（platform linux/amd64，digest `sha256:1f25fedd50aec27413031afb3a4f8ee4effcc9d843f6a76e81bfa92245ac5c06`）。

## P0 冒烟（`scripts/lab/smoke-p0.sh`）

- pull → run → exec(echo p0-smoke-ok) → logs → stop/delete：**PASS**
- 残留检查：无 `hype-*` TAP 残留、无 VM 进程残留；`firepaas0` 为服务级共享
  bridge，非残留；`cni-*` netns 属 k8s，排除在检查之外。

## 冷启动（镜像已缓存）

- 命令：`sudo bash scripts/bench-hypeman.sh cold 10`
- 样本：`scripts/lab/results/20260825t183850-cold/`
- 口径：POST /instances 起，至 `state == "Running"`（guest 就绪，可 exec）

| 指标 | n | p50 | p95 | min | max |
|---|---|---|---|---|---|
| cold_ms | 10 | 2166.0ms | 2169.7ms | 2162ms | 2171ms |

结论：**p95 2.17s < 5s，达标**。

## 未缓存冷启动

- 命令：`sudo bash scripts/bench-hypeman.sh uncached 3`（每轮先删镜像再拉取）
- 样本：`scripts/lab/results/20260825t184024-uncached/`
- 镜像走 `docker.m.daocloud.io`（`HYPEMAN_DOCKER_HUB_MIRROR`）

| 指标 | n | p50 | p95 | min | max |
|---|---|---|---|---|---|
| pull_ms | 3 | 5256.0ms | 5459.4ms | 5252ms | 5482ms |
| uncached_total_ms | 3 | 7423.0ms | 7622.8ms | 7416ms | 7645ms |

结论：**p95 7.6s < 60s，达标**（对 40MB 镜像；更大镜像需按体积另测）。

## standby / restore

- 命令：`sudo bash scripts/bench-hypeman.sh standby 10`
- 样本：`scripts/lab/results/20260825t183959-standby/`

| 指标 | n | p50 | p95 | min | max |
|---|---|---|---|---|---|
| standby_ms | 10 | 103.0ms | 322.9ms | 87ms | 497ms |
| restore_ms | 10 | 94.0ms | 95.0ms | 91ms | 95ms |

结论：restore p95 95ms < 1s，**snapshot 路径足以支撑 M4 scale-to-zero 的
性能前提**（稳定性与 50 次无泄漏循环留待 M4 验收）。首轮 standby 497ms 为
首次快照写盘，后续稳定在 ~100ms。

## warm fork

- 命令：`sudo bash scripts/bench-hypeman.sh fork 5`（from_running=true,
  target_state=Running）
- 样本：`scripts/lab/results/20260825t184547-fork/`

| 指标 | n | p50 | p95 | min | max |
|---|---|---|---|---|---|
| fork_ms | 5 | 325.0ms | 660.0ms | 243ms | 742ms |

结论：Firecracker fork 路径可用；首轮 742ms 为冷页表建立，后续 ~300ms。

## 单节点密度（1vCPU / 512MiB）

- 命令：`sudo bash scripts/bench-hypeman.sh density 40`
- 样本：`scripts/lab/results/20260825t184345-density/`

| 结果 | 值 |
|---|---|
| 成功创建并 Running | **32 / 40** |
| 第 33 个失败原因 | `insufficient_resources`:网络带宽准入——每 VM 默认 7.5MB/s，32×7.5=240MB/s 达到 bridge 有效上限 238.4MB/s（2.0x oversubscription） |

结论：32 个 micro VM 不是 CPU/内存上限，而是**默认网络带宽份额**上限。
单节点密度与可售容量模型需要把 `bandwidth_download/upload` 纳入公式；
降低每 VM 带宽份额后可继续向上测（下次补测）。

## Firecracker 专项

- 崩溃恢复：
  - `kill -9` Firecracker 进程 → 实例 state 变 `Unknown` 并带
    `state_error`（fc.sock connection refused）；DELETE 成功，无 `hype-*`
    TAP 残留。
  - `kill -9` hypeman API 进程 → Nomad raw_exec 按 restart 策略
    **6s 内恢复 /health**，alloc 记录 restart=1。
- snapshot compatibility key（本机）：Firecracker v1.14.2 + guest kernel
  ch-6.12.8-kernel-3.0-202605291 + snapshot format（hypeman 默认）+ CPU
  vendor AuthenticAMD / model 25 / SVM + host KVM 特性（`avic vgif x2avic`
  等）。跨机不兼容时按 ADR-0012 执行 cold-start 降级。
- pause/resume 时钟与熵：留待 M5 soak 专项（见 mvp-plan §9.2）。

## Host 极限与泄漏

- 每轮创建/删除后无 `hype-*` TAP 残留（smoke/bench 内置检查）。
- 密度 32 实例删除后残留检查通过。
- Host 与 k8s 共存警告：benchmark 期间 kubelet/temporal/GNOME 持续运行，
  密度与 p95 数值存在干扰；Nomad host_stats 对 `/var/lib/kubelet/pods/*`
  无权限读取的 WARN 属预期。

## 验收对照（mvp-plan §1.4）

| 指标 | 目标 | 实测(单机) | 状态 |
|---|---|---|---|
| 冷启动(缓存) p95 | <5s | 2.17s | ✅ |
| 冷启动(未缓存) p95 | <60s | 7.6s(40MB 镜像) | ✅ |
| snapshot resume p95 | <1s | 95ms | ✅ |
| 单节点 VM 数 | ≥64(4C/8G 规格) | 32(1vCPU/512MiB,网络准入上限) | ⚠️ 部分 |
| 控制面 API p95 | <150ms | 未建(无控制面) | N/A |
| 节点故障检测/重建 | <60s/<120s | 单机 kill -9 6s 恢复 | ⚠️ 单机参考 |

## 变更记录

- 2026-08-25：首轮实测完成（单机折叠实验室，ADR-0012）。
