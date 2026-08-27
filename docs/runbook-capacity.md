# Runbook：容量与告警阈值（capacity）

对应告警规则 `iac/observability/prometheus-alerts.yml`；调整阈值时两处同步。
实测基线来自 2026-08-27 e2e-m5（scripts/lab/results/m5/e2e-m5-run.log）。

## 阈值表（单机 16C Ryzen 7 8700G / 60GiB）

| 指标（/metrics） | 告警 | 处置 |
|---|---|---|
| `firepaas_host_mem_available_kb` | < 4GiB 持续 5m | 回收 VM（scale→0/apps 缩减）；禁止新建；见 OOM 小节 |
| `firepaas_host_fds_allocated/max` | > 0.8 | 排查 firecracker TAP/socket 泄漏；slot/session 泄漏用 e2e F 段计数法 |
| `firepaas_host_conntrack_count/max` | > 0.7 (critical) | 检查 ingress 连接风暴；停突发方 |
| `firepaas_host_entropy_avail` | < 128 | 实测基线 256；安装 haveged（生产）或确认 jitterentropy |
| `firepaas_host_load1_x100` | > 1600 (load1 16) 10m | 检查超售：scheduler 硬准入是否被绕过；抓取 top firecracker |
| `firepaas_operations_pending` | > 20 5m | controller/agent 失速处置链：/v1/nodes → agentd alloc 日志 → nomad job restart |
| `firepaas_nodes_unhealthy` | > 0 2m | 节点替换 runbook |

## OOM 历史与护栏

- **M3 教训**：Nomad task cgroup 1GiB 曾将同 cgroup 的 firecracker 成批 OOM 杀。
  已改为 16GiB（iac/nomad/agentd-single.hcl）+ admission
  `min(host 可用, cgroup 上限)`，禁用内存不可另行放宽。
- 宿主宿留：GNOME 桌面占用按 4–8GiB 计；VM 承诺总量不得超过
  `host_mem_available_kb - 8GiB`。

## 资源面（per-VM）

| 对象 | 数量级 | 上限来源 |
|---|---|---|
| FD | ~每 VM 8–12（TAP/fc.sock/vsock/日志）| `/proc/sys/fs/file-nr` |
| netns(ip netns) | 每 slot 1 | `e2e-* F 段` 计数需为 0 |
| TAP | 每 VM 1 | `ip link` 中 `hype-*` 泄漏可见 |
| conntrack | 每活跃连接 1 | `nf_conntrack_max`（实测基线 649 于 20 循环）|

## 时钟与镜像要求（M5 实测结论）

- guest 时钟：FC snapshot 恢复在本实验节奏（秒级 pause）漂移 **-5ms**；
  长 pause（分钟→小时）仍建议 guest 内 chrony 一次性校准（runbook 注入）。
- **镜像最低要求**：必须带发行版基础 init（alpine/nginx 类正常）；
  `FROM scratch` 自制镜像不写 hypeman boot marker，会停在 Initializing。
- 镜像准入三层：`FIREPAAS_IMAGE_REQUIRE_DIGEST`（API）、
  `FIREPAAS_REGISTRY_ALLOWLIST`（API）、`FIREPAAS_IMAGE_MAX_UNPACK_MIB`
  （agent，默认 4096，超限永久拒绝 → 换镜像/调阈值）。
