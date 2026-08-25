# 容量模型

> P0.5 冻结。公式 + 实测依据 + 调度器参数。

## 每节点可售容量

```
可售 vcpu = 物理 vcpu × cpu_overcommit(R)
可售内存 = (总内存 - 系统/hugepage 预留) × mem_overcommit
预留 = max(4GiB, 16% 内存)  # 参考 e2b start-client.sh 的巨页策略
可售磁盘 = 总磁盘 - 系统预留 - 快照预算 - 镜像缓存预算
快照预算(P0 测定单副本 standby 快照体积 S 后更新)
  = max(节点 mem 可售上限 × 0.5, 64GiB)   # 全节点副本同时 standby 的最坏情形按半内存估
镜像缓存预算 = min(总磁盘 × 30%, 100GiB) # LRU 驱逐的配额上限
```

## 磁盘水位与回收(M3 依赖,agent 守护职责)

- **镜像缓存 GC**:节点磁盘使用率 ≥ 70% 触发 LRU 驱逐(从未被任何在用 machine 引用
  的镜像开始);≥ 85% 拒绝新拉取并上报 `InfoService` 降权标签;缓存占用不超过上表预算。
- **快照预算守门**:standby 前检查剩余快照预算,不足则拒绝 Pause 并返回明确错误
  (控制面转为 cold-start 路径);快照体积随 `Machine` observed state 上报,计入对账。
- 节点本地数据不作跨节点持久承诺(ADR-0003 不变),磁盘预算只服务本节点可用性。

- 初始参数:R=4、α=0.5、K=3、mem_overcommit=1.0
- 实测依据:见 benchmarks.md(待填)
- 硬件选型建议:(待填)

## Firecracker 二进制、内核与兼容性(P0.5 冻结)

agentd 依赖 firecracker 二进制、内核与 guest rootfs 基件;分发与版本 pin 在 M0 决定(mvp-plan §4),不达标不得进入 M1:

| 项 | 决策 |
|---|---|
| 分发渠道 | (待定:随 agent artifact / 节点包管理 / 对象存储) |
| 版本 pin | (待填:firecracker 版本、内核版本、rootfs 基件) |
| 升级路径 | (待填:与 agent drain/rebuild 升级的先后关系) |
| snapshot compatibility key | (待填:Firecracker + guest kernel + snapshot format + CPU vendor/model + host KVM features) |
| 不兼容降级 | 禁止 restore，回退到 digest-pinned image cold-start |

## Host/runtime 容量与稳定性边界（M0 采样，M5 soak 冻结）

| 项 | 基线/上限 | 告警与降级 |
|---|---|---|
| host NTP / guest resume clock drift | (待填) | 超阈值禁用 snapshot resume |
| entropy | (待填) | guest 启动超时并告警 |
| host OOM / cgroup memory.events | (待填) | 节点停止准入并 drain |
| inode / file descriptor | (待填) | 高水位停止拉取/创建 |
| conntrack / TAP / netns 数量 | (待填) | 高水位停止准入并回收 |

## OCI 镜像边界（M0 冻结）

- 部署时解析 tag 并持久化 digest；machine 只运行 digest-pinned image。
- registry allowlist、最大镜像压缩体积、最大解包体积与磁盘预检查：（待填）。
- registry 使用短期 scoped credential 或 agent credential provider，不持久化长期密码。

## 规格示例

| 规格 | vcpu | mem | disk | 单节点上限(64G 节点) |
|---|---|---|---|---|
| micro | 1 | 512MiB | 5GiB | (待填) |
| small | 2 | 2GiB | 10GiB | (待填) |

## 变更记录

- (冻结日期 + 依据)
