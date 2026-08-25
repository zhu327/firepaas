# 基准与验证记录

> P0.1-P0.4 填充。每项必须由 `scripts/bench-hypeman.sh` 可重复执行，保留原始 JSON/CSV；格式包含环境、完整命令、样本数、p50/p95、错误率与结论。手工粘贴一次结果不满足 M0 出口。

## 硬件清单

| 节点 | 角色 | CPU | 内存 | 磁盘 | 网络 | KVM |
|---|---|---|---|---|---|---|
| (待填) | | | | | | |

## 冷启动(镜像已缓存)

- 命令 / 样本数 / p50 / p95 / 结论:(待填)

## standby / restore

- (待填)

## warm fork

- (待填)

## 单节点密度

- (待填)

## 镜像首次拉取

- (待填)

## Firecracker 专项

- fork 网络 override / agent 重启回收 / kill -9 残留:(待填)
- snapshot compatibility key 与跨节点不兼容时 cold-start 降级:(待填)
- pause/resume 后 guest clock drift 与 entropy:(待填)

## Host 极限与泄漏

- OOM/cgroup memory.events、inode/FD、conntrack、TAP/netns 上限:(待填)
- 每轮创建/删除后的进程、TAP、netns、磁盘增量检查:(待填)

## 验收对照(mvp-plan.md 1.4)

| 指标 | 目标 | 实测 | 状态 |
|---|---|---|---|
| 冷启动(缓存)p95 | <5s | | |
| restore p95 | <1s | | |
| ... | | | |
