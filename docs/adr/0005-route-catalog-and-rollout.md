# ADR-0005：按 hostname 发布版本化 route backend set

状态：已接受（2026-08-25）  
依据：多副本、滚动发布与 edge 路由评审

## 决策

1. 外部流量不直接查询 `machine_id`；edge 先以 `hostname + port` 查询 route catalog。
2. 一个 route 指向带 generation 的 backend set；每个 backend 至少包含 `machine_id`、`execution_id`、node/proxy 地址、app port、readiness、weight、draining。
3. PostgreSQL 持有 active route generation 和 backend 生命周期；Redis 存放可重建的 edge 查询投影。
4. 控制器只在新副本被 agent 观测为运行且通过 readiness 后，才原子发布新的 route generation。
5. 发布后旧 backend 进入 draining；排空期限后才停止旧 execution。新副本失败则不切换，或回滚到上一 generation。

## 理由

`machine:catalog:{machine_id}` 只能定位一台 VM，不能从 `<app>.<domain>` 找到多副本，也不能表达新旧 deployment 同时存在时的切流和回滚。

## 后果

- MVP 只实现 HTTP/TCP 的简单 round-robin；会话粘性、灰度权重和长连接迁移延后。
- edge 对 route generation 做短缓存，但必须在 backend 的 execution/location 不一致时拒绝转发并刷新。
- catalog miss 不能使 edge 猜测 backend；paused 场景仅在 route 明确指向可恢复的单/组副本时调用控制面恢复接口。
- 发布与 scale/节点故障/再次发布的组合场景决策表在 M3 冻结；MVP 至少实现同一 app 同时只允许一个 rollout 的互斥。
