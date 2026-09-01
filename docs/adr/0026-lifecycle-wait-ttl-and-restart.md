# ADR-0026：Lifecycle wait、TTL 与 restart 的控制面权威

状态：已接受（v1.2，2026-08-31）

Review 修订：TTL 删除采用可恢复的两阶段顺序（先持久化 route detached，再派发
fenced delete）；restart backoff 绑定失败 execution，换代与 attempt 记账由 PG
CAS/事务保护。迁移修复必须追加，不能重写已发布 migration。单节点 smoke 只覆盖
wait/TTL；跨节点 restart、leader handover 与崩溃注入仍需独立验收。
补充：ADR-0003、ADR-0015。

## 背景

MachineSpec 已预留 TTL/restart 字段，但尚无完整控制面语义。若同时启用 hypeman 本地 reaper/restart 和 firepaas reconcile，会出现双重操作、绕过 route/配额和旧 execution 复活。

## 决策

1. PG/control-plane 是 TTL 和 restart policy 的唯一权威；不启用 hypeman 本地 TTL reaper 或 restart controller。
2. TTL 持久化为绝对 `expires_at`。到期先摘 route，再创建 fenced delete operation；controller 停机不改变截止时间。
3. restart 支持 `NEVER`、`ON_FAILURE`、`ALWAYS`、`max_attempts`、固定 backoff、stable window。每次重启创建新 execution 并递增 generation。
4. `ON_FAILURE` 的权威 exit class 来自 execution-bound agent report；node/agent 失联仍由既有故障重建处理，不消耗 restart attempts。rollout create retry 与 restart attempts 分开。
5. manual stop/delete、TTL expiry 不触发 restart；已删除或过期 machine 永不复活。
6. 唯一 owner 决策表：active rollout 负责 target replica；active evacuate 负责 source replacement 且计划性 delete 不消耗 attempt；TTL/manual delete 禁止 restart；仅其余意外退出进入 restart。
7. attempts 按 logical machine 持久化；stable window 从新 execution READY 开始，pause 不重置，连续 READY 满窗口后清零。超过上限进入 `RESTART_BLOCKED`，app controller 不补建该 ordinal，直到新 deployment 或管理员 reset。
8. restart operation idempotency key 包含 machine、failed execution 和 attempt ordinal，并重新经过 quota、placement、agent admission、readiness 和 one-shot secret delivery。
9. wait API 读取 PG 权威状态；进程通知或 `LISTEN/NOTIFY` 只加速，有限轮询兜底。最大等待 5 分钟，客户端断开不取消业务 operation。
10. wait 必须绑定明确目标 revision：operation terminal、machine execution X ready 或 rollout generation Y terminal。generation/execution 被替换时返回 superseded，不能把旧代到达目标当成功；三类资源分别冻结 terminal state 集合。

## 理由

集群级生命周期必须与路由、配额和发布状态机一致，节点本地自动重启只能看到局部事实。

## 后果

- 需要持久化 attempts、next_attempt、stable_since、blocked_reason 和 expires_at；
- 修订 ADR-0015 的组合决策表；
- leader handover、ACK loss、Redis loss 下必须证明不双重执行；
- 第一版不提供指数退避、cron 或后台 job 类型。
