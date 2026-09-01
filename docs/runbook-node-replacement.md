# Runbook：节点替换（node replacement）

状态：**单机实验室已演练 evacuate 语义**（e2e-v11 D 段：machine 归零 + 事件 +
ready 后恢复）；**多节点零停机迁移验收 DEFERRED-MULTI-NODE**——生产双节点
上线后按顺序执行一次并回填实测时长。前置配置（keepalived 模板、node drain API）
已就位。

## 前提

- 集群 ≥2 计算节点；firepaas 节点状态在 `GET /v1/nodes` 可见。
- 控制面（firepaas-api）与数据面（agentd）已在节点上运行。
- v1.1（ADR-0021）起，节点维护标准路径是 **drain+evacuate**：controller 逐
  实例驱离（删除→换代重建到其它节点→READY→切流），节点 machine 归零后才动
  agentd/Nomad job——消除"agentd SIGTERM 带走全部 VM"的停机窗口（M3 已知行为）。
- VM 快照仍是 node-local（architecture §7：仅作加速）；evacuate 重建走冷启动
  （已缓存镜像 p95 <5s，ADR-0018 亲和加速重建落点）。

## 流程（v1.1 evacuate 形态）

1. **通知与观察**：确认目标节点上的 app 无进行中 rollout（active rollout 的
   machine 会被 evacuate 跳过、下轮重试；不与发布并发编排）；
   `fpctl ops ls --status PENDING` 清空在途操作。
2. **排水 + 驱离**：
   ```bash
   curl -XPOST -H 'Authorization: Bearer <root>' -H 'Content-Type: application/json' \
     -d '{"evacuate": true}' /v1/nodes/{id}/drain
   ```
   - `evacuate=false`（或不带 body）= M5.5 兼容语义：只停新放置；
   - `evacuate=true`：controller 按 ordinal 序逐台迁移（concurrency=1），
     每台：删除当前实例 → R3 换代重建（调度天然避开 draining 节点）→
     新代 READY 且路由切流 → 下一台。standby 实例直接重建（不先唤醒）。
3. **等待归零**：`GET /v1/nodes/{id}` + machine 列表推导进度（无专用进度 API，
     ADR-0021 决策）；`scheduler_events` 出现 `evacuate_complete` 即节点
     machine 归零，可安全维护。
4. **维护/升级**：`nomad job restart firepaas-agentd`（或 stop）；此时节点上
   无 VM 可被 SIGTERM 带走。`scripts/lab/upgrade-agentd.sh` 是该流程的完整
   演练（build → drain+evacuate → 等归零 → restart → ready → 对账）。
5. **回归**：`POST /v1/nodes/{id}/ready`（同时清除 evacuate 标记）；
   nodemanager 发现节点恢复 HEALTHY 后，被驱离的 machine 由 R1–R8 对账重建
   回该节点（若其余节点无容量/亲和更优）。
6. **验收**：在该节点创建 1 个 app → 200；`GET /v1/nodes` 状态 READY；
   e2e 泄漏零漂移。

## 边界（ADR-0021 §4）

- **单副本 app**：evacuate 的删除→新代 READY 窗口内存在短暂不可用（与 R4
  节点失联重建同量级）。**建议副本 ≥2 时执行 evacuate**；单副本场景接受
  短暂不可用或改用发布窗口。
- evacuate 期间节点失联（R4 抢跑）以 R4 为准（先到先得，换代幂等）。
- 不做并发多节点 evacuate、不做 evacuate 进度 API（进度从节点列表 + machine
  列表推导）。

## 回滚

节点故障而流程未完成 → 保持 DRAINING 并依赖反亲和/其他节点承接（尽力而为，
见 ADR-0009）；控制面 leader 选举与 Redis/PG 不受影响。evacuate 中断后
重启 controller 会幂等续跑（驱离进度由节点剩余 machine 数自然推导）。
