# Runbook：节点替换（node replacement）

状态：**DEFERRED-MULTI-NODE** —— 单机实验室无法实务演练；本流程在生产双节点
上线后按顺序执行一次并回填实测时长。前置配置（keepalived 模板、node drain API）
已就位。

## 前提

- 集群 ≥2 计算节点；firepaas 节点状态在 `GET /v1/nodes` 可见。
- 控制面（firepaas-api）与数据面（agentd）已在节点上运行且快照 node-local
  （VM 快照留在原节点盘上——**节点替换 = 该节点上的 VM 数据丢失，冷启动重建**，
  这是 mvp-plan §9.5 明示的首版承诺：drain/rebuild，不承诺零中断）。
- 镜像已在 registry 全量预取（`hypeman` 镜像缓存，跨节点可拉）。

## 流程

1. **通知与观察**：确认目标节点无进行中的 rollout CUTOVER；`fpctl ops ls --status PENDING`
   清空在途操作。
2. **排水**：`curl -XPOST -H 'Authorization: Bearer <root>' /v1/nodes/{id}/drain`
   → scheduler 不再向该节点放置新 VM；已有 VM 继续服务。
3. **等待现有流量迁移**：执行 app 逐个 `fpctl app scale <id> N`（或 rollout 新
   deployment），把 replica 迁至健康节点；确认 drains 墙上无旧代流量
   （edge 响应头/`fpctl app status`）。
4. **下电**：`nomad job stop firepaas-agentd`（对应 alloc）；操作系统层面关机前
   执行 `nomad node drain` 兜底。
5. **换机/修机**：物理替换或重装（Ubuntu 24.04 + /dev/kvm + 60GiB 基线，
   见 docs/plans/2026-08-25-single-node-m0.md 的 root-setup）。
6. **回归**：`nomad node eligibility enable`；firepaas `POST /v1/nodes/{id}/ready`；
   等待 nodemanager 发现并置 READY。
7. **验收**：在该节点创建 1 个 app → 200；`GET /v1/nodes` 状态 READY；
   e2e 泄漏零漂移。

## 回滚

节点故障而流程未完成 → 保持 DRAINING 并依赖反亲和/其他节点承接（尽力而为，
见 ADR-0009）；控制面 leader 选举与 Redis/PG 不受影响。
