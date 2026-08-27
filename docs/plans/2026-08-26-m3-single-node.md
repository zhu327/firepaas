# M3 单机执行计划：slot 网络、多副本、滚动发布与路由（2026-08-26）

> 依据：mvp-plan §7、ADR-0004（L3 隔离）、ADR-0005（route catalog/rollout）、
> ADR-0008（readiness）、ADR-0009（放置约束）、ADR-0011（DNS 轮询入口）、
> ADR-0015（M3 发布状态机冻结）。与 M0/M1/M2 同一模式：单机折叠实验基线
> （ADR-0012），多节点项标 `DEFERRED-MULTI-NODE`。

## 范围（单机折叠）

| mvp-plan §7 工作项 | 单机实验形态 |
|---|---|
| 1. slot manager（netns/veth/tap/nftables） | 完整实现（agent 本机能力，不受单机限制）；真机 spike 已通过（root→guest / egress NAT×2 / 私网+跨 slot 隔离） |
| 2. app/deployment/replica controller、scale N | 完整实现；稳定 ordinal = `app-r{0..N-1}` |
| 3. route backend set + readiness + generation 发布 + drain + rollback | 完整实现（ADR-0015 状态机）；单 rollout 互斥 |
| 4. app API/最小 CLI | REST 完整；CLI 只做 `fpctl app create/deploy/scale/status`（logs/exec 明确延后） |
| 5. 镜像 digest/节点缓存/registry | 本机只做：API 校验 digest 引用、registry allowlist 环境变量、镜像可用性纳入 placement 亲和评分输入（不加权重）；LRU/预热/共享 registry 部署物 DEFERRED（单节点无跨节点缓存语义） |
| 6. 放置约束落地（ADR-0009） | M2 已实现硬过滤+Best-of-K+反亲和尽力；M3 补 label 断言与 U3 验收（单节点=候选不足降级+事件，反亲和 distinct 由 sim 覆盖） |
| 7. 入口 DNS 轮询（ADR-0011） | `*.firepaas.local → 127.0.0.1`（客户端侧一次性配置，不写 /etc）；TLS 仍 M4 |
| 8. 无状态节点故障重建 + SLO 观测 | 复用 M2 R1-R8 决策表 + chaos-m2 形态；分段 SLO 观测延后 |

## 切片

- **M3.1 slot 网络**：`internal/agent/network/slot`——netns/veth/bridge/tap/nftables
  原子管理、slot idx 池化分配、启动回收（`slots.json` 状态文件 + Reconcile）、
  桥接模式灰度开关 `FIREPAAS_NETWORK_BACKEND=bridge|slot`（默认 bridge，
  实验室 agentd-single.hcl 置 slot）。attach 时点：hypeman CreateInstance
  返回后把其 TAP 移入 slot netns；release 随 DeleteInstance。真机 spike 结论：
  hypeman TAP 名 = `hype-{id[:8]}`（lib/network.GenerateTAPName），derive 只读
  metadata，TAP 不在 root ns 不影响状态推导与 release（best-effort）。
- **M3.2 readiness 探针（ADR-0008）**：`internal/agent/health`——tagHealth 存完整
  探针策略 JSON，agent host 侧经 workload endpoint（bridge guest IP / slot 路由）
  执行 HTTP/TCP 探针，复用 hypeman lib/healthcheck 的 policy/threshold 语义，
  `Machine.readiness` READY/NOT_READY/UNKNOWN/UNCONFIGURED 闭环进 controller
  `route_backends.readiness`。
- **M3.3 apps/deployments + app controller + API**：migration 0006（deployments/
  rollouts 表 + machines.deployment 外键语义不变）；AppController 对账
  `desired_replicas`（缺失 ordinal 下单 create、多余 ordinal 下单 delete、
  UNIQUE(app_id, replica_ordinal) 保证稳定 ordinal）；REST
  `POST/GET /v1/apps`、`POST /v1/apps/{id}/deployments`、`/scale`、`/status`、
  `DELETE /v1/apps/{id}`；`cmd/fpctl` 最小 CLI。
- **M3.4 发布状态机（ADR-0015）**：rollout PREPARING→CUTOVER→COMPLETE /
  失败 ROLLBACK；controller 只在目标 generation 全部 backend
  RUNNING+READY 后发布新 route generation，旧 backend 置 draining，
  drain 期限后回收旧 execution；app 级单 rollout 互斥。
- **M3.5 验收**：`scripts/lab/e2e-m3.sh`（U1 主机名访问、U2 发布/drain/回滚、
  U3 scale 3 + 杀 VM 重建缺失 ordinal、1000 次 slot 分配/释放 + agent 重启
  无 netns/veth/TAP 泄漏、guest→host/私网/跨 project 隔离断言）；
  文档回填 + make check + 提交。

## 验收映射（mvp-plan §7）

- 1000 次 slot 分配/释放与 agent kill/restart 后无 netns/veth/TAP 泄漏：
  e2e-m3 步骤（slot-only，不需要 VM）；
- guest 不能访问 host/私网，跨 project 不可互访：e2e-m3 隔离断言（slot netns
  内 curl 目标必须超时/拒绝）；
- U1：部署 nginx，通过 hostname 访问：e2e-m3 步骤 3；
- U2：新 deployment 全部 ready 前不切流；切流后老 backend drain；失败可回滚：
  e2e-m3 步骤 4/5（ADR-0015 组合场景）；
- U3：`scale 3` 放置（单机=候选不足降级+调度事件；反亲和 distinct 由
  `make sim` 覆盖）；杀一个 VM 后 controller 仅重建缺失 ordinal：e2e-m3 步骤 6；
- catalog 过期/backend 失联：edge 受控 502/404（M1.6 已实现，M3 回归断言）。

## 执行记录（2026-08-26）

- 切片 M3.1–M3.5 全部完成；`sudo bash scripts/lab/e2e-m3.sh` 真机全绿：
  U1（hostname→edge→slot→VM 200）/ 隔离（三类私网目标全拒）/ U2 成功发布
  （全 READY 才切流→旧代 drain 回收）/ U2 失败发布（409 互斥 + 自动回滚
  + 旧代持续服务）/ U3（scale 3 + placement 事件；杀 VM 仅重建缺失 ordinal）/
  1000 次 slot attach/release 无泄漏 / agent 重启收敛（VM 重建 + slot 一致
  + edge 200）/ 终态零残留（VM/netns/veth/路由/在途 op）。
- M3.2 readiness 真机验证：nginx http 探针 READY、不可达端口 NOT_READY，
  均正确投影进 route_backends 并驱动切流判定。
- 关键真机修复（详见 mvp-plan §7 执行记录）：Nomad task cgroup 内存限额
  成波杀 firecracker（准入改 min(host, cgroup) + 限额 16GiB）；发布轴唯一键
  从 generation 改 deployment_id；PG 时间戳解析；镜像错误永久化；slot
  reconcile 降级容错；app 删除墓碑化。
- DEFERRED-MULTI-NODE：双节点反亲和 U3、跨节点发布窗口、两节点 slot 网络
  独立性验收（上线前必须补测）。

## 评审修复（2026-08-27，代码评审 P0–P3 全量修复）

评审发现两类 P0（删除的 app 被 controller 无限复活；`op-del-{id}` 裸幂等键
在 scale down→up→down 后永久 409）与 P1/P2/P3 若干，全部修复并重跑 e2e：

- **P0-1 app 删除收敛**：migration 0007 新增 `apps.deleted_at`；
  `SoftDeleteApp` 事务墓碑化（deleted_at + 终结 rollout + deployment
  SUPERSEDED）；`ListApps` 过滤墓碑；`reconcileApp` 双保险不再补建；
  `deleteApp` 先墓碑后下发 delete，重复删除幂等 202。
- **P0-2 delete 幂等键**：`store.UserDeleteOpID` 嵌 execution 尾部 8 字符
  （API 与 controller 共用，不发散）；冲突降级为事件审计不阻断对账。
- **P1-1 探针与 List 解耦**：探针移入 `health.Worker` 独立循环（每轮 8s
  预算 + 单探针 per-request 超时），ListMachines 只做 O(1) Observe/Read。
- **P1-2 镜像策略**：`imagepolicy` 包（digest 形态校验 + registry
  allowlist 环境变量 `FIREPAAS_REGISTRY_ALLOWLIST`），create/deploy 均拦。
- **P2-1** ROLLING_BACK 期间 scale 对账目标改为 from 代（S6 语义）。
- **P2-2** readiness 随 execution 换代重置（Observe 检测 CreatedAt 变晚
  即 runtime=nil/readiness=UNKNOWN，防新代虚报 READY 提前切流）。
- **P2-3** `DeployApp`/`CompleteRolloutWithStatus` 事务化（app 行锁串行化
  deploy 互斥；CUTOVER 完成与 deployment 终态同事务）。
- **P2-4** e2e 新增回归：7.5 非法镜像 400 / 7.6 catalog 过期 404+重建
  200 / 7.7 stale execution 请求 agent proxy 拒绝 / 11 删除后无复活。
- **P3**：rollback 无活跃 rollout → 404；createApp 重复 → 409；探针 HTTP
  客户端不再截断用户超时；fpctl deploy 支持 --env/--port；strconvUpper
  手卷函数删除。
- 重跑验收：`e2e-m3.sh` 全绿（含新增 7.5/7.6/7.7/11 回归）；单元/PG-gated
  测试全绿；`make sim` 10 万次断言 PASS；实验室僵尸 app 已清理。

## 风险

- nftables 与主机既有规则共存：只增删自有表（ip fp-slot / ip fp-isolation），
  规则只匹配自有 veth 集合；实验室已与 docker/k8s 共存验证。
- slot 模式下 guest egress 依赖两级 NAT（slot 内 + 主机出口）；bridge 模式保留
  为回退开关。
- hypeman TAP 名依赖 lib/network.GenerateTAPName 的 8 字符截断规则；若上游
  改名需同步（contract test 覆盖）。
- 单机无法验收跨节点 U3 反亲和与两节点发布；DEFERRED-MULTI-NODE。
