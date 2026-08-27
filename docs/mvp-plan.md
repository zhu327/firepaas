# firepaas：修订后的 MVP 计划

> 状态：执行草案 v4。在 v3（运行时交互契约、部署一致性、身份降级路径、写者数量演进）基础上吸收代码评审补充：readiness 信号归属（ADR-0008）、放置约束与副本反亲和（ADR-0009）、secret 引用路径（ADR-0010）、私有化 edge 入口（ADR-0011）。任何实现以 [architecture.md](architecture.md) 与 ADR 为准。

## 1. 目标、版本边界与排期

### 1.1 首个内部生产 MVP

面向受信内部团队交付下列闭环：

```text
OCI image → app/deployment → 多副本 Firecracker VM
→ hostname 路由 → 健康检查 → 滚动发布/回滚 → 节点故障后的无状态重建
```

**团队/工期基线：3 人约 10–14 个月；2 人约 12–18 个月。** 该区间包含工程返工与稳定化，但不包含 GA 前 30 天观察期。若只做内部 alpha（不含 slot 网络、自动唤醒、DR、完整安全/可观测），3 人可争取在 6–8 个月交付；在 M0 数据出来前不承诺更短排期。

### 1.2 MVP 范围

| 能力 | MVP 承诺 |
|---|---|
| 运行时 | Firecracker、OCI 镜像、可配置 CPU/内存、无状态 VM |
| 应用 | app / deployment / 稳定 replica ordinal / machine execution |
| 流量 | `<app>.<domain>`、TLS、版本化 route backend set、简单 round-robin |
| 发布 | readiness 后切流、drain、失败回滚、scale N |
| 调度 | node discovery、硬准入、Best-of-K、资源预约与对账 |
| 隔离 | netns slot、cgroup v2、nftables 默认拒私网、project/API 权限隔离 |
| 运维 | 最低 metrics/logs/traces、PG 备份恢复、Redis 投影重建、runbook |
| 安全 | control-plane/edge/agent mTLS、RPC 授权、短期 registry 凭证、审计 |

### 1.3 明确移出 MVP（候选进入 v1.1）

- node-local 持久卷和有状态 workload SLA；
- 节点池自动缩容、无中断 agent 原地升级；
- service mesh / 跨节点 VM 私网；
- Dockerfile 构建服务、Web 控制台、计费；
- UFFD/NBD 极致冷启动优化；
- 支持 node-local snapshot 的跨节点恢复。

scale-to-zero 是**条件特性**：只有 P0 证明 standby 稳定且恢复达标后才进入 MVP；否则以 stop+cold-start 降级，不阻塞发布主线。

### 1.4 SLO 与适用范围

| 指标 | 目标与前提 |
|---|---|
| API p95 | <150ms，不含异步 operation 完成时间 |
| 已缓存镜像冷启动 p95 | <5s，以 P0 目标硬件实测为准 |
| 未缓存镜像冷启动 p95 | <60s，记录镜像大小/网络条件 |
| standby resume / autoresume | <1s / <5s，仅 origin node 健康、特性开启时 |
| 节点故障 | <60s 检出；`<120s ready` 仅适用于无状态且预热镜像副本 |
| 有状态/本地快照 | 不提供节点永久故障 RTO/RPO |

## 2. 不可跳过的前置契约

以下在实现 agent/控制器前冻结；变更需 ADR 和兼容性评审：

1. **状态模型**：PG desired/business truth，agent observed truth，Redis 可重建 projection（ADR-0003）。
2. **副本与幂等**：`(deployment_id, replica_ordinal)` 是逻辑副本；`machine_id + execution_id + generation + operation_id` 是 fenced 操作单位。
3. **路由模型**：hostname 映射到版本化 healthy backend set，不映射单 machine（ADR-0005）。
4. **内部身份**：control-plane→agent、edge→agent 使用 mTLS 与 RPC 授权（ADR-0006）。
5. **故障语义**：Redis 丢失可重建；agent ACK 丢失、旧 execution、orphan、节点失联均有明确 reconcile 决策表。
6. **运行时交互通道**：logs 的数据面通道（agent `StreamLogs` 流式 RPC，由控制面代理，CLI 不直连 agent）与 `log_url`（仅归档，可为空）语义在 M1.2 一并冻结；实时日志不经对象存储。Exec 保持实验能力：只保证会话创建幂等，MVP 断线即终止。
7. **控制面写者数量**：与部署副本数绑定、随里程碑演进（ADR-0007）：M1 单实例，M2a leader election，M2b 创建路径多写。`count` 提升是代码能力的结果，不是部署参数的自由选择。
8. **健康/就绪信号**：readiness 由 agent 在 host 侧经 workload endpoint 执行探针得出（M1 为 bridge guest IP，M3 为 slot IP），作为 observed state 随 `Machine` 上报；controller 据此发布 route backend（ADR-0008）。
9. **放置约束**：调度管线先过滤（状态/资源/label/node_pool/反亲和）后打分（ADR-0009）；DEPLOYMENT 反亲和为尽力而为，候选不足可降级并记录调度事件。
10. **secret 路径**：值与引用分离，PG 只存密文；值经 mTLS+fencing 通道一次性下发，不进响应、Redis、审计与日志（ADR-0010）。
11. **edge 入口**：M1–M3 为 DNS 轮询形态，M4 交付 keepalived VIP；证书由 step-ca 内部 CA（Caddy ACME 集成）按需签发，客户端信任链预置为运维前置（ADR-0011）。
12. **流量边界**：M1 起唯一正式路径为 `edge → agent proxy → workload endpoint`；Redis/edge 不感知 slot IP。proxy credential 只在 Create 请求中单向下发并绑定 execution。

## 3. 里程碑

| 里程碑 | 周期 | 输出/出口 |
|---|---:|---|
| M0：实验室与 go/no-go | 3–4 周 | 可运行 Nomad job、硬件与 hypeman 基准、范围决策 |
| M1：契约与单节点 vertical slice | 6–8 周 | 受认证 API → controller → agent → route → HTTP 请求 |
| M2：双节点调度与收敛 | 6–8 周 | Best-of-K、幂等/对账/混沌、基础遥测 |
| M3：slot 网络、多副本与滚动发布 | 8–12 周 | 正式 slot 数据面、U1–U3、版本化 route、回滚与 drain |
| M4：安全入口与条件弹性 | 6–8 周 | secrets、TLS/VIP、proxy hardening、可选 scale-to-zero |
| M5：内部生产就绪 | 8–12 周 | 可观测、备份/恢复、浸泡、DR、runbook、30 天观察 |

```text
M0 → M1 → M2 → M3 → M4 → M5
             └─ slot manager 研发可与 M2 并行，必须在 M3 发布闭环中集成
```

每个出口评审必须确认：验收已演示、无未决 P0/P1、回滚路径和遗留项已记录。

## 4. M0：实验室、Nomad 与数据面 go/no-go（3–4 周）

### 目标

证明目标硬件与 hypeman/Firecracker 可支撑后续投入，避免将未验证性能写成产品承诺。

### 工作

1. 准备 3 Nomad/Consul server（兼任 `control` 池 client）+ 2 compute 节点；统一使用**真实 Nomad node pool**（`compute`/`control`）：`scripts/bootstrap-lab.sh` 按 role 写入各节点 client 的 `node_pool` 配置，集群就绪后用 `iac/nomad/pools/*.hcl` 创建池。禁止 `-meta` constraint 方案，禁止两种混用。
2. 新建独立 `iac/nomad/hypeman-p0.hcl`：真实 hypeman artifact/config、host data dir、KVM/网络前置条件、真实健康检查；不复用未来 `agentd` job。
3. 对 job 执行 `nomad fmt`/`nomad job validate`/`nomad job plan`，并完成每 compute 节点一个 alloc 的 smoke test。
4. 两节点分别跑 `pull → run → exec → logs → stop/delete`；验证重启和 `kill -9` 后的进程/TAP/数据恢复行为。
5. 采集缓存/未缓存启动、standby restore、warm fork、密度、镜像拉取、内存/CPU 使用。
6. 完成可运行的 agent 依赖 spike：以独立 adapter 实际执行 Create/List/Delete，不只做静态代码判断；列出 manager 构造、本地 JSON、后台 goroutine、bridge/ingress/registry 可关闭性等耦合点，估算 upstream 修改量并决定“直接依赖 / 维护 fork / 抽 runtime core”三选一。
7. 明确 P0 hypeman 运行形态：**不配置任何 ingress**（内嵌 Caddy/DNS 不启动，避免与 Nomad/Consul 及未来 edge 端口冲突）；基准驱动方式（hypeman-cli 或 REST+JWT，token 用 hypeman `cmd/gen-jwt` 生成）记录于 benchmarks.md。
8. 冻结 Firecracker 二进制、内核与 guest rootfs 基件的分发与版本 pin 方案（随 agent artifact / 节点包管理 / 对象存储三选一），记录于 capacity-model.md；同时建立 host kernel/KVM/CPU vendor 与 snapshot compatibility key，不兼容时必须 cold-start。
9. 冻结镜像入口约束：MVP 仅运行 digest-pinned OCI image，限制镜像/解包体积并明确 registry allowlist 与短期凭证边界；mutable tag 只在部署创建时解析一次。
10. 将 `scripts/bench-hypeman.sh` 从提示脚本改成可执行 runner：固定样本数、输出原始 JSON/CSV 和 p50/p95，自动检查进程/TAP/netns/磁盘残留。

### 单机执行记录（2026-08-25，ADR-0012）

已在唯一物理机上完成折叠版实验室并跑通数据面验证：

- 环境：用户态 Go/Nomad/Consul/hypeman 工具链（`scripts/lab/`），root 运行
  Nomad client + raw_exec hypeman P0 job；compute 节点池已落地，job
  fmt/validate/plan 全绿，smoke `pull → run → exec → logs → stop/delete` PASS。
- 实测（详细与原始样本见 benchmarks.md）：冷启动(缓存) p95 2.17s；未缓存
  冷启动(40MB 镜像) p95 7.6s；restore p95 95ms；warm fork p95 660ms；
  密度 32×micro 后被网络带宽准入拦截（每 VM 默认 7.5MB/s）。
- 崩溃恢复：kill -9 VM → state Unknown + 无 TAP 残留；kill -9 hypeman →
  Nomad 6s 内恢复 /health。
- 依赖策略初步结论（M0.4）：**直接依赖 + 小范围上游化**。
  `agent/cmd/m0-spike` 已实际用 hypeman lib 完成 Create/List/Delete（PASS）；
  需要上游化的点：`cmd/api/config` 提取到 `lib/config`、新增 agent 专用装配
  函数，详见 agent/internal/README.md。
- 关键环境适配：Nomad 2.0 不支持 `file://` artifact（改用 raw_exec 绝对路径）、
  `nomad job fmt` 已改名 `nomad fmt`、node pool 用 `node pool apply`；受限网络下
  hypeman 增加 `HYPEMAN_DOCKER_HUB_MIRROR` lab 补丁并记录在 capacity-model.md。
- 单机不验证项：3-server quorum、双 compute 放置/反亲和、跨节点快照兼容
  （全部标记 `DEFERRED-MULTI-NODE`）。

### 出口（Go/No-Go）

- `docs/benchmarks.md` 与 `capacity-model.md` 有环境、命令、样本、p50/p95；✅(单机)
- 冻结 MVP 默认 hypervisor 与初始参数，或记录参数不达标的产品降级；✅ Firecracker v1.14.2，R=4/K=3/α=0.5/mem=1.0
- 明确 snapshot 是否足以支持 M4；✅ restore p95 95ms，不降级 scale-to-zero；稳定性另验
- 确认 agent 抽取路径可行并选定依赖策略（直接依赖 / fork / runtime core）；✅ 直接依赖 + 小范围上游化（spike PASS）
- P0 job 可重复部署、重启与卸载，无未解释的 host 残留；✅(单机,kill -9 两项观测通过)
- 节点池方案已在实验室实际落地；✅(单机 compute 池;多机 DEFERRED-MULTI-NODE)
- Firecracker/内核/基件的分发、pin 与 snapshot compatibility key 已决定并记录；✅(capacity-model.md)
- benchmark runner 可重复执行并保留原始样本；✅(raw.csv/jsonl/meta.json)
- 镜像 digest、大小限制、registry allowlist 和凭证方案已确定；⚠️ digest-pinned 原则已定，大小/allowlist/凭证待 M1.3 前冻结

## 5. M1：契约与单节点 vertical slice（6–8 周）

### 目标

实现最小完整链路，而不是先堆 CRUD：

```text
authenticated API → PG desired state + operation → controller → mTLS agent Create
→ observed state → Redis route projection → edge → HTTP 200
```

### 工作

1. **工程基线**：正式 module path、实际 proto generation、CI（build/test/lint/proto breaking）、开发依赖。默认收敛为一个根 `go.mod` + 多个 `cmd/*`，减少早期跨 module 发布成本；若坚持多 module，必须用 ADR 说明独立版本需求，并同时交付 `GOWORK=off` release build。`go.work` 的 `../hypeman` 只用于本地联调，agent 的正式依赖必须 pin commit/tag。每个命令必须真实执行，禁止 no-op target。**最小 e2e harness**（一键拉起 dev 集群 + U1 冒烟脚本）随 M1 交付，后续里程碑验收复用，不靠手工。
2. **契约冻结**：实现 proto 的 fencing envelope；所有变更 RPC 均带 execution/generation/operation。M1 稳定承诺只覆盖 Info 与 Create/List/Delete 及最小 proxy 路径；Pause/Resume/Checkpoint/Image/Exec 保持实验状态，不因出现在草案 proto 中而承诺兼容。`StreamLogs` 的实时/归档边界一并确定；Exec 仅保证创建会话幂等，断线即终止，不承诺续接。
3. **身份**：部署 CA/工作负载证书、agent RPC interceptor、edge/control-plane 身份与节点 ACL；加入拒绝测试。**降级路径（ADR-0006）**：M1 出口允许静态证书 + 主机端口 ACL；证书轮换与完整授权矩阵最迟 M5 完成。CA 选型在 M1.3 决定并落入 iac；**默认 step-ca**（同时承担 workload 身份与 M4 app 泛域名证书，ADR-0011），选其他方案须说明理由。
4. **状态与 controller**：PG migrations（desired/observed/operation/route/backend，**含 projects/api_keys/配额表**——M2 配额预约与 M5 API key 都依赖，现在建表避免 schema 返工）、事务 outbox；实现单 replica reconcile（部署副本数受 ADR-0007 约束，M1 为 count=1），不让 HTTP handler 直接管理 VM 生命周期。
5. **agent 最小能力**：Create/List/Delete/ServiceInfo，包装 hypeman；重启扫描后上报 observed state；未声明 health_check 时上报 `readiness=UNCONFIGURED`（ADR-0008 M1 降级语义）。同时实现 operation ledger：原子持久化 request hash 与结果、重启重放、冲突请求拒绝和可配置 GC 窗口。
6. **最小正式流量路径**：实现 agent proxy v0（edge mTLS、execution 校验、只转发声明端口）与 bridge workload-endpoint adapter；edge 完成单 hostname、单 backend、route projection 查询。M3 切换 slot 时保持 edge/catalog 契约不变，edge 永不读取 slot IP。
7. **实验室依赖落地**：PG/Redis/MinIO/registry 以 systemd 或 docker 直装于 control 节点（不进 Nomad，见 iac/README 部署形态表）；本机开发用 `make dev-up`（dev-compose 已含 registry:2）。

M1 内部子排序（关键路径；任何单项延期不得顺延 proto 冻结节点 M1.2）：

```text
① module/依赖策略 + proto/fencing/readiness/proxy 契约冻结
→ ② agent 最小能力、operation ledger + controller（可先静态证书）
→ ③ agent proxy v0 + 最小 edge → ④ 身份收尾（用足 ADR-0006 降级路径）
→ ⑤ e2e harness 冒烟
```

### 验收

- 同一 `operation_id` 重试 100 次，只产生一个 execution；相同 operation ID、不同 request hash 被拒绝；agent 重启后仍返回原结果；旧 generation 的变更操作被拒绝；
- 未持有 mTLS identity 的请求不能访问 5108/5107；edge 只能通过 agent proxy 到达 workload，route/location 投影不包含 slot IP；
- Redis 删除后，controller 根据 PG + agent 在受控时间内重建 route；
- 单机 nginx 经 hostname 返回 200；删除后不再存在 route backend；
- agent `kill -9` 后，observed state 能被重新上报并按决策表处理；
- e2e harness 可一键重建 dev 环境并跑通冒烟场景。

## 6. M2：双节点调度与收敛（6–8 周）

### 目标

让多 API/agent 失败条件下仍满足“不重复副本、不越过资源硬上限、最终收敛”。

### 工作

1. Nomad native discovery → mTLS ServiceInfo 同步；健康/Draining/Unhealthy 状态机。
2. agent 本机硬准入；控制面 round-robin 基线后实现 Best-of-K、pending/optimistic accounting；调度管线为**先过滤后打分**（ADR-0009），M2 实现状态/资源过滤与约束接口，label/反亲和过滤在 M3 补齐。
3. 用 PG operation/idempotency 保障业务幂等；Redis Lua reservation 只保障短时并发、项目配额和 pending TTL，不把 deployment 当唯一键。
4. reconcile desired ↔ observed：ACK 丢失、agent orphan、Redis miss、旧 execution、节点失联。
5. 最低可观测：machine/operation/execution ID 贯穿日志，调度、对账、RPC 的 metrics 与 trace。
6. 仿真与混沌：资源、创建、重试、节点失联和 Redis 重建；仿真断言含“过滤在打分前、DEPLOYMENT 反亲和下副本落点 distinct（候选不足除外）”（ADR-0009，tools/sim）。

### 验收

- 同一个 replica ordinal 的 1000 次并发重试只生成一个 machine/execution；同一 deployment 的不同 ordinal 可并发创建；
- 10 万次仿真满足：无重复逻辑副本、资源不突破 agent 硬准入、失联节点不再入选；
- 注入 API/agent crash、ACK 丢失、Redis 清空后，2 分钟内收敛且审计可解释；
- 两节点创建/删除 20 轮无 VM、bridge endpoint、Redis lease 泄漏；slot 泄漏测试属于 M3。

### 单机执行记录（2026-08-26，ADR-0014）

单机折叠版 M2 已交付（双 compute 节点部署项 DEFERRED-MULTI-NODE）：

- M2.1 Nomad discovery + 节点状态机（HEALTHY/DRAINING/UNHEALTHY）+
  ServiceInfo 20s 投影进 PG `nodes`，真机 `/v1/nodes` HEALTHY 通过。
- M2.2 scheduler 先过滤后打分（状态→node_pool/labels→资源硬准入→DEPLOYMENT
  反亲和尽力→Best-of-K R=4/K=3/α=0.5），pending 从 PG 在途 create 推导；
  agent 侧硬准入双保险（ResourceExhausted 换节点重试 ≤3）。
- M2.3 reconcile 决策表 R1–R8：ACK 丢失换代重建、orphan 清理、旧 execution
  reap、节点失联摘路由；全部动作写 scheduler_events 审计。
- M2.4 Redis Lua 预约（节点硬上限+项目配额+pending TTL 120s+重建回收）。
- M2a PG advisory lock leader（controller 只在持锁实例运行，备实例只读）。
- M2.5 手写 Prometheus 文本计数器（/metrics）+ machine/operation/execution
  ID 贯穿 slog。
- M2.6 tools/sim：100k 放置断言 PASS（过滤先于打分/硬准入/反亲和 distinct/
  失联排除；`make sim`）。
- 验收：`sudo bash scripts/lab/e2e-m2.sh` PASS（1000 并发重试→1 machine、
  多 ordinal 并发、20 轮创建/删除零泄漏、节点投影+metrics）；
  `sudo bash scripts/lab/chaos-m2.sh` PASS（ACK 丢失 32s、agent crash 16s、
  Redis 清空 3s、API crash 与在途 crash 均 <120s 收敛）。
- 真机验收修掉的关键 bug：换代重建未清 observed 导致 R8 短路无限换代；
  create 退避重试 opID 撞历史幂等键；reap delete 误把 desired 置 DELETED；
  graceful agent 重启会收养存活 VM（crash 注入需同时 kill firecracker）。

遗留（进入 M3 前）：两节点真实放置/反亲和验收（DEFERRED-MULTI-NODE）；
Nomad 2.0 system job Latest Deployment 历史脏状态；PG operations 的
CLAIMED 回收窗口为 30s 定时器（非精确租约）。

## 7. M3：slot 网络、多副本、滚动发布与路由（8–12 周）

### 目标

交付无状态 PaaS 的核心产品闭环，不依赖 scale-to-zero 或本地卷。

### 工作

1. netns/veth/tap/nftables slot manager、启动回收、`bridge|slot` 灰度开关；slot 由 agent 本地原子管理。将 M1 proxy 的 workload-endpoint adapter 从 bridge 切换为 slot，不改变 edge/catalog 契约。
2. app/deployment/replica controller；`scale N` 管理稳定 ordinal。
3. route backend set、readiness、generation 发布、round-robin、drain、rollback。**冻结发布状态机与组合场景决策表**：发布中 scale、发布中节点故障、发布中再次发布、回滚中 drain；MVP 至少实现“同一 app 同时只允许一个 rollout”的互斥，复杂组合明确延后。readiness 的唯一来源是 agent host 侧探针（ADR-0008）：本里程碑交付 hypeman `lib/healthcheck` 迁移与“探针 → observed state → controller → route_backends”闭环。
4. app API/最小 CLI：create、deploy image、scale、status、logs；exec 仅在不阻塞主线时加入。
5. 镜像 digest、节点缓存与预热；共享 registry 落地为明确部署物（实验室 `registry:2` 于 control 节点，生产独立服务或企业 registry）；节点镜像缓存 LRU 驱逐与磁盘水位守护（见 capacity-model.md）；将镜像可用性纳入 placement 亲和但不突破资源过滤。
6. **放置约束落地**（ADR-0009）：label/node_pool 硬过滤、DEPLOYMENT 尽力反亲和与调度事件记录；U3 验收断言反亲和。
7. 入口落地（ADR-0011）：M1–M3 为 DNS 轮询形态——`*.<domain>` 解析到各 edge 实例地址（control 池节点 IP，edge 静态 80/443），实验室一次性配置；TLS 按 M4 交付，HTTP 即满足 U1；keepalived VIP 属 M4。
8. 无状态节点故障重建与分段 SLO 观测。secrets 的数据模型在 M1 预留，完整注入移至 M4，避免与网络/发布关键路径竞争。

### 验收

- 1000 次 slot 分配/释放与 agent kill/restart 后无 netns/veth/TAP 泄漏；guest 不能访问 host/私网，跨 project 不可互访；
- U1：部署 nginx，通过 hostname 访问；
- U2：新 deployment 全部 ready 前不切流；切流后老 backend drain；失败可回到上一 generation；
- U3：`scale 3` 在可用节点间放置（候选充足时副本不落同节点，候选不足时降级并产生调度事件，ADR-0009）；杀掉一个无状态 VM 后 controller 仅重建缺失 ordinal；
- catalog 过期、backend 失联、route 更新期间 edge 受控返回/重试，不向 stale execution 转发。

### 单机执行记录（2026-08-26，ADR-0015）

单机折叠版 M3 已交付（跨节点放置/反亲和验收 DEFERRED-MULTI-NODE）：

- M3.1 slot 网络：`internal/agent/network/slot`——每 VM 一个 netns（veth/30
  桥接+TAP 移入+独立网关）、slot 内一级 NAT、root 侧 nftables O(1) 隔离
  （INPUT 默认拒 guest、FORWARD 拒私网/组播、10.12/16 出口 masquerade）、
  /32 guest 路由代理通道；`FIREPAAS_NETWORK_BACKEND=bridge|slot` 灰度开关；
  1000 次 attach/release 与 agent 重启 reconcile 无泄漏（含回归测试）。
- M3.2 readiness（ADR-0008）：`internal/agent/health` 复用 hypeman
  lib/healthcheck 阈值语义，策略 base64url 存 tag；READY/NOT_READY 随
  ListMachines 上报并投影进 route_backends（真机验证 READY/NOT_READY 分流）。
- M3.3 app/deployment/replica：migration 0006（deployments/rollouts 表 +
  machine 唯一键放宽为 (app_id, ordinal, deployment_id)）；AppController
  稳定 ordinal 对账；REST /v1/apps 全套 + fpctl CLI。
- M3.4 发布状态机（ADR-0015）：PREPARING→CUTOVER→COMPLETE / 失败
  ROLLING_BACK；全部目标 ordinal RUNNING+READY 才切流；旧代 draining、
  drain 期限后回收；单 rollout 互斥（唯一部分索引 + 409）；坏镜像
  create InvalidArgument 快速终态 → 自动回滚。
- M3.5 验收：`sudo bash scripts/lab/e2e-m3.sh` PASS（U1/隔离/U2 成功与
  失败发布/U3 重建/1000 次 slot 无泄漏/agent 重启收敛全部通过）。

真机验收修掉的关键 bug：
1. **Nomad task cgroup 内存限额（1GiB）成波杀 firecracker**——agent 硬准入
   改为 min(host, cgroup memory.max)，task 限额提到 16GiB。
2. rollout 时间戳解析（PG 文本格式 ≠ RFC3339）导致 PREPARING 立即超时回滚。
3. machine 唯一键用 generation 会被 R3 换代重建的 fence +1 撞坏——改用
   deployment_id 轴；AppController 的 have 判断同样改 deployment 轴。
4. 镜像不可拉取映射为 InvalidArgument（永久错误），否则失败发布靠 5 分钟
   超时才回滚且无限重派。
5. slot Reconcile 遇僵尸 guest 目录 crash-loop agentd——降级容错
   （attach 失败记日志继续，TAP 不在 root 时跳过）。
6. app 删除必须墓碑化（replicas=0 + deployment SUPERSEDED），否则 scale
   对账会无限重建副本。

### 评审修复（2026-08-27）

M3 全量代码评审发现两类 P0 与若干 P1/P2/P3，已全部修复并重跑验收
（详见 plans/2026-08-26-m3-single-node.md 评审修复节）：

- **P0-1**：删除的 app 被 AppController 无限复活——墓碑化改
  `apps.deleted_at`（migration 0007）+ `SoftDeleteApp` 事务（先墓碑后
  下发 delete），ListApps/reconcileApp 双层过滤，重复删除幂等 202。
- **P0-2**：`op-del-{machineID}` 裸幂等键在 scale down→up→down 后撞
  同键不同请求体永久 409——改 `store.UserDeleteOpID` 嵌 execution 尾缀，
  API/controller 共用同一约定。
- **P1-1**：探针内联在 ListMachines（共享 gRPC 10s deadline，副本多时
  探针耗尽预算引发 readiness 抖动/摘路由）——移入独立 health.Worker
  （每轮 8s 预算 + per-request 超时）。
- **P1-2**：镜像 digest 校验 + registry allowlist（`imagepolicy` 包，
  `FIREPAAS_REGISTRY_ALLOWLIST` 环境变量），create/deploy 入口拦截。
- **P2**：ROLLING_BACK 期间 scale 目标改 from 代（S6）；readiness 随
  execution 换代重置（防新代虚报 READY 提前切流）；deploy/rollout 终态
  多语句转移事务化；e2e 补 catalog 过期/stale execution/删除不复活回归。
- **P3**：rollback 404、createApp 409、探针超时配置不被客户端硬上限
  截断、fpctl deploy 支持 --env/--port。
- 重跑：e2e-m3 全绿（含新增回归步骤）；单测/PG-gated/sim 10 万次全绿；
  实验室历史僵尸 app 清理归零。

遗留（进入 M4 前）：EXEC 探针不支持（需 vsock 通道）；registry LRU/预热与
共享 registry 部署物 DEFERRED；NetworkManager 会在 root ns 对新 TAP 短暂
“assume connection”（无实测危害，M4 记录）；agentd 重启（SIGTERM）当前会
带走 VM（hypeman teardown），重建由 R1-R8 收敛（chaos 验收已覆盖）。

## 8. M4：安全入口、proxy hardening 与条件弹性（6–8 周）

### 目标

在 M3 正式 slot 数据面上补齐秘密管理、入口 HA、代理安全与可选 scale-to-zero，不再改变基本流量拓扑。

### 工作

1. **secrets v1**（ADR-0010）：secrets 表 + 信封加密 + `secret_refs`/`secret_env` 下发链路与 CLI `apps env set --secret`；审计只记 secret_id/version；日志/审计字段黑名单中间件。
2. agent proxy hardening：仅允许 edge mTLS identity，校验单向下发、execution-bound 的 proxy credential，仅转发声明端口；execution 替换/删除后立即撤销验证材料。
3. edge 高可用与 TLS（ADR-0011）：keepalived VIP（无二层环境时保留 DNS 轮询降级）；TLS 由 step-ca 经 Caddy ACME 集成按需签发泛域名证书；客户端根证书预置列为运维前置并写入 runbook；route generation 缓存失效与限流。
4. 若 M0 通过：idle controller、Pause/Resume、origin-node 优先、快照损坏/节点失联时 cold-start 降级。
5. Redis 可用性验证：明确 edge serve-stale 窗口与投影重建时限；依据验收结果决定是否引入 sentinel。

### 验收

- proxy credential 不出现在 Machine/ListMachines/Redis/日志/operation result；错误身份或 credential 不能经过 agent proxy；
- 两节点多副本通过同 hostname 稳定服务；
- Redis 宕机注入：数据面在声明的 stale 窗口内继续服务或受控降级，恢复后投影在时限内重建；
- 若启用 scale-to-zero：50 次 pause/resume 无泄漏，且只在适用 SLO 范围内达标；否则正式记录为 v1.1。

### 单机执行记录（2026-08-27）

单机折叠版 M4 已交付（跨节点入口 HA/多 edge 验收 DEFERRED-MULTI-NODE）：

- **M4.1 secrets v1**：migration 0008（secrets 表 + deployments.secret_refs）；
  信封加密（主密钥环境变量 + 随机 DEK，AES-256-GCM，AAD 绑定
  project/name/version 防行间密文互换）；`/v1/secrets` CRUD（无 reveal
  端点）；`PUT /v1/apps/{id}/secret-refs` 换绑定走 rollout 状态机；
  create 派发时解析引用为 secret_env（不入 op.Request 持久化）；
  幂等哈希剔除单向字段；auditMiddleware 字段经 redact 黑名单过滤；
  fpctl secrets set（支持 stdin）/env/rm/ls。并发版本冲突 409+Retry-After。
- **M4.2 proxy hardening**：credential = HMAC-SHA256(key, machine/execution)
  确定性派生（两侧同源，零持久化）；API `/v1/machines/{id}/traffic-token`
  按需现算给 edge（仅内存缓存）；agent 侧只存 SHA-256 摘要
  （state/creds.go，0600 原子落盘），恒时比较；delete/换代即 Drop；
  默认 fail-closed（FIREPAAS_PROXY_CREDENTIAL_REQUIRED）。评审修复：
  token 缓存按 (machine, execution) 命中，换代不命中；agent 403 →
  Invalidate + 重取一次（ModifyResponse 哨兵拦截，不半写响应）；
  API 不可达时 token 走 serve-stale（execution 匹配才复用）。
  真机追加修复：processLifecycle agent opID 改用控制面 op.ID（固定后缀
  会被 ledger 重放吞掉后续 pause/resume）；autoresume/resume 传 hypeman
  内部 ID（名字会 ErrNotFound）+ restore 后重挂 slot（reattachSlot）；
  ensureKernel 移入前清理 netns 残留同名 TAP。
- **M4.3 edge TLS/限流/缓存**：泛域名证书（内部 CA）+ :443 TLS + :80→308
  （非标端口保留）；每 hostname 令牌桶 429；路由缓存 fresh 5s +
  serve-stale 120s + 超窗 503；**权威 miss（路由已删）立即 404 + 负缓存，
  不 serve-stale**；token serve-stale 同源窗口。计数器（stale serves/
  redis errors/token errors/429/403 重试）+ FIREPAAS_EDGE_METRICS_PORT
  /metrics（Prometheus 文本）。
- **M4.4 Redis 可用性**：e2e-m4 F 步——stop redis → stale 窗口内持续
  200（X-Firepaas-Stale 头）；宕机窗口 deploy 受理不悬挂（PG 事务，
  rollout 恢复后收敛 COMPLETE）；恢复后 **FLUSHALL**（AOF 下旧实现
  未真测重建）→ 权威 miss 404 → ≤75s 投影重建回源 200。
  **sentinel 结论：不引入**——stale 窗口（120s）+ 重建时限（30s 周期）
  已满足数据面可用性目标，Redis 持久化仅加速恢复非必需。
- **M4.5 scale-to-zero（显式 API + proxy autoresume 形态交付）**：
  PauseMachine/ResumeMachine RPC（fencing+ledger+execution 绑定校验）；
  controller processLifecycle（resume 失败 → R3 cold-start 重建）；
  `POST /v1/machines/{id}/pause|resume` 显式 API；agent proxy GetEndpoint
  遇 standby 同步唤醒（autoresume，<8s SLO 含 HTTP 往返）；e2e-m4 H 步
  50 次 pause/resume 无 netns/TAP/firecracker 漂移。
  **自动化 idle 检测（CPU 阈值×时长）未实现**：需要 agent 侧 per-VM
  usage 管道（List 暴露 VM metrics → PG 投影 → app 级开关），列入 v1.1
  （见 §11 风险表）；v1 以显式 pause API + autoresume 组合覆盖核心价值
  （快照释放 VMM 内存 + 流量唤醒），origin-node 优先恢复已由 R4
  节点健康检查 + 快照本地性自然满足（无快照时 cold-start 降级）。

### 偏差与遗留（进入 M5 前）

- **keepalived VIP**：交付配置模板与 runbook（iac/keepalived/），双节点
  漂移演练 DEFERRED-MULTI-NODE；单机实验室保持 DNS 形态（ADR-0011 分层）。
- **step-ca ACME**：实验室用静态 CA（gen-certs.sh 签发泛域名），生产
  step-ca 集成记为 M5 运维前置；证书信任链预置已在 iac/keepalived/README
  文档化。
- **CLI 形态变化**：`apps env set --secret` 合并为 `fpctl app deploy
  --secret VAR=NAME[@V]` 与 `fpctl app create --secret`（同一发布语义：
  改 refs 即新 deployment）；`--secret VAR=`（空值）为移除绑定。
- **旧 e2e 脚本兼容**：e2e-m1/m2/m3/chaos-m2 已补 FIREPAAS_TRAFFIC_TOKEN_KEY
  + edge 的 FIREPAAS_API_ADDR/TOKEN（与新链路同一强制校验），重跑全绿。
- 多 edge 实例各自内存缓存 token（无共享状态）——多 edge 验收
  DEFERRED-MULTI-NODE。
- 主密钥管理仅环境变量注入，轮换/KMS 是 M5 工作项（ADR-0010 已切分）。
- agentd SIGTERM 仍会带走运行中 VM（M3 已知行为，chaos 验收覆盖重建）。

## 9. M5：内部生产就绪（8–12 周）

### 工作与验收

1. **安全**：API key 哈希、最小 scope、secrets 加密、审计、host hardening；跨 project、guest→host、无身份 agent RPC 全部拒绝；验证 digest-pinned image、registry allowlist、镜像/解包大小限制。
2. **运行时稳定性**：验证 pause/resume 后 guest 时钟、宿主 NTP、entropy、host OOM、inode/FD/conntrack/TAP 上限；将阈值和告警写入 capacity model 与 runbook。
3. **可观测**：dashboard、告警、容量/调度/route/operation trace；一次压测可关联复盘。
4. **可靠性**：PG backup/restore、Redis projection rebuild、对象存储恢复、node replacement runbook。
5. **升级**：首版只承诺 drain/rebuild 后升级；node-local 状态 machine 不保证零中断。热接管另设设计后再承诺。
6. **浸泡/DR**：72h 创建、发布、缩放与故障注入；记录 RPO/RTO 和限制。

### GA 出口

- U1–U3、隔离与恢复测试全绿；U4 仅在特性开启时纳入；
- 72h 无未解释的 VM/网络/lease/磁盘泄漏；
- Redis 投影重建、PG 恢复、节点失联演练完成；
- 支持矩阵、SLO 前提、限制和 runbook 对内部用户公开；
- 进入 30 天观察期（日历时间，不占工作量排期，对外汇报排期时单独列出）后再决定 GA 扩大范围。

### 单机执行记录（2026-08-27，M5）

- **M5.1 安全**：`api_keys` 表（SHA-256 哈希、revoke/expiry、`read<write<admin`
  三档 scope、`project_id` NULL=全部项目）+ `POST/GET/DELETE /v1/apikeys` +
  `fpctl apikey`；路由最小 scope 表 + 跨 project 防线（by-id 资源路径）+
  审计带 caller 名；镜像准入：`FIREPAAS_IMAGE_REQUIRE_DIGEST`/
  `FIREPAAS_REGISTRY_ALLOWLIST`（API 侧）+ `FIREPAAS_IMAGE_MAX_UNPACK_MIB`
  （agent 侧 ListImages 轮询 size 校验，超限永久 InvalidArgument）。
  注：实验室 DB 先行落地了同版本号 `0009_m5_api_keys.sql`（scopes text[]）；
  仓内 0009 与线上对齐，`0010_m5_apikey_meta.sql` 补齐 name/expires/revoked/
  hash 唯一索引，双轨一致。host hardening 只读审计脚本
  `scripts/lab/host-hardening-check.sh`（PASS/WARN/FAIL + runbook 链接）。
- **M5.2 稳定性**：宿主 gauge 采样器（15s：FD/inode/conntrack/entropy/load/内存）
  → `/metrics`；Prometheus 告警规则 `iac/observability/prometheus-alerts.yml`。
  实测（scripts/lab/results/m5/）：20 次 pause/resume guest 时钟漂移 **-5ms**
  （FC snapshot 在本实验节奏下不丢 wall clock）；宿主 entropy 稳定 256；
  conntrack 725→649。阈值与告警见 docs/runbook-capacity.md。
- **M5.3 可观测**：`GET /v1/operations[/{id}]` operation trace（request/result
  全树脱敏）+ `fpctl ops ls/show`；controller sync 每拍发布 machines_by_state/
  nodes_unhealthy/ops_pending gauge；Grafana datasource+dashboard provisioning
  入库 `iac/observability/grafan*`，compose `--profile observability` 可选拉起。
- **M5.4 可靠性**：`scripts/lab/pg-backup.sh`（保留 7 份）+ `pg-restore-rehearsal
  .sh`（scratch 库恢复+行数断言，e2e 实测一致）；`POST /v1/system/reprojections`
  清投影→controller sync 重建（e2e flushall 后 ≤15s route 回源）；
  `minio-backup-rehearsal.sh`（distroless 容器双拷贝清单一致，基线入结果目录）；
  节点替换 runbook（docs/runbook-node-replacement.md，双机执行）。
- **M5.5 升级**：`nodes.draining`（0011）+ `POST /v1/nodes/{id}/drain|ready` +
  scheduler 过滤「draining」+ `scripts/lab/upgrade-agentd.sh` 全流程演练
  （nomad job restart + 对账收敛，e2e PASS）。首版承诺 drain/rebuild 不承诺
  零中断，写入 runbook。
- **M5.6 e2e-m5** 六段验收 FULL PASS（约 4.5 min，日志入 results/m5/）：
  A 错 key/只读 scope/跨 project 负路径 + registry allowlist/require-digest
  拒绝路径；B 20 循环 pause/resume + guest 时钟采样（-5ms）；C /metrics
  宿主 gauge + op trace 零明文；D PG 备份→恢复行数一致 + flushall→显式
  重投影→edge 200；E drain→rebuild→ready→换版后建 app 200；F hardening
  审计无 FAIL + 终态 fc=0/netns=0/pending=0。
- **遗留与偏差**：1) 自动 idle 检测（per-VM usage 管道）→ v1.1（同 M4 记录）；
  2) hypeman 对 OCI index digest 的 `GetImage` 寻址缺陷（多平台镜像的 digest
  引用无法 lane 匹配）→ firepaas 侧改用 `ListImages` 轮询矩阵（Match on
  metadata Name）；上游修复另开 DEFERRED-hypeman-upstream；3) `FROM scratch`
  镜像无发行版 init，guest 不写 boot marker → 挂起 Initializing；文档必备
  基础镜像要求（runbook-capacity）；4) 72h soak 以 `scripts/lab/soak-m5.sh`
  交付并后台运行（结果 results/soak-m5/summary.csv），本里程碑记录 60min 排练。
- **多 edge/双机项**继续累积 DEFERRED-MULTI-NODE。

## 10. 依赖与分工

| 工作流 | 前置 | 可并行 |
|---|---|---|
| agent adapter | M0 spike | PG/controller schema |
| proto + mTLS | M0 | 工程/CI |
| route projection | 状态/契约 | 单机 agent |
| Best-of-K | ServiceInfo + agent 硬准入 | slot 网络研发 |
| rollout controller | route catalog + reconcile | CLI/镜像缓存 |
| readiness 探针（ADR-0008） | agent 最小能力 | Best-of-K |
| secrets v1（ADR-0010） | PG schema（M1 建表口） | 镜像缓存 |
| edge 入口 VIP/CA（ADR-0011） | M1.3 CA 选型 | slot 网络研发 |
| scale-to-zero | M0 snapshot go + M3 正式 proxy/slot | 不阻塞 M3 |

三人建议：A 控制器/状态/调度；B agent/网络；C 平台/身份/edge/可观测。**M4/M5 期间 A 或 B 分担 edge 与可观测**，避免 C 在两个里程碑连续成为关键路径。共享文件（architecture、proto、route contract）只在评审后串行修改。

## 11. 风险与降级

| 触发 | 决策 |
|---|---|
| P0 snapshot 不稳定或 restore 不达标 | scale-to-zero 移至 v1.1，保留 cold-start |
| slot 网络在 M2 并行 spike 后仍无法通过回收/隔离 | M3 不进入发布验收；bridge 只保留单节点开发用途，不替代内部生产隔离承诺 |
| Best-of-K 仿真失败 | 保留硬准入 + 简单均衡，调度优化延后 |
| agent 无法安全 drain/升级 | 承诺 drain/rebuild，不承诺零中断 |
| 排期超过 30% | 先砍 exec 高级能力、scale-to-zero、CLI，而非砍状态/路由/身份/对账 |
| M1 身份工作量超期 | 出口接受静态证书 + 主机端口 ACL；轮换与授权矩阵最迟 M5（ADR-0006 已留口） |
| Redis 单点故障影响数据面 | edge serve-stale 窗口 + 重建时限；M4 验收不过再引入 sentinel |
| 发布状态机组合场景超预期 | MVP 收敛为同 app 单 rollout 互斥；复杂组合明确延后 |
| readiness 探针实现延期 | M1 用 UNCONFIGURED 语义；M3 前未补齐时 route 发布降级为 RUNNING 即 READY 并显式记录（ADR-0008） |
| 反亲和候选不足（小集群） | 尽力而为降级 + 调度事件；不为反亲和牺牲可用性（ADR-0009） |
| 无二层环境无法 keepalived | 保留 DNS 轮询降级（ADR-0011 已留口） |
| 自动 idle 检测（per-VM usage 管道）延期 | v1 交付显式 pause API + proxy autoresume；自动化判定进 v1.1（M4 执行记录已注明） |
| Redis 单点（无 sentinel） | 120s serve-stale + ≤75s 投影重建验收通过；M4 结论不引入 sentinel，数据面可用性目标已满足 |
| 客户端无法预置内部 CA 根证书 | 泛域名证书改用自签 + 文档化手动信任；身份 mTLS 不受影响（ADR-0011） |
