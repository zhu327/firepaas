# firepaas 目标架构

> 本文冻结首个**内部生产 MVP**的架构契约；实施顺序见 [mvp-plan.md](mvp-plan.md)。
> 参考：本仓库 [ADR](adr/)；早期仓库外可行性分析不属于可移植架构证据。

## 1. 一句话

开发者提交 OCI 镜像，控制面把应用的**期望副本**调和为 Firecracker microVM；edge 按域名把流量发送给当前健康的副本。平台首版面向无状态、受信内部工作负载。

## 2. 不变原则

1. **Nomad 只编排基础设施**；用户 VM 由控制面选节点、agent 创建。
2. **控制、数据、流量面分离**：API 不进入 app 流量热路径。
3. **Postgres 是期望状态与业务事实的唯一权威**；agent 报告观测状态；Redis 只是可重建的路由/租约投影。
4. **每次改变运行态的操作均 fenced**：`machine_id + execution_id + generation + operation_id` 防止旧请求操作新一代 VM；普通查询不要求 operation fencing。
5. **先让 route 成立，再做调度优化**：`hostname → backend set → machine location` 是多副本和滚动发布的基础。
6. MVP 不承诺 node-local volume、node-local snapshot 在节点永久故障后的自动恢复，也不承诺 agent 原地升级零中断。

## 3. 拓扑

```mermaid
flowchart TB
    U[用户 / CI / CLI] -->|API| CP[control-plane]
    U -->|app 流量| EDGE[edge-proxy]
    CP --> PG[(PostgreSQL: desired state / operations)]
    CP --> RD[(Redis: route projection / leases)]
    EDGE --> RD
    CP -. mTLS + gRPC .-> AGENT
    EDGE -->|mTLS + traffic token| AP[agent proxy]

    subgraph compute 节点
      AGENT[agentd :5108]
      AP[proxy :5107]
      VM[Firecracker VM + netns slot]
      AGENT --> VM
      AP --> VM
    end

    CP --> OS[(S3/MinIO: image/snapshot artifacts)]
    AGENT --> OS
```

Nomad 发现并运行 `agentd` system job、API/edge service jobs；它不创建或追踪用户 VM。

## 4. 状态与调和模型

### 4.1 三类状态

| 类别 | 权威 | 内容 | 恢复方式 |
|---|---|---|---|
| Desired/business state | PostgreSQL | app、deployment、replica/machine、期望数量、generation、操作记录、配额、审计 | 备份恢复 |
| Observed runtime state | agent | 实际 VM、execution、slot、进程、健康、资源占用 | agent 启动扫描并上报 |
| Derived/ephemeral state | Redis | route backend set、machine location、reservation、短期 operation result | controller 从 PG + agent 重建 |

控制器持续比较 PG 的 desired state 与 agent observed state；它是唯一将两者调和、发布路由投影的组件。Redis 丢失不得改变 deployment 的业务结论。

### 4.2 PostgreSQL 最小模型

```text
projects(id, name, vcpu_quota, mem_mib_quota, ...)
api_keys(id, project_id, key_hash, scopes, created_at, last_used_at, ...)
secrets(id, project_id, name, version, value_ciphertext, created_at, ...)  -- ADR-0010,值仅存密文
apps(id, project_id, hostname, desired_replicas, generation, ...)
deployments(id, app_id, generation, image_digest, status, ...)
machines(id, deployment_id, replica_ordinal, desired_state, generation,
         current_execution_id, requested_vcpu, requested_mem_mib, node_id?, ...)
operations(id, machine_id, execution_id, generation, kind, idempotency_key,
           status, result, ...)
routes(id, app_id, hostname, active_generation, ...)
route_backends(route_id, generation, machine_id, execution_id, port, weight,
               readiness, draining, ...)
```

关键约束：

- `UNIQUE(deployment_id, replica_ordinal)`：一个副本槽位只有一个逻辑 machine。实现现状：迁移 0006 将唯一键放宽为 `(app_id, replica_ordinal, generation)` 形态，发布窗口内新旧 generation 的同 ordinal machine 共存（见 `internal/controlplane/controller/apps.go` 头注释）；同一代内槽位唯一语义不变；
- `UNIQUE(project_id, idempotency_key)`：客户端重复提交同一操作返回同一结果；
- slot 是 agent 本地资源；若需要持久记录，仅可 `UNIQUE(node_id, slot_index)`，不做全局唯一；
- domain/hostname 由 PG 全局唯一约束分配；vsock CID 由 agent 在节点范围安全分配并报告。

### 4.3 Redis 投影

```text
route:{hostname}:{port} -> {route_generation, revision, backends[]}
routerev:{hostname} -> 高水位 revision（墓碑记忆，不随投影删除）
machine:location:{machine_id}:{execution_id} -> {node_proxy_endpoint, app_port, credential_ref}
resv:active -> 当前 reservation epoch 指针
resv:{epoch}:... -> epoch 命名空间内的预约键（node/project/op 承诺与索引）
operation:result:{operation_id} -> TTL cache
node:metrics:{node_id} -> latest sample
```

route 投影携带控制面分配的 hostname 级**单调 revision**：PG 表 `route_publication_revisions` 在与 route 发布相同的事务内 insert-on-conflict-increment 分配（leader 换届时新进程从本表继续，绝不回退）；catalog 的 `ReplaceHostRoutes` 用 Lua 以 `routerev:{hostname}` 高水位做 CAS，仅当 incoming revision 更高才整体生效（miss 视为 0）；edge RouteCache 回源取到比缓存更低 revision 的投影时拒绝回写（计 `firepaas_edge_route_revision_rejects_total`），继续服务缓存的 last-known-good。乱序发布或重放的旧快照因此不能覆盖或复活陈旧 backend（ADR-0038）。

reservation 键空间按 epoch 命名（`resv:{epoch}:...`），active epoch 由 `resv:active` 指针键经 Lua 原子读写；重建先写新 epoch 再切指针，旧 epoch 键靠 TTL 自然过期，全程不使用 KEYS/SCAN 全库扫描（ADR-0038）。`Acquire` 在脚本内先读 active epoch 决定承诺落在哪个命名空间；对外 API 签名不变。

**edge↔控制面 traffic token 耦合 SLA（R2 评审裁决：维持现状，仅文档化断流预算）**：edge 的 TokenClient 以 `(machine, execution)` 为键缓存 execution-bound credential，fresh TTL 30s；回源失败（控制面/PG 不可达）时，若缓存条目仍在 serve-stale 窗口内（默认 120s，`FIREPAAS_EDGE_STALE_WINDOW`，与 route 缓存同源）且 execution 匹配，降级复用 last-known-good，否则 fail-closed。因此控制面/PG 中断不超过该窗口时，已缓存的 `(machine, execution)` 继续服务；中断超过窗口后，edge 无法为冷（未缓存）的 `(machine, execution)` 对签发新凭证，而窗口内已缓存条目不受影响。该 serve-stale 窗口即数据面已文档化的控制面断流预算。

`route` 的 backend 只包含 readiness=true 且非 draining 的 execution；每次发布带 generation。edge 拒绝 execution 不匹配的 location，catalog miss 只触发受限查询/恢复，不自行猜测后端。`slot_ip`、netns 名称和 TAP 等均属于 agent 内部实现，不进入 edge/Redis 契约；edge 只能访问 `node_proxy_endpoint`。

**Redis 故障语义（可用性，非仅数据）**：Redis 不可用不改变业务结论（ADR-0003），数据面可用性由 edge 的 route generation 本地缓存承担——TTL 窗口内 serve-stale，超窗受控失败；恢复后由 controller 在声明的时限内重建投影。MVP 接受 Redis 单实例（AOF）；是否引入 sentinel 依据 M4 验收决定（mvp-plan §8）。

写路径不享受同等弹性：API 的 mutation/stream 限流（read 分类仍 fail-open）与运行时会话在 Redis 不可用时按设计 fail-closed（503）；即 Redis 中断时数据面继续 serve-stale，但写路径可用性降级，不得把上述语义误读为"Redis 丢失对写者免费"。

## 5. 多副本与滚动发布

外部 hostname 不映射单台 machine，而映射一个 route backend set：

```text
client → edge(hostname) → route generation → healthy backends
       → agent proxy → execution-specific VM
```

滚动控制器的顺序固定为：

1. 为新 deployment 创建所需 replica ordinal；
2. agent 观测到 VM `RUNNING`，健康检查通过；
3. 在一个事务中发布新的 route generation（同事务递增 `route_publication_revisions` 的 hostname 级单调 revision；catalog 与 edge 按 revision 拒绝乱序/重放的旧快照，见 §4.3）；
4. edge 按 generation 转发；旧 backend 标记 draining；
5. 到连接排空期限后停止/删除旧 execution；失败则保留旧 route generation。

readiness 的唯一来源是 agent 在 host 侧经内部 workload endpoint 执行的探针（M1 bridge guest IP、M3 slot IP，ADR-0008），随 observed state 上报；edge 不做应用层健康检查，不猜测后端。

MVP 最初以 round-robin 为基线；当前 edge 已按 ADR-0020 使用 least-inflight，并在并列时轮转。会话粘性、灰度权重和 WebSocket 长连接迁移仍是后续能力。发布与 scale/节点故障/再次发布的**组合场景决策表在 M3 冻结**（mvp-plan §7）：MVP 至少实现同一 app 同时只允许一个 rollout 的互斥。

## 6. agent 契约与内部信任边界

`protos/agent/v1/agent.proto` 是唯一控制面数据面契约。

- 所有改变状态的 RPC 带 `machine_id`、`execution_id`、`generation`、`operation_id`；agent 必须持久化幂等结果并拒绝 stale generation。
- agent operation ledger 至少持久化 `operation_id/machine_id/execution_id/generation/kind/request_hash/status/result`；同一 operation ID 携带不同 request hash 必须拒绝。结果采用原子写并在 machine 删除后保留一个可配置去重窗口。
- `CreateMachine` 的幂等单位是一个稳定 machine/replica，不是 deployment。
- control-plane→agent 与 edge→agent 都使用 mTLS workload identity；agent 按调用方身份授权 RPC。
- 5108 仅接受 control-plane；5107 仅接受 edge；主机防火墙进一步限制来源。
- registry 凭证使用短期 scoped token 或 agent credential provider；禁止在长期 RPC payload、日志或 Redis 中保存密码。
- secret 值与引用分离（ADR-0010）：`secret_refs` 引用、值仅在 `CreateMachine.secret_env` 一次性下发；observed state（agent 重启扫描重建的 `Machine`）不携带秘密。
- execution-bound proxy credential 仅在 `CreateMachine.proxy_credential` 单向下发；不得进入会被返回的 `MachineSpec`、Redis、日志或 operation result，agent 只保存验证材料/摘要。
- 运行时交互（logs/exec）属于 agent 契约：`StreamLogs`/`Exec` 流式 RPC 同样受 mTLS 身份与 execution fencing 约束，由控制面代理 CLI/用户调用（属运维通道，不是 app 流量）；`Machine.log_url` 仅指向归档日志，不承担实时通道。MVP 的 Exec 断线即终止，只保证会话创建幂等，不承诺输出续传或重新 attach。

mTLS workload identity 的实现现状（2026-09）：静态证书 + 调用方 CN 白名单 + `internal/security/mtls` CertManager 热重载（契约 C-1；agentd、edge-proxy、agentclient 均接入），各进程导出 `firepaas_tls_cert_not_after_seconds` 并配 30d/7d 到期告警（`iac/observability/prometheus-alerts.yml`）。证书轮换、per-node 身份与吊销仍为延期项，见 ADR-0006 后果更新。

## 7. 放置与恢复

控制面使用 Best-of-K 作为**软决策**；agent 以本机真实资源做硬准入。调度管线**先过滤后打分**（ADR-0009）：状态/资源/label/node_pool 硬过滤、DEPLOYMENT 尽力反亲和（候选不足可降级并记录调度事件）；CPU 超售比例、内存超售、K、Alpha 都是 P0 基准后的可配置值，初始 `R=4/K=3/Alpha=0.5/memory=1.0` 不是承诺值。

恢复优先 origin node；origin 不可达时：无状态 VM 可冷启动重建；node-local 快照仅作为加速，不提供跨节点恢复保证。

## 8. 网络与安全基线

- M1 即建立唯一正式流量边界 `edge → agent proxy → workload endpoint`；M1 的 workload endpoint 可由 bridge adapter 提供，M3 切换为 netns slot 时不改变 edge/catalog 契约；
- M3 起每个 VM 使用独立 netns slot、cgroup v2、nftables 默认拒绝宿主机与私网；
- 不做 overlay 或跨节点 VM 私网直连；跨节点业务流量经 edge/agent proxy；
- agent 以 root 运行，是受 mTLS、网络 ACL 和最小 RPC 授权保护的可信组件；
- MVP 面向受信内部租户，仍必须拒绝跨 project API/路由访问与 guest→host 访问；
- edge 入口：M1–M3 DNS 轮询、M4 keepalived VIP；TLS 由 step-ca 内部 CA 经 Caddy ACME 集成按需签发泛域名证书，客户端根证书预置是运维前置而不是平台功能（ADR-0011）。

## 9. SLO 适用范围

| 工作负载 | 目标 |
|---|---|
| 无状态、镜像已缓存 | cold start p95 < 5s（以 P0 实测为准） |
| 无状态、镜像未缓存 | cold start p95 < 60s（镜像/网络条件记录在案） |
| standby 可用且 origin node 健康 | resume p95 < 1s，autoresume p95 < 5s |
| 节点失联的无状态预热副本 | 检出 < 60s；检测后 ready < 120s 的目标仅适用于预热镜像 |
| node-local volume / snapshot | 不提供节点永久故障后的 RTO/RPO 承诺 |
