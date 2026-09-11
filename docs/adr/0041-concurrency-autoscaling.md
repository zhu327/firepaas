# ADR-0041：基于并发的 App 自动弹性（KPA 式，P0）

状态：已接受（2026-09-10 初稿；2026-09-11 评审修订 v2；2026-09-12 落地实现并通过独立审查）
关联：ADR-0003（PG 权威/Redis 投影）、ADR-0005（route catalog）、ADR-0007（单 leader 写者）、ADR-0015（rollout 状态机）、ADR-0017（auto-standby 与 scale-to-zero 边界）、ADR-0018（镜像预取）、ADR-0020（edge 并发控制与 least-inflight）、ADR-0022（多端口）、ADR-0023（capability）、ADR-0024（一次性 secret 下发）、ADR-0027（审计与指标基数约束）、ADR-0035（配额治理）、ADR-0039（scope 能力化）、ADR-0040（mesh 直达）；`docs/architecture.md` §4/§5。
修订：不修改 ADR-0015/0017/0020 语义；本 ADR 只新增“谁来写 `desired_replicas`”。信号跨 edge 聚合是 ADR-0020 显式延后的 v1.2+ 项，本 ADR 承接；ADR-0020 的 per-edge hard 语义、ADR-0017 的单机入睡语义不变。
计划口径：`docs/plans/2026-09-10-review-remediation.md` 把 autoscaling 标为 v2 范围，本 ADR 只提前“并发副本弹性”这一增量，不改变该计划其余非目标。
评审修订（v2）：修正 migration 号段事实（不得占用 0037/0038）、hostname 唯一性事实、per-edge 样本过期语义、目标代容量口径、手动接管与决策写入的并发写序、“零容量”与“信号丢失”的区分、指标命名与基数；补充跨 edge/东西向盲区、mixed-version 行为与验收矩阵。

## 背景

1. 现状只有手动弹性。`POST /v1/apps/{id}/scale`（`cmd/api/apps.go:scaleApp`，`1..100`）调 `store.SetAppReplicas`；`internal/controlplane/controller/apps.go:reconcileAppScale` 把目标 deployment 的 ordinal 集对账到 `desired_replicas`（缺建多删）。`desired_replicas` 的写者只有：创建默认、手动 scale、删除 app 置 0；全仓无 `autoscal/min_scale/max_scale/target` 实现。
2. 唯一可用的负载信号是 edge 的 per-machine inflight。`internal/edge/handler.go` 的 `inflightTracker.selectAndAcquire` 已实现 least-inflight + 随机抖动 + `HardConcurrency` 硬限（`Config.HardConcurrency ≤0` 归一 256）；`Counters` 已有 `proxiedReqs/hardRejected/req2xx-5xx` 与 `routeLookup/tokenFetch/upstreamRTT` 直方图。这是请求生命周期维度（含 WS/SSE 长连接），比 RPS 更接近真实负载（ADR-0020 §理由 3）。
3. CPU/内存 per-VM 管道不存在。`cmd/api/lifecycle.go` 注释自认“自动 idle 检测需要每机 usage 管道，v1 以显式 pause API + autoresume 交付”；`ServiceInfoResponse.usage` 只有节点级，无 per-execution 并发/CPU 可信源。P0 做 CPU 弹性是假数据。
4. `pause/standby` 不是弹性。`machine.Adapter` 的 `Pause/Resume` 与 agent proxy `GetEndpointForPort` autoresume 已有，但 `createApp:rejectSecretStandbyCombo` 与 `ErrSecretSnapshotForbidden` 禁止带 secret 的 execution standby；`Pause` 不改变副本数，machine 仍以 `desired_state=CREATED/RUNNING` 计入调度容量（`AllocatedByNode`），不省 placement 容量。ADR-0017 已把 standby 定义为“延迟优化 + 省 VMM”，不是“调副本数”。两者不能混为一谈。

## 目标 / 非目标

目标：

1. 给 `App` 加上 Knative-KPA + Fly-concurrency 形态的自动扩缩：按并发算 `want`，快扩慢缩，panic 快扩，发布互斥，信号丢时 fail-closed。
2. 只调既有 `desired_replicas`，复用 `reconcileAppScale / rollout / placement / quota / fencing` 全链路，不发明第二套副本模型。
3. 多 edge 可用：信号在 edge 侧产出、Redis 聚合，controller leader 侧决策。

非目标：

1. 不做 CPU/内存/QPS 预测/GPU/cron/jobs（无数据源，另立 ADR）。
2. 不做 pause 数量自动伸缩，不改 `Backend.Weight`（`routepublisher` 里 `Weight: 100` 是死字段，权重分流另立 ADR）。
3. 不做跨 cell/跨区弹性（单 leader + PG advisory lock 模型内闭环，见 ADR-0007/0034）。
4. 不做东西向流量的弹性信号：mesh/eBPF 直达不经 edge，P0 观测不到；`min_replicas=0` 仅对纯南北向服务声明安全（见 §4.4）。
5. 不改 deployment 不可变语义：策略跟 `app` 走，改策略不产生新 generation（区别于 `auto_standby/egress` 随 deployment 固化）。

## 决策

### 1. 策略归属与 API/DB

- 策略是 `app` 级一行配置，PG 权威，Redis 不保存策略。新增顺序 migration（截至 2026-09-11 工作树已占用到 `0038`：`0037_v15_fork_machine_uniqueness.sql`、`0038_v15_hot_path_indexes.sql`，均为未提交的 v1.5 文件；**本 ADR 的 migration 取落地时的下一个空闲号，不得复用 0037/0038**，示例 `0039_autoscale.sql`）：

```sql
ALTER TABLE apps ADD COLUMN IF NOT EXISTS autoscale_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS min_replicas int NOT NULL DEFAULT 1;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS max_replicas int NOT NULL DEFAULT 10;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS target_concurrency int NOT NULL DEFAULT 20;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS scale_down_delay_sec int NOT NULL DEFAULT 120;
ALTER TABLE apps ADD COLUMN IF NOT EXISTS panic_threshold double precision NOT NULL DEFAULT 2.0;
```

- 只加列 + 默认值（默认即当前行为，零回归）；`apps` 行锁短，可在线执行。回滚：`enabled=false` + 停 ticker；需要时后续 migration 删列。
- 约束（API 层校验，失败 400；store 侧读时同样 clamp，防脏行）：
  - `0 <= min <= max <= 100` 且 **`max >= 1`**（`max=0` 会让 enabled app 永久零副本）；
  - `1 <= target_concurrency <= 256`（推荐 `<= 64`，必须显著小于 per-edge hard 256 留 headroom）；
  - `30 <= scale_down_delay_sec <= 600`；`1.5 <= panic_threshold <= 5.0`。
  - `min=0` 即显式开启 scale-to-zero；默认 `min=1`。
- 端点（`cmd/api/main.go` 注册；`cmd/api/authm5.go` 必须**同时**登记 `routeScope` 与 project-scoped 两个表，未登记路由默认拒绝）：
  - `PUT /v1/apps/{id}/autoscale`（全量替换策略）：`deploy` scope；
  - `GET /v1/apps/{id}/autoscale`：`read` scope；
  - 关闭（`enabled=false`）时行为冻结在当前 `desired`，不自动回 1；开启不立即改写 `desired`，由下一拍决策按稳定窗收敛。
- 策略变更写 `user_events`（沿 `rolloutUserEvent` 模式）：enable/disable/参数变更各一条，`type=autoscale.policy`。
- 手动 `POST /v1/apps/{id}/scale` 与 autoscale 互斥：启用中手动 scale **隐含置 `enabled=false`**（手动接管语义），并在**同一条 UPDATE 内**写 `desired_replicas`；API 同时写 `user_events`（`type=autoscale.takeover`）。
- 写序/TOCTOU（v2 新增，冻结不变量）：
  - autoscaler 的写入一律用条件 UPDATE（乐观 CAS），不得无条件 `SetAppReplicas`：
    ```sql
    UPDATE apps SET desired_replicas=$3, updated_at=now()
    WHERE id=$1 AND deleted_at IS NULL AND autoscale_enabled AND desired_replicas=$2
    ```
    `RowsAffected=0` 视为“策略已变/手动接管/其他写者”，本拍跳过并计数，绝不覆盖；
  - 手动 scale/接管是单条 `UPDATE ... SET desired_replicas=$2, autoscale_enabled=false`；
  - 策略 PUT 与 CAS 的竞态由“CAS 失败即跳过”兜底；策略变更最迟下一拍（10s）生效。
- `store.App` 加同名字段，`appColumns/scanApp` 同步（`ListAppsFiltered` 形状不变）。

### 2. 信号管道：edge 推，Redis 聚，controller 读

现状断层：`inflightTracker/Counters` 全是 edge 进程内存，ADR-0020 已声明 per-edge 语义，controller 读不到，多 edge 无法求和。实现必须按以下口径编码：

- `inflightTracker.snapshot()` 是瞬时 per-MachineID 计数，不是 EWMA；10s EWMA 由新上报协程自建。
- `Counters.proxiedReqs/hardRejected` 是 edge 进程全局原子计数，无 hostname 维度；per-hostname 计数需新增。
- `catalog.Route` 的 Redis 投影无 `AppID`；PG `routes` 表有 `app_id`，但 edge 不连 PG，归属判定只能在 controller 侧做。

**记账点（每请求单归属，v2 精确化）**：信号一律按请求 Host 的 hostname 记账（与 `Route{Hostname}`、route cache key 同口径，不做额外规范化）。四个 per-hostname 计数：

- `served`：请求级 inflight。**在 `proxiedReqs.Add(1)` 同一判定点（选路成功且拿到 token）对该 hostname +1**，handler 出口 defer -1；内部 forbidden/transport 重试不重复 +1（同一请求只有一次“进入转发”）。该口径与机器级 inflight 的 acquire/release 生命周期解耦：retry 时机器计数先释放再获取，host 计数保持到请求结束。
- `served_rps`：同一判定点的请求数/采样窗（不是读全局 `proxiedReqs`）。
- `hard_rejected`：`writeSelectionError` 的 `errHardLimit` 分支记账；全局 `hardRejected` 保留，仅作跨 app 对账。
- `unserved`（P0 冷起信号）：hostname 有请求但**零容量**——route 查无（含 `ErrNotFound` 负缓存）或 `Backends` 为空（404 分支）或 `writeSelectionError` 的 `errNoEligible`（503）。**明确不计入**：pin miss（404，单机不可用不是容量）、hard limit（过载走 panic）、限流 429（edge 自保）、`ErrBeyondStale`/catalog/token 错误（控制面或依赖故障，按信号丢失处理）。零副本 app 的 route 键已被 `PruneRoutes` 删除（publisher 只从 serving machines 构建投影），此时 `served_*` 恒为零；没有 `unserved`，`min=0` 就是单向门，到零即永死。

**上报**：edge 新增 5s 上报协程（与 RouteCache/TokenClient 同进程；失败只记既有 `redisErrors`，不阻塞转发）：

- `HSET autoscale:{hostname} {edgeID} <json>` + `EXPIRE 20s`；`<json> = {ewma, rps, hard_rejected, unserved, ts_ms, win_ms}`。两次写建议用单个 Lua/EVAL 原子完成（避免 crash 留下无 TTL 键）；TTL 靠过期自清，不进 `keepRoutes`/`PruneRoutes`，无需清理。
- edgeID 取稳定标识：`FIREPAAS_EDGE_ID`，缺省回退 `FIREPAAS_MESH_EDGE_ID`/hostname；同 key 内 field 冲突视为配置错误（后写覆盖）。
- 短窗口计数（rps/hard_rejected/unserved）上报**最近约 2×poll 的滚动和**（约 10s），保证 controller 10s 轮询不漏掉 5s 窗口；计数被重复观测方向保守，可接受。`ewma` 为 5s 采样的 10s EWMA。
- 记账表沿用 `lru.go` 有界口径（独立容量上限，env 可配）；**空闲 hostname 的零值样本不得按时间淘汰**（否则 idle app 信号丢失会冻结缩容），仅在容量压力下 LRU 逐出。满则逐出最旧，不无界增长。
- 未知 Host 的 `unserved` 也进同一有界表；reporter 只写该表中的 hostname，写入速率与表容量同阶。Host 头扫描只能污染 edge 本地有界表与有一定 TTL 的 Redis 键，controller 只读已知 app 的 hostname（`apps.hostname` 反查），未知 hostname 的 key 永不参与决策。

**controller 聚合（每 10s，只读 `autoscale_enabled` 的 app）**：`HGETALL autoscale:{hostname}`，仅使用**fresh 字段**（`-5s <= now-ts_ms <= 20s`，容忍小时钟偏差与轻微未来时间）：

- `served = sum(ewma_fresh)`；`unserved = ceil(sum(rolling_unserved_fresh)/target)`（窗口计数折算成并发量级；低流量下偏保守，多给不少给）；`total_rps = sum(rps_fresh)`；`panic_reject = sum(hard_rejected_fresh) > 0`。
- **新鲜度与缩容的 fail-closed 规则（v2 修正）**：
  - 没有任何 fresh 字段（key 不存在 / 全字段过期 / Redis 读失败）→ **hold**，不写 `desired`；
  - 过期且全零的字段 → 视为缺席（可忽略）；
  - 存在**过期且非零**的字段 → **禁止缩容**（hold shrink），扩容仍可用 fresh 字段决策。理由：edge 与 Redis 分区但仍在 serve-stale 服务时，其 inflight/rps 不可见；此时缩容会删掉它正在服务的副本。
  - 同一 key 内字段由 live edge 持续刷新时，死 edge 的过期非零字段不会自动消失（key TTL 被刷新）——这是**有意 fail-closed**，代价是该 app 在 reporter 恢复前不缩容；排障入口是 `/metrics` + 手动关闭 autoscale。不做时间过期丢弃（会把分区 edge 的服务量丢掉）。
- **“零容量”不是信号丢失**：`Backends 为空`/`desired==0` 是合法状态，不得作为 hold 条件（与 §4.4 冷起直接冲突）。controller 侧不读 route 投影做决策；容量来自 PG machines（§3）。
- 不走 Prometheus query（控制循环不能依赖抓取），不走 PG 信号表（edge 不连 PG）。hostname→app 用既有 `apps.hostname`（`0001_init.sql` 唯一约束）。
- 明确盲区：
  - 限流 429 不产生负载信号（edge 自保，有意）；
  - mesh/东西向流量不经 edge，P0 不可见（§4.4）；
  - mixed-version：控制器先上、edge reporter 后上时，无 fresh 字段 → 全量 hold（不缩容），edge 铺开后自动恢复；回滚控制器即可。

### 3. 决策循环：leader 内新 ticker，只写 desired

- `controller/controller.go:Run` 加 `autoscaleTicker`（默认 10s，与 opTicker 1s / syncTicker 5s 并列，首拍延后一拍即可）；`reconcileAutoscale()` 在新文件 `internal/controlplane/controller/autoscale.go`，决策段为纯函数，便于单测。
- 决策状态**不持久化**：`scaleDownSince[appID]`、`quotaFrozenUntil[appID]`、预取去重表均为 leader 进程内有界 map；leader 切换/进程重启后重置（重置方向 = 推迟缩容，保守）。ADR-0003 语义不变：PG 仍是 desired 唯一权威。

```go
for app := range ListApps() {                       // 失败 → 全量跳过，fail-closed
  if !app.AutoscaleEnabled || app.Deleted { continue }
  target, err := targetDeployment(app); if err != nil || target == nil { continue }
  if rl, err := ActiveRolloutForApp(app.ID); err != nil || rl != nil { continue }
  sig, ok := aggregateFresh(app.Hostname); if !ok { continue }   // 无 fresh 字段 → hold
  ready   := count(targetMachines, machineServing && !nodeDraining)
  warming := count(targetMachines, notServing && desiredState!=DELETED && withinCreateTimeout)
  eff := ready + warming                           // 目标代口径，冷启中不重复下单
  load := sig.served + sig.unserved                // 无容量流量也是负载；到零后靠 unserved 冷起
  want := ceilDiv(load, app.TargetConcurrency)
  want = clamp(want, app.MinReplicas, app.MaxReplicas)
  if eff > 0 && (sig.panicReject || load > app.PanicThreshold*float64(app.TargetConcurrency*eff)) {
    want = min(app.MaxReplicas, max(want, ceil(app.PanicThreshold*eff), eff+2))  // 只放大
  }
  if frozen(app.ID) && want > app.DesiredReplicas { record(hold, "quota"); continue }
  switch {
  case want > app.DesiredReplicas:                 // 快扩：立即
    setDesiredCAS(app.ID, want, app.DesiredReplicas)
    maybePrefetch(app, target)                     // 尽力而为，不阻塞
    resetScaleDown(app.ID)
  case want < app.DesiredReplicas:                 // 慢缩：稳定窗全程成立
    if !shrinkable(sig) { record(hold, "stale_signal"); continue }
    if !scaleDownElapsed(app.ID, want, now) { continue }
    if want == 0 && sig.totalRPS > 0 { resetScaleDown(app.ID); continue }  // 在飞请求
    setDesiredCAS(app.ID, want, app.DesiredReplicas)
    resetScaleDown(app.ID)
  default:
    resetScaleDown(app.ID)
  }
}
```

- `targetMachines` 只含 `m.DeploymentID == target.ID` 的目标代 machine；`ready` 同 publisher 口径（`machineServing`：RUNNING|PAUSED + READY|UNCONFIGURED）；`warming` = PENDING/INITIALIZING（或已 RUNNING 但未就绪）且创建/重启证据在 `AgentRPCTimeout` 内，超龄的不占有效容量。DRAINING 节点上的 machine 不计入 `ready`。
- `scaleDownElapsed`：`want < desired` 首次出现时记 `scaleDownSince=now`；同一 want 持续 `scale_down_delay` 后放行，**直接缩到 want**（KPA 稳定窗）；任何 `want>=desired`、信号 hold、策略变更、手动接管、rollout 出现/结束都清零计时。
- 参数初值：`poll 10s / 扩 0s / 缩 120s / panic 2.0x / target 20`；决策一律读列值，不硬编码。`target=20` 取 per-edge hard 256 的约 1/10，给首请求唤醒与 `create 10s~2m（AgentRPCTimeout）` 留缓冲。
- 扩容顺手预取：复用 `prefetchCandidates` + `placer.PrefetchTopK`（controller/apps.go 已有），但**不能直接复用 rollout 级 `prefetchedRollouts` 去重**；需按 `deployment_id` 独立的有界去重表并限频，失败不阻塞、不改变决策。
- `maybePrefetch` 只在 `want` 增加时触发；缩容不预取。
- 每次**状态变化**（扩/缩/冻结/解冻/手动接管）写 `user_events`（`type=autoscale.decision`，`details={from,to,reason,sig{served,unserved,rps,hard_rejected,ready,warming}}`，沿 `rolloutUserEvent` 模式）；hold 只计数不落库（10s 一拍全落库会淹掉该表）。**不写 `scheduler_events`**：它是 placement 内部审计通道，autoscale 是 app 语义决策，`user_events` 已够用。
- 指标 label 收敛（ADR-0027 §8）：`firepaas_autoscale_decisions_total{result}`（result ∈ scale_up|scale_down|hold_signal|hold_rollout|hold_quota|conflict|error）；不设带 `app` label 的 desired gauge（无 label 的 gauge 会互相覆盖，带 app 又高基数），desired 排障走 `user_events.app_id`。

### 4. 护栏（逐条冻结）

1. **发布互斥**：`ActiveRolloutForApp != nil`（PREPARING/CUTOVER/ROLLING_BACK）时跳过；查询失败 fail-closed 跳过。`apps.go:rollingLimit` 已让发布中 scale 只扩新代 batch，autoscaler 再改 `desired` 会放大 batch，P0 直接冻结（ADR-0015 S8“作用于目标代”不变，只是不由 autoscaler 驱动）。
2. **冷启动去重**：`warming` 计入 `eff`，否则 `AgentRPCTimeout=2m` 窗口内重复下单；`reconcileAppScale` 的 ordinal 存在性判定本来就是第二道去重。`PendingUsageByNode` 与 Redis 软预约（`reservations.AcquireR` / `ErrProjectQuota`）仍是唯一准入，autoscaler 不加第二套。
3. **快扩慢缩**：扩立即，缩要求 `scale_down_delay` 全程成立（KPA 稳定窗），缩容走既有 `reconcileAppScale` 的“超 N ordinal 删”路径，与 `CUTOVER drain 30s + edge fresh 5s/负缓存 5s/stale 120s` 对齐，不与缓存打架。
4. **到零与冷起共用同一公式，不用 pause**：`min=0` + 稳定窗内 `want==0`（`served+unserved==0`）**且** `rps==0` → 条件 UPDATE 写 `0`，复用 scale-down 删除（`enqueueUserDelete`，opID 嵌 execution 后缀）。流量（无论 served 还是 unserved）到达即 `want>=1`，自然阻止缩零。到零后靠 `unserved>0` 冷起（route 键已被 prune，404 → unserved）。secret 应用到零只能冷起（ADR-0024 禁 standby 快照是安全不变量，不为省钱破）。
   - 秒级心跳应用满足不了 `rps==0` 会永不缩零——P0 接受（它确实在服务流量）。
   - **东西向盲区（v2 明示）**：mesh 直达/VM→VM 流量不产生 edge 信号；`min=0` 时东西向消费者没有冷起信号，可能长期 404。P0 声明：`min=0` 仅对纯南北向服务安全；东西向服务的信号源（`.internal` DNS 投影或 mesh endpoint）另立 ADR。
   - 零副本下的请求在冷起完成前会持续 404/503；P0 不做请求缓冲（没有 activator），慢可接受、卡死不可接受。
5. **配额/资源封顶，只冻扩不冻缩**：`placement.Place` 的 `reservations.ErrProjectQuota`（`isQuotaError`）与 `filter_rejection:resources` 是唯一真值；autoscaler 观测到**本 app** 的 `quota.rejected` 或连续 `resources` 拒绝后冻结扩容并写事件，**缩容通道保持开放**（缩释放配额，是自愈方向）。冻结带超时（默认 `10×poll=100s`，controller Config 可配），超时自动解冻重探。信号来源可用同 leader 进程内的拒绝记录（与 controller 派发同一进程），不要求持久化、不要求每拍扫 `scheduler_events`；冻结在 leader 切换后重置（重探一次是安全的）。
6. **钉扎/调试不干扰**：`X-Firepaas-Pin-Machine` 只影响单次 `selectBackend`，请求照常计入 hostname 竞争（pin 不豁免 hard limit，ADR-0020）；DRAINING 节点的 backend 不计入 `ready`。
7. **多 hostname/多端口不 double-count**：现状 `apps.hostname` 单列 + `UNIQUE`，一 app 一 hostname（迁移 0003 只放开了 routes 表未来多 hostname 的可能，App 模型未放开）；machine.Hostname 来自 app.Hostname。edge 按请求 Host 单归属记账（§2），controller 对一 app 的全部已知 hostname 求和，故每请求只被一个 hostname 计一次、每个 hostname 只被求和一次。未来一 app 多 hostname 时该求和天然成立；对应验收改为决策函数单测（e2e 当前无法构造多 hostname）。
8. **手动接管原子性（v2）**：手动 scale 在一条 UPDATE 中写 `desired_replicas` 且置 `enabled=false`；autoscaler 用条件 UPDATE，永远不会覆盖接管结果。接管事件可审计。
9. **策略变更只影响下一拍**：`PUT` 不立即改写 `desired`；`max` 调小后由下一拍稳定窗收敛，避免 API 请求中间态破坏稳定窗。

### 5. 可观测与审计

- 用户事件（稳定契约）：`autoscale.policy`（enable/disable/参数）、`autoscale.takeover`（手动接管）、`autoscale.decision`（scale_up/scale_down/freeze/unfreeze，`details` 含 from/to/reason/sig）。hold 不落库，只进指标与 debug 日志。
- edge `/metrics` 复用 `Counters.WritePrometheus`，新增（不带 hostname label）：
  - `firepaas_edge_autoscale_reports_total`（成功上报次数）、`firepaas_edge_autoscale_report_errors_total`；
  - `firepaas_edge_unserved_requests_total`（零容量请求数，冷起链路排障）。
- controller 新增 `firepaas_autoscale_decisions_total{result}`；沿用既有 `firepaas_filter_rejections_total` 判断配额/资源冻结。
- controller 日志在每次决策与 hold 时记录 app/hostname/sig/reason（hostname 不是秘密，合法；不得记录任何 secret/token）。
- route 重建沿用 `buildRoutes`（autoscaler 只写 `desired`，路由仍由 `syncObserved+buildRoutes` 推导，不直写 Redis，守 ADR-0003 PG 权威）。

## 理由

1. **并发是唯一诚实信号**：edge 已有、请求生命周期口径、WS/SSE 成立；CPU/内存无 per-VM 源，RPS 对长连接失效。抄 KPA（`ceil(avg_inflight/target)` + panic + 稳定窗）+ Fly（soft=target/hard=256 双层）是最小改动，不是风格偏好。
2. **只写 desired 是风险最小**：`ordinal 建删、幂等键（op-{machine}-{exec8}）、墓碑复活、fence CAS、drain` 全复用；autoscaler 无持久化状态、可整段关闭（`enabled=false` 即回手动），故障半径 = 一次条件 UPDATE。
3. **Redis 聚合是唯一不倒退的链路**：edge 不连 PG、controller 不抓 Prometheus；`autoscale:{hostname}` 沿既有 Redis 投影族（`route:{hostname}:{port}` 无 TTL 靠 rebuild/prune；`dns:internal:*`/`mesh:endpoint:*` 带 TTL）新增一个**带 20s TTL 的瞬时信号族**，单点故障只导致“保持不动”而不是“删光”。per-field ts 是跨 edge 聚合的正确性前提。
4. **与 0017/0020 正交**：0017 管单机入睡/唤醒延迟，0020 管单次选路与过载拒绝（per-edge hard），本 ADR 管副本数。三者通过 `machineServing`（PAUSED 计入可服务）与 `hardRejected→panic` 接缝，不互相改语义。按请求 Host 记账而不是复用 machine 视图，保证 retry 与多端口场景不重复计数，并为未来多 hostname 留出正确形状。

## 后果

- migration 只加列 + 默认值，可在线；回滚 `enabled=false` 或删列/回滚二进制（Redis `autoscale:*` 自然过期，无需清理）。
- 新代码：
  - `internal/controlplane/controller/autoscale.go`（纯决策函数 + 有界状态）+ `Run` ticker；
  - edge 上报协程与四个 per-hostname 计数（`internal/edge/autoscale.go` 或并入 handler，`cmd/edge-proxy/main.go` 装配）；
  - `PUT/GET autoscale`（`cmd/api` + `authm5.go` 两表登记）+ 手动 scale 原子接管 + 条件 desired UPDATE（store）；
  - 事件类型常量与指标。
- 落地顺序（每步可独立回滚）：① migration + store + API（默认关，零行为变化）；② edge reporter + 记账（只写 TTL 键，旧 controller 不读）；③ controller 决策循环（默认对全 app 生效但 enabled 默认 false）；④ e2e/runbook；⑤ 按 app 开启。控制器领先 edge 上线时全量 hold，安全。
- 文档同步：`docs/architecture.md` §5 增加“desired 的自动写者”一句；`internal/edge/README.md` 与 `internal/controlplane/README.md` 补 reporter/决策循环；README ADR 计数更新；`docs/runbook-operations.md` 增加 autoscale 不扩/不缩的归因决策树（先看 `user_events`，再看 edge/controller 指标，最后查信号 key 新鲜度）。
- 风险接受：
  - `10s` 粒度对秒级突发仍慢（panic 只缓解不根治；突发吸收仍靠 `hard 503 + Retry-After`，与 ADR-0020 同风格）；
  - 采样、per-edge EWMA 与 least-inflight 在百副本下抖动会传导给 `want`（EWMA + 稳定窗压制，不根治）；
  - 分区 edge 的过期非零字段会冻结缩容（fail-closed 的代价，排障见 runbook）；
  - Host 头扫描可能逐出记账表条目，导致该 app 短时信号丢失（只影响收敛，不影响转发）；
  - 429 与东西向流量不产生信号（§2/§4.4 明示）；
  - `unserved` 折算在冷起风暴下会偏保守地多给副本（受 max/quota 封顶）。
- 高风险改动（公开 API、migration、并发写者、Redis 契约）：完成前必须指定一个独立 `code-reviewer` 审查 changeset（AGENTS.md）。

## 回滚

1. 逐 app / 全量 `autoscale_enabled=false`：行为立即回到当前 `desired` 冻结，手动 scale 可用；
2. 停 ticker（二进制回滚）：`autoscale:*` 键 20s 内自然过期；
3. 需要彻底移除时，后续 migration 删列（forward-only；已发布 migration 不重写）。

## 验收线（P0 关闸）

1. **阶跃 2x**：并发负载阶跃 2 倍后 60s 内 `desired` 跟上（10s tick + 稳定窗不阻塞扩容），期间 `hardRejected` 不持续增长。
2. **归零与冷起**：`min=0` 时约 2min（120s 稳定窗 + tick）后 `desired=0`；再打流量时 `autoscale:{host}` 出现 fresh `unserved>0` 且 30s 内 `desired>=1`；冷起期间客户端 404/503 可见但系统不卡死。
3. **信号丢失不缩容**：key 删除 / 全字段过期 / Redis 读失败 / 存在过期非零字段时 `desired` 不变（单测 + e2e 必过）；零容量（`desired=0` 或全 backend 不可用）不被误判为信号丢失。
4. **发布冻结**：PREPARING/CUTOVER/ROLLING_BACK 期间 `desired` 不变（含 rollout 查询失败时）。
5. **配额冻结**：`quota.rejected` 后不再加副本；缩容仍可用；冻结超时（默认 100s）自动解冻重探；事件可见。
6. **多 hostname 不翻倍**：决策纯函数单测——同一 hostname 集合（含合成多 hostname）的 served/unserved 求和等于逐 hostname 之和，不随 hostname 数放大（当前 e2e 不可构造多 hostname，见 §4.7）。
7. **手动接管**：`POST /scale` 后 `enabled=false` 且 `desired` 不再被改写；与 autoscaler 并发交错时 CAS 不覆盖接管值。
8. **并发写序**：条件 UPDATE 在策略变化/接管时的 `RowsAffected=0` 路径有单测；autoscaler 不覆盖手动值。
9. **校验**：`min>max`、`max=0`、`target<=0`、`target>256`、`delay`/`panic` 越界一律 400；`max_replicas>100` 拒绝。

## 验证矩阵

| 层 | 内容 | 命令/证据 |
|---|---|---|
| 决策纯函数 | panic 双路、稳定窗、信号丢/过期非零、零容量与冷起、发布互斥、配额冻结/超时、warming 超龄、clamp、CAS 冲突 | `go test ./internal/controlplane/controller/...` |
| edge 记账 | 每请求一次（含 retry）、unserved 分类排除 pin-miss/hard/429/beyond-stale、EWMA 数学、有界表、上报键 TTL | `go test ./internal/edge/...` |
| store | 条件 desired UPDATE、策略 CRUD、migration 顺序 | `go test ./internal/controlplane/store/...` |
| API/鉴权 | PUT/GET scope（deploy/read）、400 校验、手动接管原子性 | `go test ./cmd/api/...` |
| 契约/回归 | `make check`（build+vet+test+tidy-check）；有 PG/Redis 时 `make check-lab` | CI 等价入口 |
| e2e | 阶跃、归零+冷起、信号丢失、发布冻结、配额冻结、手动接管 | `scripts/lab/e2e-autoscale.sh`（新增；需 lab PG/Redis/agent） |
| 独立审查 | 高风险 changeset 的完整审查 | `code-reviewer`（AGENTS.md 要求） |
