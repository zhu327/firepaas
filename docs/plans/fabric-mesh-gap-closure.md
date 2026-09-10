# Fabric mesh 差距收敛计划（2026-09-10 核实后制定）

## 1. 已核实的落地边界（代码为准）

- Underlay：`internal/agent/network/wg/wg.go` — peer 按 node /64 聚合全量 `syncconf` 替换
  + `PersistentKeepalive=25s`（注释明确“非失败检测”）；无 handshake-age 采集、
  无 Healthy/Suspect/Failed 状态机、无自动摘除/回切。`soak-fabric.sh` 仅记录
  handshake-age，从不据此动作。
- Fencing：单 `fabric_generation` 高水位（`internal/agent/state/fabric.go` 全量快照
  + `operation_id` ledger 幂等；控制面 `fabric_versions.generation` 只升不降）。
  内容哈希是**单一** `snapshotHash(prefix, peers, identities, eastwest, dns)` —
  任意 DNS 变化都会 bump 总 generation 并重推 WG/identity/policy 全集。
- 发布语义：控制面 10s 周期 reconciler + unary `ApplyFabric`（`internal/controlplane/fabric/fabric.go`）。
  可观测性只有“推送成功记 Info / 失败记 Warn”；无 desired/sent/ack/applied 分层水位，
  无 per-node `fabric-status` 接口。agent 侧 `Current()` 有快照但无对外状态面。
- eBPF 策略：`ApplyFabricPolicy` 为 `clear(ipcache,policy,policy_ports)` + 全量重写
  （fail-closed 但有短暂全拒绝窗口）。已有 `PolicyGen` gauge，无条目计数、无失败计数、
  无 active-generation 一致性校验。双 map 原子切换未实现（需 BPF 对象变更）。
- 服务发现：`.internal` 只回 AAAA（契约 `DnsRecord.aaaa`），无 A/SRV 兼容路径；
  DNS 随主 generation 全量替换（serve-stale 仅在 Redis 读失败时用 `dnsCache` 顶一轮）。
- GA 证据：`docs/ga-observation-scorecard.md` 全部 `NOT ASSESSED`（含 fabric datapath /
  chaos / soak）。不得据此宣称生产就绪。

## 2. 优先级（按“能否上线、安全运维”排序）

- P0-1 故障检测与可观测摘除：先做 handshake-age 采集 + 健康分级暴露（不自动改路由），
  自动摘除/回切/防抖需独立设计 + 双节点真机 chaos 证据，**本次不做自动动作**。
- P0-2 版本与发布状态分层：proto 拆分 `underlay/identity/policy/dns_generation` 是
  高风险契约变更（需 `make proto` + 两端兼容 + migration 式发布门禁），**本次不拆契约**；
  先做控制面 section-hash + per-node desired/applied/last-error 状态面（纯加法）。
- P0-3 eBPF 发布可观测：双 map 原子切换需 BPF/TC 对象变更 + 内核兼容矩阵，
  **本次不做**；先做 `FabricStats`（generation + 三表条目数）与 apply 失败计数（纯加法）。
- P1 规模化（增量/分层 snapshot、多 endpoint/relay、policy 编译器）：需契约与存储变更，
  列入后续，不在本次。
- P2 生态（`fpctl fabric explain`、SRV/A 兼容、L7 网关）：明确不引入半成品 Istio；
  本次只为 explain 预留 section-hash 与 stats 数据源。

## 3. 现在值得做（本次实施）

1. 控制面 `Reconciler` 增加 section-hash（peers/identities/eastwest/dns 独立哈希，
   日志打出变化面）+ 线程安全 `Status()/Statuses()`（desired/applied/lastHash/
   sectionHashes/lastError/lastSuccess/lastAttempt）。不改 fencing 语义。
2. agent WG 增加 `PeerHealth`（`wg show <iface> latest-handshakes` 解析 +
   Healthy(<60s)/Suspect(<180s)/Failed 分级 + from-never-seen=Unknown）。
   只观测不上报、不改路由；注释明确 handshake≠业务可用，ULA 探测仍为 TODO。
3. eBPF `Backend` 增加 `FabricStats()`（generation/三表条目数/更新时间）+
   `FabricApplyFailures()`（校验与落表失败计数）。`ApplyFabricPolicy` 成功时更新、
   失败时计数；不改 clear+rewrite 语义（原子切换仍为后续项）。

## 4. 暂时不该引入

- VXLAN/Geneve 叠加层；完整 Istio/sidecar；复杂 selector 下沉 eBPF；proto 子 generation
  拆分；WG 自动摘除/relay 自动切换；A 记录 hairpin 回 edge；BPF 双 map 切换 —
  均需独立设计、兼容矩阵与 chaos/soak 证据，待 P0 观测项落地并跑出双节点故障数据后再议。

## 5. 验证

- `go build ./...`，`go test` 触及包（fabric 无 PG 时单测跳过需注明），`go vet` 触及包。
- 未验证：双节点真机断电/断网/分区、72h soak、BPF 内核矩阵 — 仍为 NOT ASSESSED，
  见 scorecard，不在本变更中宣称。
