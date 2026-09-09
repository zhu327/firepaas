# G2 实施设计（ADR-0040 §14–§19，DRAFT FOR REVIEW）

状态：设计评审中 —— **未开工**。高风险变更（edge 鉴权迁移、mTLS 边界扩展）按 AGENTS.md 要求，先确认设计+回滚方案，实施后加一次独立 `code-reviewer`。
前置：G1 收尾（hypeman tag → adapter 注入 → spike stage1/2）。

## 现状基线（2026-09-08 审计）

- ✅ proto：`ServiceSpec.mesh_direct`、`EastWestPolicySpec`、capability（`Requires(mesh→ebpf)`）、`FabricService`、校验。
- ✅ PG：`workload_identities` / `ipam_allocations` / `wg_peers` / `eastwest_policies` + store fencing；T4c 派发分配/删除释放。
- ✅ 快照下发：reconciler → `ApplyFabric` → agent ledger/持久化 + underlay 生效 + ipcache 填充。
- ✅（G2a 落地）EastWest CRUD API + 全表随快照 + eBPF policy/policy_ports 全量替换（见切片）。
- ❌ catalog `Backend` 无 `ula/identity_id/generation`；edge 只走 `node_proxy_endpoint`（`:5107`）。
- ❌ 无 `.internal` DNS、无 edge 经 mesh 直达、无 fabric ingress 终结器、无 Redis mesh 投影。

## 切片

### G2a EastWest 策略执行（数据面先行，不碰南北）✅ 2026-09-09

- 控制面：租户 HTTP CRUD `PUT/GET/DELETE /v1/eastwest-policies`（dst 侧归属授权：受限 key 只能开放自己 project；generation 服务端派生）；全表随 fabric 快照下发（`ApplyFabricRequest.eastwest`，空表 nil = 默认 deny）。
- Agent：`ApplyFabric` Effect 内（underlay 之后）按快照 `eastwest` ∧ identities `mesh_direct` 计算 `policy{src_id,dst_id}` + `policy_ports{src,dst,dport}` 条目全量替换 eBPF map（`api.FabricPolicyWriter` 插件缝；nft emergency 后端为 nil 跳过）。
- eBPF C 增量：`policy_ports` 精确端口表（逐端口放行，非 TCP/扩展头无端口语义 → deny）；`policy_val.ports_bm` 保留字段退化。netns 套件扩展断言（放行端口通/未放行端口拒/快照清空拒）。
- deploy `mesh_direct`：service body → `appcommand.Service` → `store.ServiceSpec` → `pb.ServiceSpec` → 快照 identities（`FabricIdentitiesForNode` SQL 取 `services->0->>'mesh_direct'`，三方同口径）。
- 回滚：删 policy 行 → 下一快照周期条目消失（默认 deny）；`mesh_direct=false` 逐服务回退。
- 验证：`contracts` + 两端单测 + netns 实机套件（root）+ `GOWORK=off make check` 全绿。

### G2b catalog ULA hints（只加字段，不改选路）✅ 2026-09-09

- `catalog.Backend` 加 `ula/identity_id/generation`（`omitempty`，无提示 backend 的 JSON 与旧形态字节一致——测试锁定）；`ReplaceHostRoutes` Lua 透传（revision/`routerev` 高水位逻辑不动，新字段随条目 JSON 原子写入，旧 revision 重放带提示也拒）。
- routepublisher：`Store.FabricIdentities`（集群在役身份，与 fabric 快照同源）→ `Derive` 只为 `mesh_direct` 服务且在役（backend set 同源门控：非 serving/draining 已 continue）填提示；无身份（未入 mesh/分配未落）= 零值；FabricIdentities 查询失败降级记日志（缺提示只影响未来 mesh 路径）。
- PG `route_backends` 不存提示（权威在 ipam_allocations，发布器每次从在役查询活取；避免双写同步问题）；edge 用 `catalog.Backend` 直接反序列化，未知字段忽略（G2d 前不读）。
- 回滚：停填 → edge 回落旧字段（提示字段 omitempty，旧 edge 二进制直接兼容）。
- 验证：publisher 单测（mesh_direct 填/非直连零值/无身份零值）+ catalog 单测（旧 JSON 字节形态、往返、高水位拒重放）真 Redis 过。

### G2c `.internal` DNS（控制面权威，节点本地服务）✅ 2026-09-09（真机验收）

- 控制面：`Derive` 同源产出 `InternalDNS`（mesh_direct ∧ READY 非 draining ∧ 在役身份，与 backend ULA 提示同一门控）；`dns:internal:{app}.{project}` Redis 投影（TTL = serve-stale 预算 120s，与 FIREPAAS_EDGE_STALE_WINDOW 同值；发布器每轮刷新，leader 死亡→键过期→reconciler 摘除→节点停服）；fabric 快照新增 `dns` 全表（`DnsRecord{name,aaaa,generation}`，哈希覆盖、变更推新代、读失败降级跳过不断发）。
- Agent：`dnsserver`（UDP+TCP :53，监听节点 ULA，随快照 NodePrefix 幂等绑定/重绑）；区内 AAAA 命中/未命中 NXDOMAIN/非 AAAA 空应答（IPv4-only 不支持）；区外 REFUSED（split-horizon：公网名落到第二 nameserver，公网解析不受影响）；快照冻结语义与 fabric 其余投影一致。
- guest resolv.conf：`IPv6DNS = 节点 ULA` 注入（adapter `SetFabricDNSAddr`）；hypeman fork init v6 nameserver 排首位（NXDOMAIN-first 语义：公共 resolver 不能先答 .internal）。
- 真机修复：健康探针回程被 eBPF private-drop 误杀（新增 `host_addrs4` + ACK 识别 host 发起连接回程，无 conntrack kfunc 内核兼容）；recreate 路径丢 health_check/egress（重建副本探针全丢）；slot 透明代理 DNAT 误插 host 发起转送流（`iifname != veth` 守卫）；nft fallback ip6 INPUT 杀 NDP（NDP 类型放行）；mesh 节点回落 nft = 策略静默失效（不上报 fabric 公钥不入 mesh）。
- 验证：DNS/发布器/catalog/reconciler 单测（含同源门控、TTL 预算、降级）+ root-netns 套件；真机：in-guest `wget http://dns-h.dev.internal/` exit 0（解析→mesh 连接→HTTP 200），公网名正常回落。

### G2d edge 经 mesh 直达（最高风险，最后做）✅ 2026-09-09（真机验收）

- 控制面：edge hub 注册为具名 WG peer（`FIREPAAS_MESH_EDGE_PUBKEY/ENDPOINT/ID`，与 workload 身份域隔离 = WG 密钥域）；`mesh:peer:{node}` / `mesh:endpoint:{machine}:{execution}` Redis 投影（reconciler 写，TTL 120s stale 预算；endpoint 含节点 ULA + ingress 端口 + workload ULA）。
- Agent：`fabricingress` 终结器（[节点 ULA]:5109，随快照幂等绑定）——凭证是唯一路由依据（`LookupByDigest` 反查 execution，无 X-Firepaas-Machine/Execution 头）；端口参数 `X-Firepaas-App-Port`（非路由依据）；转发复用 proxy.Proxy（endpoint 解析/autoresume/重试语义一致）；peer 准入由 WG 加密承担（eBPF 不查本路径，§14）。
- Edge：`edge/mesh` 包（WG hub 密钥管理 + peer 周期同步 + endpoint TTL 缓存 last-known-good）；`FIREPAAS_EDGE_MESH_DIRECT` 开关（默认关，回滚即关全量回落 legacy）；直达优先（backend ULA 提示存在），拨号 3s 超时快速回落（节点失联 SYN 黑洞不吞请求预算——真机实测踩坑）；指标 direct/fallback/errors 三计数分母一致。
- legacy `:5107` 保留；回程仍走 edge；relay 回落（东西向经 edge 中转）策略不等价已声明（中转身份 ≠ 原始 workload 身份，容量预算 = edge hard concurrency，回落打点 firepaas_edge_mesh_direct_fallback_total）。
- 真机修复：publishRedis 转换层丢 backend ULA 提示（G2b 回归——Derive 产出但 catalog.Backend 转换漏字段，加 Rebuild 级回归测试锁定）；直达拨号无超时（回落前 OS 默认 ~130s）；iptables 拐 v6 需 ip6tables（脚本修复）。
- 验证：mesh/edge/fabricingress/catalog/fabric 单测 + 真机 stage3（spike 脚本）：直达 200 + 指标计数 + 阻断 ingress 端口后 3s 回落 legacy 200（回滚语义）。

## 决策结论（2026-09-08 已定）

1. **EastWest API 面 = 租户 HTTP API**（project 隔离 + 审计）：与 ADR-0039 租户自助方向一致；fpctl 只读观测后续补。
2. **G2d 门槛 = G2a–c 验收 + spike stage2 证据齐备**：G2d 回退面最宽，必须等直连证据落地。
