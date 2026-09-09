# ADR-0040：网络 Fabric（eBPF-first + WireGuard mesh + Identity），G1/G2/G3

状态：已接受（v3，2026-09-08 三评审修订；§0 gate 经 2026-09-08 管理拍板解除，见 §0.3）
关联：`docs/architecture.md`；ADR-0003、ADR-0004、ADR-0005、ADR-0006、ADR-0008、ADR-0016、ADR-0023、ADR-0027、ADR-0034。
修订：本 ADR 落地时取代 ADR-0004 §1/§2/§5、ADR-0016 禁 mesh 条款、ADR-0034“不引入 overlay/mesh”后果；并同步修订 `docs/architecture.md` §4.3（edge 只见 `node_proxy_endpoint`）、§8（不做 overlay/跨节点私网直连）与 AGENTS.md 架构不变量 2（固定链路 `client → edge → agent proxy → workload endpoint`，edge 不直连 VM）。未点名条款继续有效。
评审吸收：v3 吸收 2026-09-08 三份评审的全部 P0 与主要 P1（ULA 非法字面量、WG 术语、东西向授权缺失、Identity/ULA 稳定性矛盾、下发通道不存在、DSR/TLS 互斥、DNS 落地、capability 格式、ipcache 缺失、`fc00::/7` 冲突、fallback 范围、EgressGateway 破坏性变更、hypeman v6/MTU、WG 规模、节点级 fencing、CNI 越权、0016 推翻条件、契约修订清单）。

## 0. 前置 gate（满足 ADR-0016 推翻条件）

1. ADR-0016 规定的唯一推翻路径是“未来 ADR 明确论证东西向需求无法用阶梯 1/2 满足”。本 ADR 按此执行，进入 G1 的前置 gate 为以下任一：
   - 出现真实东西向消费场景（同 project 内 app 互访私有 db/cache、实例间低延迟 RPC），并附定量证据证明阶梯 1（节点内 slot 互通 + 服务发现）与阶梯 2（跨节点经 proxy 受控放行）不满足（proxy hairpin 延迟/带宽/成本实测，或策略表达力不足）；或
   - 将本 ADR 显式降级为条件触发型提案：在 gate 未满足前只允许 G1 的接口抽取与 eBPF 对等（不碰 mesh/ULA 下发），G1 的 mesh 部分与 G2/G3 不得开工。
2. **管理拍板路径（2026-09-08 新增，本 ADR 据此进入执行状态）**：技术负责人明确拍板“跳过定量证据收集，按依赖序全量推进 G1→G2→G3（身份/IPAM/下发 → WG → eBPF → 真机 spike → G2 → G3）”。该决定显式偏离 ADR-0016 的唯一推翻路径，0016 禁 mesh 条款随本 ADR 落地时以本节记录为准取代；风险接受点：东西向收益主张在无实测证据下成立，G1 验收仍要求真机 VM→VM ULA 直连证据（§25.3 不变）。
3. 背景 §4 的旧表述“绕公网 hostname”不准确（edge 为内部组件，东西向走 edge 并不出公网），v3 改为“绕 edge hostname 中转”。

## 背景

1. 现状拓扑为节点内地址复用 + 节点间 L3 隔离 + 全跨机流量走 `edge → agent proxy → VM`（ADR-0004）。MVP 正确，但形成三个结构性问题：地址与身份耦合（`inst.IP` 是 datapath 地址、转发目标与 egress 连接查找键；proxy 路由键本身是 `machine_id + execution_id` 头，见 `internal/agent/proxy/proxy.go`，v3 修正 v2“proxy 路由键是 IP”的失实表述）、edge 在热路径中心（东西向 hairpin）、无跨机 L3 语义（私网默认 drop，edge/catalog 禁止感知实例地址）。
2. `slot + nftables` 后端紧耦合（`exec ip/nft` + 大锁 + `machine.Adapter` 穿透 `slot.Manager` 具体类型、`egress.Manager` 依赖 `slot.EgressRuleSet`；nft 文本拼装分处 `slot/slot.go` 隔离表与 `slot/egress.go` fp-slot 表），加 datapath 只能再写分支；策略（`netpolicy` + `egress.Policy`）与执行纠缠。
3. Cilium 证明 `endpoint → identity → policy map` 可解“换 IP 不换策略”；Fly.io 6PN 证明 WG mesh + ULA + `.internal` DNS 是东西向正确形态。两者各解决一半，FirePaaS 需要交集。
4. 当前瓶颈是镜像冷启动而非少一跳 proxy；本 ADR 的收益主张仅在 §0 gate 满足的东西向下成立，不宣称普遍性能收益。

## 目标 / 非目标

目标：

1. 双层身份（稳定 service 身份作策略键 + execution 绑定作活性/fence 水位），一套策略快照，同时管南北与东西。
2. eBPF 为默认 datapath；WireGuard 为跨机 underlay（以 WG 自身 UDP 封装为唯一隧道层，不叠加 VXLAN/Geneve）；ULA 为 execution 生命周期内稳定的 workload 地址；edge 退为南北入口 + 全局 LB。
3. 插件缝留在 `Datapath / PolicyEngine / Underlay` 三个小接口（LB 归属 edge，见 §11）。

非目标：

1. 不做 K8s 全兼容（不引 kubelet/apiserver/etcd）；CNI 只做 agent 内部 fenced 调用语义 + 可选 shim 二进制，无外部 runtime 调用方。
2. G1/G2 不做 TLS MITM、不做 L7 DLP/body 检查（延续 ADR-0027）、不做 UDP 域名控制；DNS snooping（如做）只用于观测/关联，绝不参与授权（ADR-0027 §5/§6 立场不变）。
3. G1/G2 不做跨 cell running VM 迁移、全局 session affinity（延续 ADR-0034）；EgressGateway 网关化出口移出本 ADR，另立独立 ADR（G2 不改变 ADR-0027 默认 `unrestricted` 语义）。

## 决策

### G0. 继承的不变量（本 ADR 不触碰，另加节点级 fencing 扩展）

1. PG 唯一权威（desired/business），agent 为 observed，Redis 只做可重建投影；不得从投影反推业务结论（ADR-0003）。
2. 运行态变更 fenced 幂等：machine 级沿用 `machine_id + execution_id + generation + operation_id`，同 operation 不同 request hash 拒绝，ledger 原子持久化；**节点级变更（WG peer/密钥、ULA 分配、策略快照 epoch）是新增的运行态变更类型**，使用 `node_id + fabric_generation + operation_id` fencing 并纳入 agent ledger 持久化与重启重放（G0-2 的扩展，而非例外）。
3. secret 值只存 PG 密文、`CreateMachine.secret_env` 一次下发；credential 单向下发只存验证材料；不进响应/List/Redis/日志/result（ADR-0010/0024）。`workload_identities` 表只存身份文档与数值 ID，不存任何私钥/证书私钥材料。
4. `:5108` 只接受 control-plane，`:5107` 只接受 edge；fail closed。**新增的 edge 经 mesh 直达节点路径的鉴权见 §14，不绕过该边界而是显式扩展它**（高风险改动，实施前独立 `code-reviewer`）。
5. 审计不记录 path/query/header/body/credential；域名/IP 不进 Prometheus 高基数 label（ADR-0027 §8）。

### G1. Fabric 地基（身份 + 地址 + 连通 + 最小 eBPF）

6. **身份双层（v3 重写，删除 SVID 数据面表述）**。控制面签发两层：
   - 稳定层 `ServiceIdentity{trust_domain, project_id, app_id, service, identity_id: uint32}`：跨 execution 稳定，是 eBPF 策略 map 的唯一 key（含 `PolicySnapshot` 索引与审计关联键）；`identity_id` 由控制面分配（PG 唯一），解决 BPF map 不能用 UUID 作 key 的问题；
   - 绑定层 `ExecutionBinding{machine_id, execution_id, generation}`：表活性和 fence 水位。换 execution 即换绑定、撤销旧绑定的新建连资格；**既有 established 连接按有界 drain 窗口结束（Southwestern：新连拒绝、旧连排空），不在包级“立即失效”与“不断”之间二选一**。
   agent 只验证不签发。所谓“轮换”轮换的是 `ULA ↔ identity` 映射与绑定水位，不是 x509；报文（WG+ULA、无额外封包）不携带证书，datapath 信任来自“源 ULA 由平台映射表背书 + 节点 eBPF 源绑定防伪 + WG 对端可信”。
7. **地址双栈、职责切开（v3 修正字面量与稳定性语义）**。IPv4 `10.100/10.12` 冻结为 legacy 节点内 + NAT 出口（注：ADR-0004 原文写 `10.11`，实现为 `10.100/10.12`，以实现为准冻结）；新增 ULA 示例 `fd7a:9a55::/32`（合法十六进制占位，正式值按 RFC 4193 随机 global ID 生成）。分层 `cell /40 → project /48 → node /64 → instance /128`（联邦见 §23）。**稳定性域明确为 execution 生命周期内、单节点内稳定；跨节点迁移（evacuate/重建）= 新 execution + 重编号新 ULA + DNS/catalog 收敛，不承诺“IP 跟 workload 走”。** 成立的收益是“策略以 identity 为键，换 IP 不换策略”（Cilium 只承诺这个）。ULA 只做东西向 + mesh，南北出口仍节点 SNAT（G2 默认语义不变，见 §15）。
8. **IPAM 分治**。ULA `/128` 由 PG 事务分配（`owner_project/owner_node/owner_machine/generation`）；Redis 只做 projection + peer 发现。节点本地 `veth /30` 仍由 slot index 确定性推导、agent 离线可重建，**不走 PG 事务**（“收敛为 `IPAM.Allocate/Release`”仅指接口统一，本地实现保留，避免 slot 创建绑定控制面可用性倒退）。
9. **Underlay 用内核 WireGuard（v3 术语修正）**。以 WG 自身 UDP 封装为唯一隧道层。`VM ULA 即 AllowedIPs` 明确为**按 node `/64` 聚合的 peer 路由**，实例 `/128` 为节点内主机路由，不逐实例建 peer（规模模型：peer 数 = 节点数 + 网关/edge hub 数，与 VM 数解耦；AllowedIPs 更新频率 = 节点上下线/轮换级，非 execution 级）。控制面经**新增**常驻下发通道（见 §18）下发 `peer{node_id, pubkey, endpoint, node_prefix/64, fabric_generation}`；密钥轮换为新 generation 全量替换 + 旧 generation 重放拒。WG 无失败检测：直连失败探测（handshake-age）+ 路由/DNS 翻转 + 回切防抖 + relay 容量预算见 §14/§22，G1 只定阈值与探测机制，不宣称“自动换路”。
10. **eBPF 最小闭包 + ipcache（v3 补映射表与 attach 拓扑）**：`tc-ingress(isolation)`（挂 host 侧 veth）+ `tc-egress(CIDR + proxy redirect + conn-limit)`（挂 slot 内 veth）。maps 必含 `ipcache(ULA → identity_id)`（Cilium 式源/目的解析表，无它则 per-identity 策略不可执行）、`policy(identity_id → gen+规则)`、`cidr_allow/deny(LPM-trie)`、`conn_count(execution+proto → count)`、`audit_events(ringbuf → egress sink)`。Host/SNI 判定仍在 userspace 透明代理，eBPF 只 redirect（等价现有 DNAT）。跨 netns 路径明确：slot netns 内 tc-egress redirect → root ns 代理端口（现有 DNAT 拓扑的 eBPF 版，实施前必须给出挂载点/port 对照图）。同一 `PolicySnapshot` 输入，走 nft 或 eBPF 行为一致。`fc00::/7` carve-out：`netpolicy` canonical 保留集包含 `fc00::/7`，平台 ULA 落在其中，策略执行必须显式放行“平台 ULA、且仅东西向方向”，不得改动保留集本身语义。
11. **接口收敛**（`internal/agent/network/api` 唯一接口包，具体包不得被 `machine/proxy/egress/server` 直接 import）：`Datapath{AttachNetns, DetachNetns, Check}`、`PolicyEngine{ApplySnapshot}`、`Underlay{UpdatePeers, DialULA}`。`Check` 即 CNI CHECK 语义（幂等补齐）；`Reconcile` 三方对齐错误降级不崩溃（沿 M3 纪律）。**`LB{SelectBackend}` 不放在 agent 网络包**（least-inflight 是 edge 职责，ADR-0020，`internal/edge/handler.go`），edge 全局 LB 的插件点如需抽象，放在 edge 侧包。
12. **基线与降级（v3 修正 ID/范围/seccomp）**：x86_64 6.8+（最低 5.15，`conn_count` 的 conntrack 对称计数在 5.15 基线需单独验证）+ BTF；库用 `cilium/ebpf` CO-RE，`bpfel.o` 提交仓库、CI 校验 `git diff` 为空。探测不满足自动回落 nft 并上报稳定能力 ID（二选一上报 `network.ebpf.v1` / `network.nftfallback.v1`，符合 ADR-0023 `name.vN` 小写形态；`mesh.eastwest.v1` 在 `RequiredFeatures` 推导中显式依赖 `network.ebpf.v1`，mesh 服务永不调度到回退节点）。**fallback 范围明确为 legacy 南北 egress emergency 模式**，不是全功能等价；G1.13 的“逐项对等断言”仅覆盖南北隔离/CIDR/redirect/限额四项。**后端切换语义**：开关 mesh 只摘路由、不断 execution；在 nft↔eBPF datapath 之间切换必须重建 execution（在途连接按 drain 语义结束，见 §6）。仍需 root + `CAP_BPF/CAP_NET_ADMIN`；当前无 seccomp profile，若引入 confinement 再放行 `bpf()`（v3 修正“补”字样）。
13. **G1 验收**：新增 hypeman guest v6 注入 spike 通过（见 §25）为前置；VM→VM ULA 直连 ping + 跨 project 拒 + peer/fabric 旧 generation 重放拒 + 同一快照 nft/eBPF 四项对等断言（established、私网集、`netpolicy` 同源、v6 源绑定、代理回流不 masquerade、限额口径对齐）。

### G2. 策略 / 出口 / 入口（东西 bypass，edge 经 mesh 直达节点）

14. **流量路径（v3 删除 DSR 术语，重写鉴权）**：东西向 `VM → 本机 eBPF(identity 鉴权) → WG直达 → 对端 eBPF → VM`。南北保留 edge TLS 终结 + 限流；edge 经 mesh **直达目标节点、绕过 agent proxy `:5107`，回程仍走 edge**（L7 终结场景无 DSR）。为此：edge 节点成为具名单独身份的 WG peer（与 workload 身份域隔离）；北向直达的授权点从 `:5107`（edge-only mTLS + execution-bound traffic token）迁移为“目标节点 fabric ingress（userspace 小终结器）校验同一 execution-bound token（HMAC 口径不变）+ eBPF 仅做 peer 身份准入”——eBPF 不查 HMAC。东西回落 edge-relay 时，对端看到的源是 edge 中转身份而非原始 workload 身份，回落路径策略不等价，必须在容量与审计中单独声明（relay 容量预算 + 回落打点）；`agent proxy :5107` 保留为 legacy 兼容入口；`X-Firepaas-*` 头在新路径不再是路由依据。
15. **策略三层与东西向授权对象（v3 新增缺失模型）**：L3/L4（eBPF：default deny + project 隔离 + **EastWestPolicy 声明的跨 app 放行** + service 端口白名单 + per-execution conn-limit）/ L7（userspace Host/SNI，ADR-0027 语义不变）/ Egress（可信 resolver + 透明代理，ADR-0027 三模式与默认语义不变）。新增策略对象 `EastWestPolicy{src_project, src_app, dst_project, dst_app/service, ports, generation}`：只表达“谁能连谁的哪个服务端口”，`ServiceSpec.mesh_direct` 只表达“本服务可被直连”，两者缺一不可；全量替换 + generation fencing。UTM 澄清：**默认公网出口语义不变**，EgressGateway 网关化另立 ADR。
16. **服务发现（v3 补落地）**：`.internal` DNS（`app.project.internal → ULA set AAAA`），控制面权威；guest `resolv.conf` 由 hypeman guest init 注入 split-horizon（nameserver 指向节点本地 DNS，见 hypeman 依赖 §25）；节点本地 DNS 跑在 agent 侧（agent 挂则 `.internal` 降级，公网 DNS 不受影响）；AAAA 集合与 route backend set 同源（仅 READY 且非 draining 的 execution 可发布）；IPv4-only 应用明确不支持 `.internal`（只回 AAAA）；serve-stale 预算与 edge 同源（默认 120s，`FIREPAAS_EDGE_STALE_WINDOW` 同值）。
17. **catalog 演进**：`backends[]` 加可选 `ula + identity_id + generation`（hints 转正）；只有 `service.mesh_direct` 声明的服务 edge 才读 ULA；revision 高水位守卫（`routerev` Lua CAS + edge `RevisionRejects`）原样延续到 ULA 字段。`Machine.slot_ip` 仍禁入 Redis/edge；入 catalog 的是 execution 域 ULA workload 地址。同步修订 `docs/architecture.md` §4.3 与 §8（见文件头修订清单）。
18. **契约增量（v3 补全漏掉的下发面）**：`ServiceSpec.mesh_direct bool` + 新增 `EastWestPolicySpec` + capability `mesh.eastwest.v1`（依赖 `network.ebpf.v1`，ADR-0023）；**新增 fabric 下发 RPC**（**立项决定（2026-09-08）：采用 unary `ApplyFabric` 全量快照 + 控制面主动推送**，不采用 server-streaming Watch；收敛延迟由控制面推送周期约束，计入东向建连 SLO；`peer/blob` 纳入 §G0-2 节点级 fencing 与 ledger 持久化，fencing 键 = `node_id + fabric_generation + operation_id`）；PG 加 `workload_identities / wg_peers / ipam_allocations / eastwest_policies` 表（全 desired；`services` 复用既有 app/service 模型，不另建表）；Redis 加 `mesh:peer:{node}`、`mesh:endpoint:{machine}:{execution}`、`dns:internal:{app}.{project}`（全可重建 + generation fencing + TTL + epoch 指针，沿 `resv:{epoch}` 模式）。删除 v2“其余 proto 字段语义不动”表述。
19. **G2 验收**：Host/SNI 策略与 ADR-0027 对等 + 出口审计 + edge 经 mesh 直达与旧 proxy 路径对比压测 + DNS serve-stale 断流预算声明 + token 经新路径校验 e2e + relay 回落策略不等价声明。

### G3. 生产化（CNI + 可观测 + chaos + 联邦）

20. **CNI shim（v3 加调用约束）**：`cmd/cni-firepaas` 实现 `ADD/DEL/CHECK/VERSION` + `prevResult` + `Result`，调同一 `Datapath`；**仅由 agent 以 fenced 方式调用**（CNI 请求映射到内部 operation_id + ledger），不接受外部 runtime 直接调用（否则绕过 operation ledger 与 observed 权威，违反 AGENTS.md §4）。不实现 portmap/bandwidth 链。
21. **Hubble 式 flow**：eBPF ringbuf → 统一 flow sink（`project/app/machine/execution` 关联），`EgressAuditStats` 聚合口径不变；新增 `firepaas_agent_ebpf_attach_errors_total / firepaas_agent_ebpf_fallback_total / firepaas_agent_ebpf_policy_gen`（低基数，域名/IP 不进 label）。
22. **chaos 必项**：peer 丢失/分区/密钥轮换/旧 generation 重放（含 WG handshake-age 探测翻转/回切防抖/relay 容量）/split-brain 无双活；72h soak（含 1000 次分配/释放无泄漏，M3 同口径）；冷启动 p95 不退化声明。
23. **multi-cell 联邦**：ULA 按 cell 切 `/40`（如 `fd7a:9a55:01xx::/40` cell-A），各 cell 独立 PG 在自有前缀内分配，跨 cell 只交换聚合路由；global route 仍 `hostname → cluster endpoint generation`（ADR-0034 §3 不动），进入 cell 后才用 ULA set；先 active/passive，自动 gate 只接受 immutable 产物（延续 ADR-0034 §10）。

### 配置

24. `FIREPAAS_NETWORK_BACKEND=ebpf`（默认；`nft-fallback` 只做 §12 定义的 emergency；**删除 `bridge` 为兼容性行为变更**：移除 `slotManager == nil` 的 M1 遗留路径，实施时点名）；`FIREPAAS_EBPF_FALLBACK=true`（false 则 fail-closed 退出）；`FIREPAAS_MESH=disabled|eastwest|full`（G1 只接受 `eastwest` 以下，默认 `disabled` 起步灰度）。

### 25. hypeman 与 guest 前置（v3 新增，G1 开工门禁）

1. guest 网络注入（`hypeman/lib/system/init/network.go` 现仅 `ip addr add <v4>` + 单 nameserver）需新增 ULA 地址/网关/DNS 注入（`vmconfig` 加 `GuestIP6/GuestGW6/GuestDNS6` 或等价 onlink 路由）与 TAP MTU 下调/MSS clamp（`lib/vmm` 已有 `Mtu` 字段可用）；`.internal` 的 `resolv.conf` 注入同属此项（ADR-0008 guest 契约面变更）。
2. 上游策略：AGENTS.md pin 已更新为 `v0.4.1-firepaas`（2026-09-09 发布，含 v6 中间层 API，W0-1 闭环）；上游 [kernel/hypeman] PR 仍为 ADR-0016 记的 H1/H2 风险点，本 ADR 不回避。
3. 无此 spike 证据，G1 验收“VM→VM 直连 ping”不可实现，不得开工。

## 理由

1. 先定身份地址再定 datapath：否则 eBPF/mesh 各自为政，换 IP 换策略，未来每加一个网络特性改三处。
2. eBPF 管策略、WG 管连通、Identity 管身份、ULA 管地址、edge 只管南北入口：合在一起则南北/东西/可观测一次收敛。**收益表述修正为“换 IP 不换策略”，不承诺“IP 跟 workload 走”。**
3. 内核 WG（唯一隧道层）+ TC 无第二封包层；ULA 在 execution 域稳定使迁移只更新映射与 DNS，不改策略（ipcache 同理）。
4. 代价（内核基线 + WG 运维 + IPAM/peer 分发 + hypeman 上游）在保守演进里也要付一半；是否一次付清取决于 §0 gate 是否满足——v3 不再无条件主张。

## 后果

1. ADR-0004 §1/§2/§5（无 overlay、地址复用、`bridge|slot` flag）被本 ADR G1/G2 取代；**删除 `bridge` 后端点名**。
2. ADR-0016 禁 mesh 条款被本 ADR 取代（以 §0 gate 为生效条件）；其阶梯评估思想保留为本 ADR gate。
3. ADR-0034“不引入 overlay/mesh”后果被 G3 取代；其 cell 所有权/failover/active_epoch/CP 优先条款继续有效。
4. `docs/architecture.md` §4.3/§8 与 AGENTS.md 不变量 2 按文件头清单同步修订后才可实施。
5. IPv4 段冻结：新容量只规划 ULA；`10.12/10.100` 不再扩展，不主动迁移存量。
6. 调试面变化：nft 文本排障让位 `bpftool prog/map` + flow sink；runbook 重写排障章节。
7. e2e 需补：身份轮换/绑定 drain、peer 重放、重编号后策略不动、网关出口（独立 ADR）、DNS stale、edge 经 mesh 直达回退、relay 回落。
8. Egress 默认语义不变；网关化出口如需推进另立 ADR，不在本 ADR 验收内。

## 回滚

1. G1：mesh `disabled` 只摘路由、**不断 execution**；WG peer 保留但 datapath 不用；在途会话按 drain 语义结束，新流量走旧 edge→proxy 链。**nft↔eBPF 后端切换才需重建 execution**（v3 澄清 v2 矛盾）。
2. G2：关 `mesh_direct`/EastWestPolicy（服务逐个回退），edge 恢复 `node_proxy_endpoint` 全量路径；`dns:internal` 停读，公网 hostname 路径不受影响。
3. PG 中 identity/ipam/peer/policy 行保留；旧二进制忽略新增字段与未知 `feature_ids`（ADR-0023 未知 ID 可忽略）。

## 实施偏离记录（2026-09-09，三份评审改进落地）

1. **MTU-only、无显式 MSS clamp（§25/§37 行 83）**：TAP MTU 1280（slot
   `ensureSlotV6Locked`，attach/reconcile 双路径）+ guest eth0 MTU 1280
  （hypeman `planNetwork`，仅 GuestIP6 非空时，纯 v4 零回归）。不做
   TCPMSS clamp：guest 是端点而非中转，MTU-correct 端点 MSS 自动派生
   （1220/1200），clamp 仅对穿越型 middlebox 有意义；WG 封装开销下
   1280 无分片黑洞。验证表 "TAP MTU/MSS" 行按此口径执行。
2. **`route_backends` 持久化 mesh 直连提示（§16/§19 补）**：`mesh_ula /
   mesh_identity_id / mesh_generation` 随 backend 世代同事务写入 PG
   （migration 0036，零回填默认值）。PG 仍是已发布 backend set 的权威
   （AGENTS.md 不变量 3），edge/终结器消费的 ULA 提示与路由同源同世代；
   Redis `mesh:endpoint` 投影保持可重建、不做业务结论。
3. **reconciler 读失败 serve-stale（§16 机制落地）**：PG `dns:internal`
   整轮一次读失败时沿用本节点上轮 `dnsCache` 继续推送（哈希不变不推），
   不推送空表——避免单轮读失败造成全网 `.internal` 闪断；预算仍按 §16
   120s serve-stale 窗口执行。
4. **mesh endpoint 投影按 machine 粒度发布（§14 分层点名）**：
   `publishMeshProjection` 只查 `round.direct.Machines[machine]`（任一
   service 声明即整机发布 endpoint 记录），READY/逐 service 门控显式留给
   route 层（edge 只对带 ULA 的 route backend 查 endpoint，今日无害）。
   后续消费者不得把 endpoint 投影误当“已门控集合”用；如需收紧，在记录
   加 `ready` 字段并由 edge 双检（另立改动，不在本轮）。

## 验证

| 改动 | 最低验证 |
|---|---|
| hypeman guest v6/MTU spike | 真机 ULA 注入 + TAP MTU/MSS + 跨节点 ping（G1 门禁） |
| 身份/IPAM/peer 分发 | 单元 + PG 事务/幂等 + 重放旧 generation 拒 + ledger 重放 |
| eBPF datapath | `network/...` + `egress/...` 单测 + `bpf_prog_test_run` + 真机隔离断言 + soak |
| catalog/edge 经 mesh/DNS | `contracts/...` + 两端测试 + revision 乱序/重放 + token 新路径 + DNS 同源门控 |
| CNI shim | CNI conformance（ADD/DEL/CHECK）+ 仅 agent fenced 调用断言 + Reconcile 对齐 |
| 发布前 | `make check` + lab e2e/chaos/soak（高风险：独立 `code-reviewer` 一次） |
