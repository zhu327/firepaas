# 2026-09-10 网络安全加固 Wave 2（实施中）

状态：W2-1/2/3/4/5/6 已实现并通过独立 code-reviewer 评审后的修复（R1–R11）；W2-5 的“陈旧 nft 表清理”经评估后**不做**（见该节说明）。Wave 3 为产品/架构决策项（不写代码，列出决策点与不做原因）。

评审修复记录（code-reviewer REQUEST_CHANGES → 已逐条处理）：
- R1（P0）双向声明下 TCP 已建立流被误丢：非纯 SYN 改为 dport/sport 并集判定；gated 测试新增双向 TCP 用例，已 red→green 验证。
- R2（P2）限额落表在 egress map 切换之后：`EnsureNetnsTCPLimit` 提到 map 写入之前，失败不产生半应用状态。
- R3（P2）nft→eBPF 切换残留 `egress-fwd`/集合：`cleanupLegacyNftPolicy` 在每次 ApplyEgress 前回收（best-effort）。
- R4（P3）双缓冲只做到逐查询原子：每包只读一次 `fabric_active` 并传给各 lookup。
- R5（P3）原子性“中途写失败”无故障注入：保留为已声明的验证缺口（不引入 test-only 后门）。
- R6（P3）`Backend.snapshots` 死状态：删除。
- R7（P3）`FlowSampleEvery` 注释与实现不符：注释改为“0 = 默认 128，1 = 全量”。
- R8（P3）残留 `conn_cap` 注释：修正。
- R9（P3）singleflight 把 leader 取消失败广播给等待者：leader 改用 `context.WithoutCancel` + 10s 有界超时。
- R10（P3）mesh 缓存淘汰每次全量排序：改单次扫描淘汰最旧（O(n)，无额外大 map）。
- R11（P3）限额规则文本两处手写：抽 `tcpLimitRule` 共用。
- 另修（自查）：BPF 采样后 Go sink 又按 1/128 二次采样（实际 1/16384），改为直接记录收到的事件。

## Goal

关闭 Wave 1 遗留的数据面正确性与控制面可用性缺口：连接计数泄漏、策略快照非原子替换、
回程端口语义过宽、fabric 推送无自愈/无校验、以及 edge/slot 的工程性缺口。

## Validation

```bash
make check && make tidy-check && make vet
# eBPF gated（本机 root；独立端口避免与运行中的 lab 冲突）：
printf '1\n' | sudo -S -p '' --preserve-env=PATH,HOME,GOPATH,GOMODCACHE,GOCACHE \
  env FIREPAAS_TEST_NETNS=1 FIREPAAS_TEST_PROXY_PORT80=38080 FIREPAAS_TEST_PROXY_PORT443=38443 \
  /usr/bin/go test ./internal/agent/network/ebpf/... -count=1 -v
# 注意：slot 包的 root-gated 测试会扫描/删除 fp-slot-*，与运行中的 lab 冲突，本 wave 不跑。
```

## 验证结果（2026-09-10）

| 范围 | 命令 | 结果 |
|---|---|---|
| 我在 Wave 1/2 涉及的全部包 | `go build/vet/test ./internal/agent/... ./internal/edge/... ./internal/contracts/... ./shared/...` | PASS |
| eBPF 数据面（root，6.8） | `sudo FIREPAAS_TEST_NETNS=1 FIREPAAS_TEST_PROXY_PORT80=38080 FIREPAAS_TEST_PROXY_PORT443=38443 go test ./internal/agent/network/ebpf/... -count=1` | PASS（9 个测试：datapath/per-slot/root NAT/原子切换/双向合并/flow 解码） |
| 控制面 fabric（PG） | `FIREPAAS_TEST_POSTGRES=...firepaas_w2test go test ./internal/controlplane/fabric/... -count=1` | PASS（scratch 库；dev 库因 lab 数据导致既有用例 `TestSyncCarriesEastWestAndMeshDirect` 断言污染，与本次改动无关） |
| 全量 `make check` | 依赖并发会话的 `cmd/fpctl`/`store` 完成态 | 一度 PASS（19:05）；当前为并发会话的 `cmd/fpctl` WIP 阻断（未使用 `net/url` + `normalizeAntiAffinity/buildHealthCheck` 未定义），非本 wave 文件 |

R1 的 red→green：临时把非纯 SYN 分支还原为 SPORT-only 后，per-slot 测试确定性失败
（双向 TCP helper 8s 超时）；恢复后全套 PASS。

## 切片

### W2-1 eBPF 连接限额改用内核 conntrack（修计数泄漏与两端语义漂移）
- 问题：`syn_seen` 是 LRU，长连接条目被淘汰后 FIN/RST 不递减，`conn_count` 只增不减，
  最终拒绝合法新连接；nft 后端 `ct count` 随 conntrack 超时自愈，两端行为不一致。
- 方案：eBPF 后端不再用 BPF 计数；`ApplyEgress` 在 slot netns 的 `fp-slot` 表内
  批量维护 `meta l4proto tcp ct state new ct count over N counter drop` 规则
  （与 nft 后端同一实现口径）。删除 `conn_cap/conn_count/syn_seen` 及 Go 侧字段。
- 验收：既有 conn-limit gated 断言继续通过（第 3 条连接超时）；删除泄漏路径。
- Files：`internal/agent/network/ebpf/bpf/tc.c`、`ebpf/backend.go`、`slot/nft.go`（helper）、gated 测试。

### W2-2 Fabric 策略双缓冲 + 原子切换
- 问题：`ApplyFabricPolicy` 先清后写，中途中失败/并发读会看到部分策略（fail-closed 窗口）。
- 方案：`ipcache/policy/policy_ports/node_ula` 各双份（`*_v0/*_v1`）+ `fabric_active` 选择子；
  Go 写非活动集后一次 `Put` 翻转；启动时两集为空、active=0，durable 快照重放后翻转。
- 验收：切换过程中 datapath 只看到旧集或新集；写失败保持旧集（gated 测试断言活动集内容不变）。
- Files：同上。

### W2-3 回程条目端口语义（TCP/UDP 源端口匹配）
- 问题：对称回程条目无端口，A→B 放行后 B→A 的 UDP 任意端口、TCP 任意非 SYN 均放行。
- 方案：`policy_val` 用 flag 位区分 dport/sport 校验；`policy_ports` key 增加 dir 位
  （0=dport、1=sport）；反向条目携带同一端口集置 sport 标志。UDP 双向都声明时按 dir 各查
  一次（任一命中），不把两方向混在一张表里。
- 实现期发现的附带问题：双向 EastWest 声明会在同一 (src,dst) 上产生正向+回程两条目，
  必须按位合并 flags（`mergePolicyEntries`），否则后写覆盖前写（丢约束或放宽）。
- 验收：per-slot gated 的 UDP 回程仅在声明端口放行；双向合并有单测。
- Files：`tc.c`、`ebpf/backend.go`、`server/fabric.go`（含 agentd 重放共用 `BuildFabricPolicy`）、测试。

### W2-4 控制面 fabric 推送自愈与预校验
- 问题：内容哈希门控 + 无 agent 水位查询 → agent 状态丢失后永不重推；edge hub 公钥长度
  错误会让全网推送永久失败；控制面发送前不预校验。
- 方案：(a) 组装后调用 `contracts.ValidateApplyFabricRequest`，失败不发并记录状态；
  (b) `ForceSyncEvery`（默认 10m）到点即使内容哈希未变也强制推一代，限定 agent 状态
  丢失后的陈旧窗口；哈希未变但强制推送时 New generation 单调。
- 验收：bad pubkey 请求不出网且状态可观测；force 窗口到期产生推送；哈希未变时默认不推。
- Files：`internal/controlplane/fabric/fabric.go` + 测试。

### W2-5 数据面卫生：NDP 收窄 / 陈旧 nft 表清理（不做）/ allow flow 采样
- NDP：workload veth 只放行 RS/NS/NA（133/135/136），RA/redirect（134/137）drop。
- 采样：新增 `flow_sample`（数组，模数，默认 128，1 = 全量），仅对 allow 事件按
  `bpf_get_prandom_u32()` 采样；deny 全量（审计）。
- **陈旧 `ip fp-isolation` 表清理：不做**。理由：该表在升级节点上提供 10.12/16 的
  备用 root NAT，且与正在运行的旧 binary/lab 相关；删表收益仅是回收一份过时保留段
  清单（eBPF 路径已自带隔离），风险是混合后端共存/未升级节点断 NAT。保留它只会更严
  （宁多 drop），需要清理时由运维在确认所有节点均升级并已有 `fp-egress` NAT 后手动删。
- Files：`tc.c`、`ebpf/backend.go`、gated 测试。

### W2-6 Edge/Slot 工程性缺口
- `mesh.Endpoints` 缓存无界 → 容量上限 + 过期/最旧淘汰（execution 换代不泄漏）。
- legacy upstream Transport 缺 `ResponseHeaderTimeout`/拨号超时 → 与直达同口径。
- RouteCache 回源无 singleflight → TTL 边界并发回源合并。
- slot Reconcile：live instance 的 ULA 变化要同步持久状态并回收旧 /128；`!exists` 分支
  走 `releaseLocked`（清后端 map），不只删内存条目。
- Files：`internal/edge/{mesh/mesh.go,edge.go,handler.go}`、`internal/agent/network/slot/slot.go` + 测试。

## Wave 3（决策项，不在本 wave 写代码）

1. ULA 节点块 IPAM（Cilium-style）：需 PG migration + 分配算法替换 + 回收对账；
   当前逐 /128 分配（4096 上限、O(n²) 低地址扫描）在规模前可用，先明确目标规模再动。
2. WG 失败检测 → 控制面联动（DNS/route/edge-direct 摘除）与 relay 备路：需要
   Info/proto 上报 peer 健康 + controller 过滤语义 + relay 容量模型，是跨组件特性。
3. 可操作密钥轮换：需要 agent 生成新密钥/上报的受控路径 + 旧代重放窗口语义。
4. 目的端策略（纵深）：需要在 WG/入口路径增加 ingress 裁决程序与威胁模型。
5. 东西向 L4 VIP/健康感知 DNS（A/SRV）：新产品语义（VIP 分配、健康、会话）。
6. FQDN 全端口出口（DNS 观察 + 编程 IP 缓存）：ADR-0027 明确当前只做 80/443 MITM-free。
7. EgressGateway 网关化出口：ADR-0040 明确要求另立 ADR。
8. CNI shim 接线与 status API（NodeFabricStatus 导出）：G3 范围 + 新公开面。
