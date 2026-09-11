# 2026-09-10 网络安全加固 Wave 1（实施中）

状态：Wave 1 已实现并通过独立 code-reviewer 评审后的修复（T1–T11 + R1–R8）；Wave 2/3 为已识别但本次不做的缺口，保留在本文末尾。

评审修复记录（code-reviewer REQUEST_CHANGES → 已逐条处理）：
- R1（P1）proto mesh_direct 注释与 W3 分工矛盾：已同步注释并 `make proto`（仅注释 diff），T6 取舍写入本文档。
- R2（P2）ULA 空值语义：引入 `effectiveGuestIP6`（空 = 保持既有），修 Resume/复挂路径“路由已删但 slot_ula 仍认旧 ULA”的不一致；补单测。
- R3（P2）非规范 VethCIDR 导致 root NAT 规则无界追加：`normalizeVethCIDR`（Masked）+ 单测。
- R4（P2）T1 与 `host_bound_established` 重复判定：删除重复分支，统一走 helper（行为逐位等价，gated 套件复验）。
- R5（P2）拨号集判定依赖魔法字符串：`Decision.CIDRAuthorized` 结构化字段替代。
- R6（P3）直达 RTT 口径未定义：注释明确 per-attempt（与 legacy 重试路径一致）。
- R7（P3）直达回落未查 ctx 取消：与 legacy 对齐加 `r.Context().Err() == nil` 守卫。
- R8（跨切）T9 升级陷阱：模式/域名校验从共享 `ValidateEgressPolicy` 移到
  `ValidateEgressPolicySubmission`（仅 API/appcommand 用户提交路径），存量
  unrestricted+domains 保持可调度（placement/controller 仍用宽容的结构校验）。
背景：`docs/plans/` 之外的一次全面网络 review（对照 K8s CNI 实现）确认了若干 P0/P1。
本计划只覆盖可在当前仓库内完整实现、且能用现有带 root 的 gated 测试或纯单测验证的切片。

## Goal

修复 eBPF 数据面与 edge 路径上的租户隔离/授权绕过，补齐 eBPF 模式缺失的 root 出口 SNAT，
让 agent 侧 fabric 策略与契约语义一致；所有改动可回滚、可测试，不改变对外 API/proto 形状。

## Architecture

- 数据面继续遵循 ADR-0040 §10/§11：`tc.c` 是唯一 eBPF 裁决点，Go 侧只维护 map 与生命周期；
  nft 仍是 DNAT/出口 NAT 的执行层（eBPF 后端已依赖 `slot.EnsureNetnsNAT`）。
- Go 侧保持"契约 → 状态 → 后端"分层：`slot.SlotRef`/`api` 接口扩展只加字段，不改语义。
- edge/egress 的修复保持既有错误映射与响应纪律，不新增对外头/状态码。

## Validation

```bash
make build && make vet            # 全量编译与静态检查
make test                         # 单测（无 root 的默认路径）
# eBPF 内核行为（本机已具备 root；使用独立端口避免与 lab 冲突）：
printf '1\n' | sudo -S -p '' --preserve-env=PATH,HOME,GOPATH,GOMODCACHE,GOCACHE \
  env FIREPAAS_TEST_NETNS=1 FIREPAAS_TEST_PROXY_PORT80=38080 FIREPAAS_TEST_PROXY_PORT443=38443 \
  /usr/bin/go test ./internal/agent/network/ebpf/... -count=1 -v
make check                        # 最终集成（build+vet+test+tidy-check）
```

不可用验证：真机双节点 WG/edge-mesh 直达 e2e（需重启 lab 的 agentd/edge，本轮不执行）。
Wave 1 中对 living lab 只做只读核验。

## 依赖与执行顺序

| Task | Type | Blocked by | 写集 |
|---|---|---|---|
| T1 eBPF v4 代理端口 host-bound | AFK | - | tc.c + 生成物 |
| T2 eBPF v6 per-veth 源绑定 | AFK | - | tc.c、slot/、ebpf/、生成物 |
| T3 eBPF root 出口 NAT | AFK | - | slot/nft.go、ebpf/backend.go、cmd/agentd、cmd/cni-firepaas |
| T4 egress 拨号集=授权集 | AFK | - | egress/ |
| T5 edge 直达响应纪律 | AFK | - | edge/ |
| T6 fabric identity service 粒度 + MeshDirect | AFK | - | server/fabric.go |
| T7 HTTP 嗅探边界 | AFK | - | egress/sniff.go |
| T8 保留段单一事实源 | AFK | - | egress/resolver.go |
| T9 allowed_domains 模式校验 | AFK | - | contracts/agentv1 |
| T10 Fabric 快照深拷贝 | AFK | - | state/fabric.go |
| T11 deny flow 日志限速 | AFK | - | cmd/agentd |

T1/T2 共用 tc.c，合并为一次编辑与一次 `go generate`；其余可独立回滚。

---

## Task 1: eBPF v4 代理端口放行限定 host-bound

Goal：`tc_ingress` 对 `dport ∈ proxy_ports` 的放行必须同时满足目的地址是本机地址，
恢复与 nft `INPUT` 链等价的边界，消除跨 slot 在代理端口上的可达性。

Acceptance：
- destination 为同节点其他 guest 的 IP 且 dport=proxy port → `TC_ACT_SHOT`（timeout）。
- destination 为本机地址的代理回流包（DNAT 后）→ `TC_ACT_OK`（既有 `TestEbpfDatapathNetns` 代理路径继续通过）。
- 纯 v4 slot 不受影响。

Files：`internal/agent/network/ebpf/bpf/tc.c`、`internal/agent/network/ebpf/tc_x86_bpfel.{go,o}`（生成）。
Contracts：无 Go 接口变化。
Tests：`ebpf_perslot_test.go` 增加跨 slot 代理端口断言（显式配置 proxy 端口，A→B 该端口必须超时）。
Validation：上节 eBPF gated 命令；`go generate ./internal/agent/network/ebpf/` 后 CI 校验 `git diff` 为空。
Risk controls：只收紧放行条件；若 host_addrs4 未包含 DNAT 目标地址，代理路径回归会在 gated 测试暴露。
Rollback：还原 tc.c + 生成物（单 commit 路径）。

## Task 2: eBPF v6 per-veth 源绑定

Goal：`tc_ingress` 的 v6 源校验从"地址在全局 ipcache"升级为"地址等于本 host veth 的期望 ULA"，
阻断同节点 workload 冒用他 workload identity。

Acceptance：
- 合法源（本 slot 的 ULA）在 policy 允许时放行。
- 伪造源（同节点其他 slot 的 ULA，即使 ipcache/policy 都有条目）→ `FLOW_DENY_SRC`。
- 未配置 ULA 的 slot 不产生 v6 放行面（map 无条目即拒）。
- pure-v4 slot / `GuestIP6 == ""` 零回归。

Files：
- `tc.c`：新增 `slot_ula`（HASH，key=ifindex u32，value=16B ULA），`tc_ingress` 比较 `skb->ifindex` 对应 ULA。
- `internal/agent/network/slot/backend.go`：`SlotRef` 增加 `GuestIP6`。
- `internal/agent/network/slot/slot.go`：`refFor` 填充；`ensureKernel` 用目标态副本构造 ref（execution 更替时新 ULA 必须立即生效）。
- `internal/agent/network/ebpf/backend.go`：AttachSlot 写/清 `slot_ula`（`interfaceIndex` helper），DetachSlot 清理。
Contracts：`SlotRef` 仅加字段；`slot.Backend` 接口不变。
Tests：`ebpf_perslot_test.go` 为 refA/refB 填写 `GuestIP6` 并新增伪造源断言（用 flow verdict 判定）；
`ebpf_netns_test.go` 的 ref 填写 `GuestIP6=ulaSrc`。
Validation：同 Task 1。
Risk controls：所有 slot 均写期望 ULA；detach 按现存 veth 的 ifindex 删除，re-attach 覆盖写防 ifindex 复用陈旧条目。
Rollback：单 commit 还原；map 为 agent 生命周期内派生状态，无持久化影响。

## Task 3: eBPF 模式 root 出口 NAT

Goal：eBPF 后端 `EnsureNode` 幂等确保 root 对 slot veth 源段的 masquerade（与 nft 后端
`fp-isolation post` 同语义），修复全新 eBPF 节点上非代理南北向流量无回程的问题。

现场证据（2026-09-10 只读）：lab 两个 agentd 均为 `FIREPAAS_NETWORK_BACKEND=ebpf`，
root 仅存在遗留 `table ip fp-isolation`（`10.12.0.0/16 masquerade`，旧 nft 运行残留）；
agent B 的 `10.13.0.0/16` 无任何 root NAT。

Acceptance：
- `New(Options{VethCIDR})` + `EnsureNode` 后，root nft 存在 `ip saddr <vethCIDR> masquerade`。
- 重复调用幂等；多 agent（不同 CIDR/table）互不覆盖；表名可配置（默认 `fp-egress`）。
- nft 不可用时返回明确错误（与 EnsureNetnsNAT 同前置）。

Files：`slot/nft.go`（新增 `EnsureRootEgressNAT`）、`ebpf/backend.go`（Options 增 `VethCIDR`、`RootNATTable`）、
`cmd/agentd/main.go`（传 `slotVethCIDR`）、`cmd/cni-firepaas/main.go`（传 bundle VethCIDR）。
Contracts：`ebpf.Options` 仅加字段（零值回落 `slot.VethRange` / `fp-egress`）。
Tests：新增 `TestEbpfRootEgressNAT`（独立 table 名，断言规则存在后清理）。
Validation：eBPF gated 命令 + `make build`。
Risk controls：只 add 规则、不改/删既有表；与 nft 后端规则并存无冲突（同 CIDR 双 masquerade 幂等）。
Rollback：还原代码；已写入的规则由下一次 EnsureNode 或运维删除，无持久化状态。

## Task 4: egress 拨号集合与授权集合一致

Goal：CIDR allowlist 命中后只允许拨号命中 `allowed_cidrs` 的地址，关闭"任一命中即整体放行"的绕过。

Acceptance：`allowed_cidrs=[203.0.113.0/24]`，解析集 `[198.51.100.7, 203.0.113.5]` → 只拨 203.0.113.5；
若仅剩未授权地址 → `dial_failed`/拒绝；domain 命中与 unrestricted 行为不变。

Files：`internal/agent/egress/policy.go`（返回可拨集合或新增 helper）、`internal/agent/egress/proxy.go`。
Tests：`policy_test.go`/`proxy_test.go` 增加混合解析集用例。
Validation：`make test`（egress 包）。
Risk：可能把原本"误放行"的流量改为拒绝，属 fail-closed 方向。
Rollback：单函数还原。

## Task 5: edge mesh 直达响应纪律与 legacy 对齐

Goal：直达路径具备与 legacy 相同的响应清洗与重试语义：内部头不外泄，retryable 502 可回落/重试，403 触发凭证/路由失效。

Acceptance：
- 直达响应中的 `X-Firepaas-Proxy-Retryable`、`X-Firepaas-Credential` 不出现在客户端响应；retryable 502 无 body 请求回落 legacy 并计 fallback。
- 直达 403（无 body）走既有 `retryForbidden` 失效+重试；第二次仍 403 → 403。
- 直达路径计入 upstream RTT 观测。

Files：`internal/edge/handler.go`。
Tests：`handler_test.go` 增加直达 retryable 502 回落、403 失效重试、内部头剥离用例。
Validation：`go test ./internal/edge/...`。
Risk：403 语义与 legacy 完全一致（guest 业务 403 也会触发一次重试，legacy 现状如此）。
Rollback：单文件还原。

## Task 6: fabric 策略按 service 解析 identity（MeshDirect 门控仍归控制面）

Goal：identity 查找不再折叠到 app 粒度：规则按 `dst_service` 精确解析，src 侧按
“该 app 全部 service identity”展开。

设计取舍（实施时定稿，修正初版计划的验收项）：`mesh_direct` 门控**保留在控制面
组装侧**（`buildEastWestSnapshot` 按 `MeshDirectIndex` 剪裁），agent 不二次门控。
理由：identity 是 `(project,app,service)` 级稳定值、跨 execution 共享，而 `mesh_direct`
是各 execution 行上的声明——同一 identity 的多行声明可能不一致，agent 按行门控
只能靠“任一行 true 即放行”近似，与组装侧已有的“任一在役 machine 声明即收录”
完全等价，却多一处可漂移的判定点（W3 测试已固定该分工）。

Acceptance：
- `byService` 以 `(project, app, service)` 为键；规则按 `dst_service` 解析；
- src app 多 service 时对每个 src identity 各生成正向+对称回程条目；
- dst_service 不匹配任何 identity（含未部署）→ 规则跳过；
- `mesh_direct=false` 的 identity 行由控制面剪裁保证不会单独成规则（agent 不重查）；
- 现有单 service 行为不变（既有测试通过）。

Files：`internal/agent/server/fabric.go`。
Tests：`server/fabric_test.go`（多 service 解析、src app 多 service 展开、dst_service 缺席）。
Validation：`go test ./internal/agent/server/...`。
Risk：多 identity app 的策略条目数上升（预期）；无 identity 的规则仍跳过。
Rollback：单函数还原。

遗留（另立改动，不在本 wave）：`protos/agent/v1/agent.proto` 中 `mesh_direct`
字段注释仍写“agent 建 policy 条目要求缺一不可”，与 W3 既定分工相反；正确修法
是同步注释并 `make proto`（涉及契约生成物，单独提交）。

## Task 7: HTTP 首包读取上界

Goal：请求行/头部读取有硬上界，恶意 guest 不能通过无换行长行造成代理内存放大。

Acceptance：请求行超 8KB → `ErrNoHostInfo`/拒绝；头部累计超限 → 报错；正常 HTTP 行为不变。
Files：`internal/agent/egress/sniff.go`（有界 `readCRLFLine`）。
Tests：`sniff_test.go` 长行、恰好边界、CRLF/EOF 路径。
Validation：`go test ./internal/agent/egress/...`。
Risk：正常超长 header 的请求会被拒（本就在 8KB 语义内）。
Rollback：单函数还原。

## Task 8: 出口保留段改用 netpolicy 单一事实源

Goal：`egress.ReservedChecker` 消费 `netpolicy` canonical 集合，补齐 NAT64/6to4/240.0.0.0/4 等缺口。

Acceptance：`64:ff9b::a00:1`、`64:ff9b:1::1`、`240.0.0.1`、`192.88.99.1` 判为 reserved；平台 extra 段仍生效。
Files：`internal/agent/egress/resolver.go`。
Tests：`resolver_test.go` 扩展表驱动。
Validation：`go test ./internal/agent/egress/...`。
Risk：更严格（原本可连的保留地址变为拒绝），符合 ADR-0027 防 SSRF 方向。
Rollback：单函数还原。

## Task 9: allowed_domains 仅在 allowlist 模式合法

Goal：拒绝 `unrestricted/deny_all + allowed_domains` 的静默失效配置（fail closed，部署期报错）。

Acceptance：`ValidateEgressPolicy` 对上述组合返回错误；allowlist 组合不受影响；API/appcommand 部署路径得到明确 400。
Files：`internal/contracts/agentv1/validate.go`。
Tests：`contract_test.go` 组合矩阵。
Validation：`go test ./internal/contracts/...`。
Risk：现存使用该组合的部署需要修正（当前语义本就是忽略 domains）。
Rollback：单函数还原。

## Task 10: Fabric 快照深拷贝

Goal：`Current()`/`Apply` 不再与调用方共享 `EastWest/DNS` 切片。
Files：`internal/agent/state/fabric.go`。
Tests：`fabric_test.go` 别名断言。
Validation：`go test ./internal/agent/state/...`。

## Task 11: deny flow 日志限速

Goal：东西向 deny 事件日志有全局速率上界，避免高 pps 拒绝流量造成日志放大。
Files：`cmd/agentd/main.go`（flowLogSink 窗口计数 + 抑制摘要）。
Tests：`cmd/agentd/main_env_test.go` 或新测试文件（纯函数化限速器）。
Validation：`go test ./cmd/agentd/...`。

---

## Wave 2/3（本次不做，已识别）

1. conn_count 计数泄漏（LRU syn_seen 淘汰导致 FIN/RST 不递减）。需带超时的流表或内核 conntrack，
   不能用"无条件递减"修复（guest 可伪造 RST 把自身 cap 降到 0）。
2. 控制面 fabric：agent 水位查询/强制 resync、edge hub pubkey 预校验、`wg_peers` 退役 GC、
   ULA 节点块分配、分面 generation。
3. WG 失败检测→路由/DNS 联动、relay、可操作密钥轮换。
4. eBPF 策略双 map 原子切换；egress 内核/代理提交顺序重审。
5. 反向端口语义（UDP 无端口对称回程）、node_ula 端口白名单、NDP 类型收窄、目的端策略。
6. 东西向 L4 VIP/健康感知 DNS；FQDN 全端口出口；egress gateway。
7. stale `ip fp-isolation` 表在 eBPF 节点的清理（需 root NAT 落地后并考虑 nft-fallback 共存）。
