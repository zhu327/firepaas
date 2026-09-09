# 2026-09-08 网络 Fabric 实施计划（ADR-0040 G1→G2→G3）

## Goal

按 ADR-0040 依赖序落地网络 Fabric：G1（身份/IPAM/WG/eBPF/真机 spike）→ G2（EastWestPolicy/catalog ULA/.internal DNS/edge 直达）→ G3（CNI/可观测/chaos/联邦）。

## Architecture

- 控制面：PG 新增 desired 表（workload_identities / wg_peers / ipam_allocations / eastwest_policies），Redis 只做可重建投影；fabric 下发走 agent 契约（protos/agent/v1）。
- Agent：node 级 fencing（node_id + fabric_generation + operation_id）纳入 operation ledger；WG underlay 与 eBPF datapath 都挂在 `internal/agent/network/api` 三个插件缝后。
- Guest：hypeman v6 注入（vmconfig/init）在 /home/zty/Learn/hypeman 本地有未提交改动，尚未打 fork tag；firepaas 侧 GuestIP6 下发接线未做。
  （2026-09-09：tag `v0.4.1-firepaas` 已发布，`go.mod` 切回公开 replace；firepaas 侧接线完成，见 09-09 评审修复计划）
- 依赖纪律不变：machine/proxy/egress/server 只 import network/api。

## Gate 决策（已记录于 ADR §0）

2026-09-08 管理拍板：跳过定量证据，按依赖序全量推进；显式偏离 ADR-0016 唯一推翻路径，以 ADR §0 第 3 条记录为准。

## 任务依赖表

| Task | Type | Blocked by | 说明 |
|---|---|---|---|
| T0 环境 + ADR gate 修订 | AFK | - | docker mirror、lab 依赖、clang |
| T1 控制面 PG：fabric 四表 + store + migration | HITL | T0 | identity_id 分配、ULA /128 事务、peer/eastwest 行 |
| T2 proto 契约：mesh_direct + EastWestPolicySpec + mesh.eastwest.v1 | HITL | T1 表结构 | make proto + contracts 两端测试 |
| T3 fabric 下发 RPC + agent 节点级 fencing/ledger | HITL | T2 | 立项定：unary ApplyFabric 全量快照 + 节点 fencing；✅ 2026-09-08 |
| T4 agent WG underlay + ULA 路由 + peer 轮换 | HITL | T3 | 内核 wg（ip 命令），按 node /64 聚合；✅ 2026-09-08（含双 netns 真机内核握手测试） |
| T4b 控制面 fabric reconciler（PG→节点快照组装+推送水位） | HITL | T4 | leader 内两阶段同步；fabric_versions 水位 + 内容哈希去重 + stale 自愈；✅ 2026-09-08 |
| T4c 控制面 ULA 生命周期接线（create 分配 / delete 释放） | HITL | T4b | 缺失：store 有 Allocate/Release 但 controller 未调用，快照 identities 恒空 |
| T5 eBPF datapath（BPF 程序 + loader + fallback 探测 + 能力上报） | HITL | T1-T3 | ✅ 真机 netns 套件 6/6 绿；修 parity 缺口 3 项（deny_all 默认drop/mode_drop、NDP 133-137 放行、conn_count spinlock 原子化）+ 测试 bug 4 项（保留段地址、v6 拓扑 via-local、bind 缺地址、proxy EOF 误杀） |
| T6 guest ULA 注入接线（firepaas→hypeman） | HITL | T1,T4 + hypeman 新 fork tag | hypeman 中间层本地完成；slot v6 三层接线完成（真机netns测试通过）；adapter 注入完成（SetFabricULA+方案A暂态语义+单测4/4）；✅ go.mod 直接 replace 本地 /home/zty/Learn/hypeman（用户指示 2026-09-09），`GOWORK=off make check` 全绿 |
| T7 真机 spike：VM ULA + 跨节点 ping | AFK | T4,T5,T6 | ✅ 2026-09-09 双节点全链路验收：同主机双 Nomad client（agentd-fabric-dual.hcl，每节点独立 WG 端口/设备/slot 前缀/egress 端口/bpffs pin dir，新增 ServiceInfo.fabric_wg_port 上报）；自建 guest 内核 6.12.8（IPv6+EROFS+OVERLAY，hypeman 发布内核均无 IPv6）；stage1（host→guest ULA ping）+ stage2（跨节点 guest→guest 过 WG + EastWestPolicy/eBPF 放行 + 无规则反向 TCP 拒）均 PASS，证据 /var/lib/firepaas-p0/e2e-fabric-spike/ |
| T8 G2：EastWestPolicy 执行 + catalog ULA + .internal DNS + edge 直达 + token | HITL | T5,T7 | G2a ✅（真机 stage2）；G2b ✅；G2c ✅（真机 in-guest wget .internal exit 0）；G2d ✅ 2026-09-09（真机 stage3：edge hub WG peer + mesh:peer/endpoint 投影 + 节点 fabricingress 终结器（凭证唯一路由）+ edge 直达优先/3s 回落 + 三计数指标；真机修复 G2b 转换层丢提示回归）。G2 全部完成 | 高风险，独立 code-reviewer |
| T9 G3：CNI shim + flow sink + 指标 + chaos + soak | HITL | T8 | ✅ 2026-09-09（代码+脚本+文档；chaos/soak 执行待用户授权）：1) `cmd/cni-firepaas`（ADD/DEL/CHECK/VERSION + prevResult + fence 守卫（仅 agent fenced 调用，§20）+ conformance 测试）；2) flow sink（tc.c ringbuf 事件（allow/deny×各拒绝原因）+ 读取器 + identity 关联 + deny 全量/allow 采样日志，EgressAuditStats 口径不变，§21）；3) 低基数指标 ebpf_attach_errors/fallback/policy_gen；4) `chaos-fabric.sh`（peer-loss/partition/key-rotation/stale-replay/split-brain，未执行需授权）+ `soak-fabric.sh`（1000 分配/释放无泄漏 + 72h 窗口 + 冷启动 p95 声明，未执行需授权）；5) 文档修订（architecture.md §4.3/§8 + AGENTS.md 不变量 2，ADR §1 要求）；联邦（§23）为独立 cell 部署形态，ADR 已含规范 | conformance 测试过；chaos/soak 待授权执行 |

## 验证

- 每个 Task：目标 package `go test -count=1`；涉及 proto 时 `make proto` + `git diff` 干净 + contracts 测试。
- 每个波次结束：`GOWORK=off make check`；lab 可用后 `make check-lab`。
- 发布前（G2+）：`make check` + lab e2e/chaos/soak + 一次独立 code-reviewer（高风险 changeset 规则）。

## 假设

- docker.io 直连不通，已配置 daocloud registry mirror（主机级，已授权）。
- hypeman 改动先在本机 checkout 验证，发布新 fork tag 时更新 go.mod replace（CI 消费点）。
- 真机 spike 需要 Firecracker/KVM：本机为 Ubuntu 24.04 + KVM 环境，lab 脚本沿用 scripts/lab/。
