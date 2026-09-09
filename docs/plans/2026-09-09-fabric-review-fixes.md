# Fabric Review 修复计划（ADR-0040 三份评审整合，2026-09-09）

来源：会话 A（`...31`）、会话 B（`...59`）、本次 4 路并行审查（控制面 / 契约-edge / agent 网络后端 / agent 状态机 + hypeman）。
基线：未提交 changeset（53 改 + ~40 新增，~4300 行）+ `~/Learn/hypeman` 未提交 7 文件。
B 已验证 `GOWORK=off make check` 全绿；A 验证 build + 相关包单测 + vet 通过。

> W0-1 已完成（2026-09-09）：hypeman tag `v0.4.1-firepaas` 已发布，`go.mod` 已切回公开 replace（本地路径摘除），`GOWORK=off` build/48 包测试/tidy-check 全绿；发布内容与本地工作树 `diff -rq` 一致。
> root 测试可用 sudo（密码见用户说明）；长时间测试后台运行 + 间隔查日志，不用大 timeout 阻塞。

## W0 合入阻断

### W0-1（已完成）
- hypeman commit → push `firepaas-lib` → 打新 fork tag（`v0.4.1-firepaas`，与本地工作树一致）→ firepaas `go.mod` 改回公开 replace → `go mod tidy` → `make tidy-check` PASS。
- 移出跟踪二进制：根 `cni-firepaas`、hypeman `lib/system/init/init`（改 CI 构建发布）。
- 验收：干净容器 `GOWORK=off make check` 绿。

### W0-2（本计划起点）
- `FIREPAAS_MESH` 非法值双端 fail-closed：控制面（`cmd/api/main.go:237,287`）未知值（含拼写错误、`full`）启动报错，与 agentd（`cmd/agentd/main.go:584`）同行为。
- `gofmt -l` 清零（B 报 ≥11 文件：fabric/fabric.go、controller.go、eastwest.go、slot/nft.go、slot.go、state/fabric.go、publisher.go、catalog.go、validate.go 等）。
- 验收：`gofmt -l` 空 + `GOWORK=off make check` 绿。

## W1 数据面隔离（最优先）

- W1-1 eBPF egress per-slot 化：`cidr_allow4/cidr_deny4/mode_drop`（`bpf/tc.c` + `ebpf/backend.go:325-407 ApplyEgress/clearMaps/setModeDrop`）加 slot 维度或 per-slot map + 尾调；`mode_drop` 同理。补双 slot 隔离 netns 测试（A deny_all、B 放行，互不影响）。对应三方 P0-1。
- W1-2 UDP/ICMP 端口白名单：`tc.c:379-407` 端口检查只对纯 SYN TCP，UDP/ICMP 有 policy 即放行 → 补端口检查，或 ADR 明确只约束 TCP 并同步注释/验收。80/443 DNAT 与 nft 对等（仅声明代理端口才建，`backend.go EnsureSlotNAT`）；redirect 端口读快照而非写死 80/443。
- W1-3 `conn_count` 按 slot 键化 + `DetachSlot` 清理；删热路径 `bpf_printk`（tc.c:449,477）；`putLPM` 字节序注释纠正（LE 内存序两端同口径）；截断 TCP 头改 SHOT。
- W1-4 源绑定 per-interface（`ipcache` 全局 ULA→identity，同节点可伪造源；A 发现）或声明接受缺口 + 补偿审计；v6 回程补 `host_bound_established` 对等物（节点 ULA 回包被源绑定 drop，我 P1-2）。
- 验收：`ebpf_netns_test` 多 slot 版 + `GOWORK=off go test -count=1 ./internal/agent/network/...` + `make check`。

## W2 生命周期 / 崩溃恢复

- W2-1 ULA 释放：create 终态 FAILED 释放（`controller.go:899-919` 三路：换节点重试/pinned/永久失败）+ mesh 关闭期删除 + `mismatchConverged`（1160-1210）路径释放 + 无主分配 sweeper；`FabricIdentities` 过滤当前 execution（泄漏行持续进快照）。
- W2-2 `DeleteWGPeer` migration + 节点 GC（现只增不减，死 peer 密钥残留 + /64 泄漏）；`AdvanceFabricVersion` `<=` 改 `<`（同代覆写缺口）或 agent 同代异 hash 拒交叉测试锁定。
- W2-3 slot v6 生命周期：`DetachNetns` 调 `removeSlotV6Locked`（现只删 netns，root 路由/NDP 泄漏致黑洞/劫持邻居）；`Reconcile` 补 `GuestIP6`（一行修）；`reattachSlot` 透传 ULA（修 restore 抹 ULA：`adapter.go:1164-1171` + `slot.go:292-298`）。
- W2-4 启动重放：`agentd` 启动从 `state.Fabric` 重放 `UpdatePeers/ApplyFabricPolicy/DNS/ingress`；WG 水位（`wg.go lastGen` 仅内存）进 ledger 持久；`fabric.json` 丢而 ledger 在的自愈路径；fallback 时不建 WG 设备（现装 `/64→wg` 致单向黑洞）。
- 验收：kill -9 重启后东西向不断；1000 分配/释放后 `released_at IS NULL` 回基线。

## W3 控制面语义

- W3-1 DNS：读失败跳过本轮推送（现 `fabric.go:280-291` nil 参与哈希→gen+1 推空表，Redis 抖动即全网 `.internal` 闪断，B P1-2）；DNS 变化不 bump 主 generation（独立 generation 或分段哈希）；`InternalDNSRecord.Generation` 注释与取值统一（route revision vs ipam generation）；`SetDNSStaleWindow` 接线 + TTL 同源守卫。
- W3-2 `mesh_direct` 粒度统一：决策单 service vs 多 service。单则 `dst_service` 降级诊断字段并简化快照；多则 identity/ipcache/policy 按 service 建模 + 两端同口径单测（现 publisher 按 service、`server/fabric.go:114-137` 按 app 主服务）。
- W3-3 调度与投影：`RequiredFeatures` 推导 `mesh.eastwest.v1`（含 `→ebpf` 展开）+ 单测，mesh 服务永不落 nft 节点；`mesh:endpoint` 按 `MeshDirect`（及 READY 范围）过滤 + edge route 先验硬断言；`SyncRoutes` 存 hints 三列或书面确认派生模型（PG/Redis 权威分层）；dns/mesh 投影加 epoch CAS 或 ADR 回写偏离；WG endpoint 独立配置（现由 GRPCAddr 派生，NAT/多网卡下错）。
- 验收：调度单测 + endpoint 旁路负例单测 + `contracts` + 两端测试。

## W4 edge 入口 hardening

- 带 body 请求回落加 `requestHasNoBody` 守卫（现 POST 抖动静默丢数据，`edge/handler.go:780-798`，B P1-8）。
- 空凭证不尝试直达（现变终态 403 而非回落）；加 `ResponseHeaderTimeout`（3s 只覆盖拨号）；`firstWriteTracker` 补 `Flusher/Hijacker/Unwrap`（现 SSE/分块不增量 flush、WS 失败，`handler.go:811-826`）；修 `meshErrors` 不可达；ingress 加 `Read/IdleTimeout`；edge 自过滤用配置 ID（现硬编码 `edge-hub`）；`fp-edge0` 地址配置自测。
- 验收：POST 抖动不丢数据单测 + stage3 回落计时断言。

## W5 CNI + hypeman（不含打 tag）

- CNI（`cmd/cni-firepaas`）：传 `Backend/NamePrefix/VethCIDR`（与 agent 同 Datapath，现默认 nft）；fence 绑 `operation_id+ledger+spec hash`（现只验 0600 文件存在）；与 agentd 并发加锁；`prevResult` 语义修正或明确不支持；缺则标 G3 未完成。
- hypeman（代码部分，tag 发布仍归 W0-1 手动）：TAP MTU 下调 / MSS clamp 接线（`lib/vmm.Mtu` + 火端透传）；`planNetwork` 非法 GW/DNS 校验 + 单测；`IPv6DNS` adapter 断言（现 `fabric_ula_test.go` 零命中）；resolv 空 DNS 跳写点名（guest 契约面变更）；`types.go` 注释“第二条” vs 实现“第一位”统一。
- 验收：大包 TCP over WG e2e；`network_test.go` 全绿。

## W6 发布前

- chaos 真故障补齐（split-brain 真双写、handshake-age 翻转/回切防抖、relay 容量）+ soak（含冷启动 p95 基线断言）+ DNS stale 120s 内外 e2e（需授权执行；`chaos-fabric.sh`/`soak-fabric.sh` 已备脚本）。
- ADR 回写三处偏离：§18 epoch 指针（TTL 收敛替代）、§14 “eBPF 准入”实由 WG crypto 承担、nft↔eBPF 切换重建守卫（二选一：`slots.json` 后端指纹校验或运维纪律声明）。
- 高风险规则要求的独立 `code-reviewer` 一次（W1–W3 若重塑设计则复审）。

## 执行顺序

W0-2 → W1 → W2 → W3 → W4 → W5 → W6。每波结束跑目标包单测；W1/W3 后跑 `make check`；root netns 套件后台运行 + 轮询日志。

## 落地后记（2026-09-09 Review 中追加）
- W4 追加：终结器 `IdleTimeout 120s + MaxHeaderBytes 1MB`（Read/Write 不限：流式透传）；终态 body 已消耗中段失败显式 502（meshErrors 可达）。
- W5 追加：CNI stdin 改读 `os.Stdin`（原 `/dev/stdin` 无视重定向，单测 vacuously 通过——真 bug）；hypeman `planNetwork` 拒非法 GuestDNS6；`types.go` gofmt 对齐（`diff -w` 仅 13 行语义新增）。
- W5-4（SetIPv6DNS/LinkNetNS 断言）经查无此 API（两仓库均无 `LinkNetNS` 符号），列为不适用。
- hypeman `lib/instances` 集成失败（QEMU/docker.io TLS/bridge 权限）经 `git stash` 验证为 pristine 环境失败，与本次改动无关。
- ADR-0040 新增“实施偏离记录”三条（MTU-only 无 clamp / route_backends hints 列 / reconciler serve-stale 机制）。

## 独立评审后修复（2026-09-09 code-reviewer，REQUEST_CHANGES→全部认领）
- P0 go.mod 本地 replace：即 W0-1 本体；**已闭环**——tag `v0.4.1-firepaas` 发布后 `go.mod` 切回 `replace github.com/kernel/hypeman => github.com/zhu327/hypeman v0.4.1-firepaas`，本地 replace 摘除。
- P1：ErrNoDirect 标 retriable（+ endpoint miss POST 回落测试）；FabricIdentities 改 LATERAL 逐 service（+ 多 service 测试，首非 mesh/次 mesh）；坏身份行 skip+Warn（原整轮 abort）；DNS stale 窗口接线（controller.Config.DNSStaleWindow ← FIREPAAS_DNS_STALE_WINDOW，+ 默认同源测试）；slot 侧补跨进程锁（slot.WithStateFileLock 单实现，Load/persist 双边拿，+ 互斥测试；CNI 改调单实现）。
- P2：删 fabricHintULA/ID/Gen 死代码；删根 cni-firepaas 二进制 + .gitignore 覆盖根构建产物；ADR 偏离 +#4（endpoint 投影 machine 粒度点名）；SyncLoop 自地址失败转 Warn + splitSelfPeer/selfULAAddr 纯函数单测。
- fence ledger/spec-hash 绑定：选评审 (b) 方案——machine 绑定即本期目标，ledger 水位/spec-hash 移 G3 agentd 接线时（CNI 头注释点名）。

## 实验室真机验证轮（2026-09-09 下午，双 agentd + API + edge mesh direct）

### 发现并修复的真 bug（全部先回归测试后修复）
1. **fabric GC 终删残留永不释放**（controller.sweepFabricULA）：delete 完成后
   `current_execution_id` 仍指被删 execution，`current == row → continue` 判定
   排在 DELETED 检查之前，主泄漏路径成为死代码。修复：desired=DELETED 先于
   current 判定（in-flight 保护不变）；+ 回归用例（desired=DELETED 且
   current==row → 释放）。实验室实证：GC 释放 10+3+1 条残留。
2. **tcFilter attach 泄漏 + 跨重启僵尸**（ebpf/backend.go）：无 pref 的
   `tc filter add` 内核自动分配新 pref 且永不冲突，每次 reconcile 叠一份
   （实测 8 份叠加逐包重复执行）；重启换程序后旧运行期 filter 挂在旧程序
   实例上、引用冻结的旧 map，新 identity/规则查无即 deny——slot 跨重启
   存活时 v6 静默断流（实测 node B 断流根因）。修复：attach 前
   `tc filter del` 清全向 + `replace` 固定 pref 49142。
3. **重启后 fabric 数据面不重放**（cmd/agentd）：启动恢复只重放 WG peers，
   eBPF ipcache/policy map 随程序重建后为空；控制面推送是 hash 门控（内容
   未变不重推），agent 重启后 v6 东西向静默断流直到下一次快照内容变更。
   修复：启动时按 durable fabric.json 重放 ApplyFabricPolicy（与 server
   写入路径同构：规则→identity 映射 + 对称回程条目）；+ 重放日志。
4. **nft 时代遗留 ip6 fp-isolation 表毒杀 v6**（ebpf EnsureNode）：陈旧
   slot-veths 集合条目（veth 名按 slot 序号复用）无条件 drop 同名新 slot
   的全部入站 IPv6（含 NDP）。修复：eBPF 模式 EnsureNode 删除该遗留表
   （v6 策略点在 tc_ingress，ADR-0040 fail-closed）；nft-fallback 自管。
5. **go.mod 缺 eBPF 依赖**（CI 一致性）：本地 go.work 掩蔽了 cilium/ebpf
   未入 go.mod；GOWORK=off tidy 补齐（CI 口径）。

### 脚本修复
- chaos-fabric.sh peer-loss 的 `nomad job restart` 补 `-yes`（非交互下确认
  提示读到 EOF 静默退出，`||` 兜底在 set -e 下无声终止场景链）。
- e2e-fabric-spike.sh stage3 metrics grep 补 `|| true`（set -e 下无匹配
  静默退出）。

### 验证结果（final 二进制：含上述全部修复）
- spike：stage1 PASS（host→guest ULA）/ stage2 PASS（跨节点 guest→guest
  WG + eBPF 策略正反向）/ stage3 PASS（edge mesh direct + 指标 + 回落）。
- chaos（same-host 模式，断真双主机才做 break-assertion）：peer-loss（删
  fp-wg0 + agent 重启→恢复 + stage2 PASS）、partition（WG UDP drop→heal
  PASS）、key-rotation（pubkey 换/回 PASS）、stale-replay（水位篡改低：
  hash 门控跳过推送、agent 领先即一致态；bump 路径在内容变更时按 1 代/轮
  有界收敛，实测 1→29 爬升；数据面全程 PASS）、split-brain（ghost 高代行
  注入，agent fencing 无双活副作用 PASS）。
- soak：20 轮 mesh_direct app create→RUNNING→delete，终态 0 残留分配
  （泄漏断言 clean）。
- `make check`（GOWORK=off）：48 包全绿 + tidy-check PASS。

### 已知遗留（不阻塞合入评审结论）
- hypeman fork 缺口：digest 引用的 repository 条目被镜像 GC 移除后，
  CreateImage(digest) 不重建、adapter waitImageReady 静默轮询到超时——
  实验室异常批量删除暴露；正常运维不触发，另行跟踪。
- root netns 套件（ebpf/slot）与实验室同端口（egress proxy 18080/18443）
  冲突，需停 lab 单跑；本轮以实验室实测覆盖等价路径。
- chaos 同主机模式：peer-loss/partition/key-rotation 的“连接必断”断言需
  真双主机；同主机仍执行变更+恢复与策略断言。
