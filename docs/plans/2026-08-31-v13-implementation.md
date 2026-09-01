# v1.3 实现计划（执行记录）

> 按 docs/v1.3-plan.md 发布切片顺序实现。本文件记录本会话的关键实现决策，
> 不与已接受的 ADR 冲突；计划文档只描述范围，不覆盖冻结行为。

## 切片与状态

- [x] v1.3.0 Egress policy + 审计（ADR-0027）：控制面（proto/能力/契约/migration/store/controller/API）+ agent（透明代理/SNI/可信 DNS/限额/审计/slot nft）+ 单测 + e2e-v13-egress.sh
- [x] v1.3.1 Snapshot 资源 + checkpoint + retention（ADR-0028）：SnapShotService proto、snapshots/snapshot_schedules 表、checkpoint（memory/filesystem）、schedule + max_count/max_age retention、UNAVAILABLE↔READY
- [x] v1.3.2 受限 fork + filesystem rescue（ADR-0028）：ForkSnapshot/RestoreSnapshot RPC、fork debug machine（TTL 必填/新 execution/无 route）、restore_mode
- [x] v1.3.3 LOCAL_RW volume（ADR-0029）：VolumeService proto、volumes/volume_attachments 表、单写 attach、locality 硬钉、detach/delete fencing；DATASET_RO import/seal（ADR-0030）框架已入 schema，导入流未落地

## 验证状态

- `make check`（build+vet+test+tidy）通过。
- e2e-v13-egress.sh / e2e-v13-snapshot.sh / e2e-v13-volume.sh 已写入 scripts/lab/，
  后台运行 + 日志轮询（避免长 timeout 卡死）。

## v1.3.0 实现决策

- egress policy 随 deployment 固化（`deployments.egress_policy jsonb`），修改策略
  走现有 rollout（新 generation 新机器），满足"policy 属于不可变 deployment"。
- proto：`NetworkSpec.egress`（field 5）新增 `EgressPolicy`；`Machine.egress_audit`
  （field 15）携带 per-execution 拒绝/放行计数，控制面聚合到 PG 拒绝摘要表。
- 能力门控：deployment 携带 allowed_domains 时要求节点上报
  `egress.domain.v1`（fail closed）；CIDR-only 策略不要求新能力。
- agent 执行层：
  - `internal/agent/egress`：纯 Go 策略判定（exact/single-label wildcard、
    case-insensitive）、HTTP Host 与 TLS ClientHello SNI 嗅探（可回放）、
    miekg/dns 可信 resolver（完整 CNAME 链、TTL 缓存上限、保留段拒绝）、
    per-execution TCP 连接限额、结构化审计 sink（无 URL path/query/header/body）。
  - nftables（slot netns）：allowlist 模式 tcp 80/443 DNAT 到 root ns 透明代理
    （80→18080、443→18443，按本地 veth 地址识别 machine）；其余 TCP/UDP 只按
    CIDR 规则；deny_all 除 allowed_cidrs 外全拒；unrestricted 保持现状（denied
    CIDR 生效）。
  - 代理连接忽略 guest 原始目标 IP，只连接本次可信解析且通过保留段检查的
    A/AAAA 集合；解析失败/集合为空/全部连接失败即拒绝（不回退 guest DNS）。
- e2e：`scripts/lab/e2e-v13-egress.sh` 覆盖 allowlist Host 放行/拒绝、CIDR 矩阵、
  deny_all、连接限额、审计脱敏；SNI/ECH/DNS rebinding 矩阵由单测覆盖。
