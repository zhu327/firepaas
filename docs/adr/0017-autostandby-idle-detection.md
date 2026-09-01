# ADR-0017:scale-to-zero 空闲检测——集成 hypeman lib/autostandby(conntrack 视角)

状态:已接受(2026-08-28)
依据:mvp-plan §11 将"自动 idle 检测(per-VM usage 管道:List 暴露 VM metrics →
PG 投影 → app 级开关)"列为 v1.1 项;对照评审(docs/fp.md)指出 hypeman 已内置
生产级 conntrack 驱动 auto-standby(`lib/autostandby`),其语义与 M4.5 已交付的
autoresume 唤醒侧恰好互补。经对两仓实际代码核对(`lib/autostandby`、
`internal/agent/health`、controller reconcile、route 投影),本 ADR 废止自建
usage 管道方案。

## 决策

### 1. 空闲检测的执行者与判定源

- **执行者在 agentd**(节点侧),复用 hypeman `lib/autostandby.Controller`;判定源是
  **host 侧 conntrack**(入站 TCP 流,SYN_SENT 即计为活动),不是 CPU/内存启发式。
- agentd 装配 firepaas 侧 InstanceStore 适配器(参照 hypeman
  `lib/providers/auto_standby_linux.go` 的 store 模式:ListInstances/
  StandbyInstance/SetRuntime/SubscribeInstanceEvents)+ `autostandby.NewConntrackSource`。
  providers 版不可直接 import(依赖 `cmd/api/config`),适配器在 firepaas 仓内实现
  (约百行,上游化候选)。
- 唤醒路径不变:M4.5 autoresume(agent proxy `GetEndpoint` 遇 standby 同步
  restore + reattachSlot)。

### 2. 策略归属与下发链路

- 策略是 **deployment spec 的一部分**(`auto_standby: {enabled, idle_timeout_seconds}`),
  随 rollout 语义变更(改策略 = 新 deployment);PG migrations 扩展 deployments 表。
- proto `MachineSpec` 增加 `auto_standby`;agent create 时翻译为 hypeman
  `CreateInstanceRequest.AutoStandby`(`Policy{Enabled, IdleTimeout}`)。默认关闭——
  未声明策略的 app 行为与 M5 完全一致。

### 3. 探针流量必须排除(关键集成点)

host 侧 readiness 探针(ADR-0008,health worker → slot IP:port)会被 conntrack
记为入站活动,导致实例**永不清闲**。因此策略下发时必须注入
`IgnoreSourceCIDRs` 覆盖探针源段(slot 网关/veth 段,如 `10.12.0.0/16`);平台保留
字段 `IgnoreDestinationPorts` 透传给 app 声明(如监控拨测端口)。

### 4. 路由与对账语义(已验证的既有行为,本 ADR 确认为契约)

- **standby 实例保留在 route backends**:health worker 对非 RUNNING 实例跳过探针
  并冻结最后 readiness(`internal/agent/health` probeDue 的 `isRunning` 过滤);
  route 投影不按 state 摘除 backend。standby 实例冻结在 READY/UNCONFIGURED,
  流量可达 → 触发唤醒。这是 M4.5 autoresume 已验收的行为,本 ADR 将其升格为
  **显式契约**:"standby 是可服务态,readiness 冻结在入睡前值"。
- **reconcile 不重建 PAUSED 实例**:`agentStateUsable` 含 PAUSED;auto-standby 的
  状态迁移不产生 operation(节点内事件),不触发 R1–R8 任何重建分支。
- NOT_READY 入睡的实例冻结在 NOT_READY,edge 不路由、永不唤醒——正确语义
  (未就绪不接流量),文档化即可。

### 5. 发布门控调整(ADR-0015 扩展)

PREPARING 期间新代副本无真实流量(探针已排除,见决策 3),idle_timeout 到点会
standby。CUTOVER 门控从"全部目标 ordinal RUNNING+READY"扩展为
"RUNNING 或 PAUSED,且 readiness=READY/UNCONFIGURED(冻结值)"——切流后首个请求
承担唤醒延迟(autoresume SLO <5s 内)。降级路径:若实测首请求延迟超 SLO,回退为
"PREPARING 期间禁用策略、CUTOVER 后经 UpdateMachine 热启用"(hypeman
UpdateInstanceRequest 支持策略替换)。

## 理由

1. **现成且更准**:conntrack 看真实入站连接(含握手中的 SYN),比 CPU 阈值启发式
   更接近"有真实需求"语义;hypeman 实现含重启恢复(idle 计时持久化)、全量 sync
   兜底、后台限并发——自建 usage 管道要重新解决这一整类问题。
2. **零新协议**:策略经既有 create 链路下发,状态经既有 ListMachines 上报,
   唤醒经既有 autoresume;控制面只新增"deployment 策略字段 + 门控接受 PAUSED"。
3. **闭环已半验证**:M4.5 的 50 次 pause/resume + autoresume 验收覆盖了
   standby→restore→reattachSlot 链路;本 ADR 只补"自动入睡"这一侧。

## 后果

- v1.1-A 工作包:mvp-plan §11 的"自动 idle 检测"遗留项关闭;原 per-VM usage 管道
  方案废止(观测动机由 per-VM 指标直抓覆盖,v1.1-F)。
- agentd 新增后台控制器与 conntrack 权限依赖(root 已满足;非 Linux/无
  CAP_NET_ADMIN 时代码路径需可降级关闭,沿用 hypeman unsupported 构建模式)。
- metrics 新增:auto-standby 触发计数、唤醒耗时分布;事件表记录 standby 迁移
  (审计可见"谁睡了多久")。
- e2e 新增断言:idle→PAUSED→curl 唤醒 200、内存释放(VMM 进程归零)、
  50 轮无泄漏、默认关闭回归。
