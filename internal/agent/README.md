# agent 内部结构(P1 开始填充)

```
cmd/agentd/                 # 入口:配置/启动 gRPC + 后台控制器
internal/agent/server/      # gRPC 服务实现(InfoService/MachineService/ImageService)
internal/agent/machine/     # 包装 hypeman lib/instances 的 Machine 生命周期
internal/agent/image/       # 包装 hypeman lib/images + 本地缓存
internal/agent/network/slot/# M3 slot 管理器：netns/veth/TAP/nftables 原子管理
internal/agent/health/      # M3 host 侧 readiness 探针(ADR-0008)
internal/agent/proxy/       # M1 起唯一流量入口，edge mTLS + execution/credential 校验
internal/agent/info/        # 容量/用量采集(基于 hypeman lib/resources + lib/vm_metrics)
internal/agent/state/       # ledger/fence/credential 的兼容持久化机制
internal/agent/mutation/    # typed fenced-mutation protocol（post-effect/recoverable/tombstone）
```

设计红线:
- hypeman 作为远程 module 经根 `go.mod` replace 消费:
  `replace github.com/kernel/hypeman => github.com/zhu327/hypeman v0.4.0-firepaas`
  (公开 fork 的 firepaas-lib 分支 tag,提交了 go:embed 必需的 firecracker/guest-agent/init
  二进制,可远程拉取,无需同级 checkout)。只 import `lib/*`,不改 hypeman 上游行为;
  agent 本地变化只落在本仓库。
- **fork 策略**:fork 承载 firepaas 必需的 lib 扩展——one-shot secret 下发通道依赖的
  vsock/guest agent 能力(ADR-0024)、image/volume quarantine API、snapshot artifact
  完整性校验(sha256)等(见根 go.mod 注释)。规则:不得删除 replace 或改为浮动版本;
  上游 kernel/hypeman 发布包含所需 API 的正式 tag 后,切换 require 并删除 replace(先评审)。
- **版本 pin 策略**:发布/生产构建不依赖同级 checkout——消费的是 fork 的固定 tag
  `v0.4.0-firepaas`,升级是有意识动作并跑 agent 回归。本地联调未发布变更可创建不入库的
  `go.work.local`;CI 与发布始终以 `GOWORK=off` 走根 go.mod。
- agent 本地 runtime metadata 只是 observed 恢复缓存，业务权威状态在 PG；Redis 仅为投影。operation ledger 是节点侧幂等权威，必须原子持久化 request hash/result 并可在重启后重放。
- `internal/agent/mutation` 明确区分三种协议族：无 pre-effect claim 的 post-effect 操作、可从 inventory 恢复的 durable-claim 操作，以及 Exec 的 non-reattachable tombstone。它只编排 ledger/fence/serialization 原语，不用 flags 或通用 effect callback 隐藏 runtime、credential 与 recovery 的顺序。create/delete、pause/resume、snapshot create/delete/fork/restore、volume create/import/attach/detach/delete、Exec claim 与 CopyTo 均由 typed family 方法编排；server 只保留验证、gRPC error mapping、adapter effect/recovery 和 credential hook。
- edge/catalog 不得感知 slot IP；proxy 通过内部 workload endpoint 接口解析 bridge/slot 地址。
- proxy credential 仅由 Create 请求单向接收并保存验证材料/摘要，不进入 Machine/ListMachines/日志/operation result。
- gRPC 端口 5108、proxy 端口 5107(与 e2b 的 5008/5007 区分,避免混部冲突)。

## M1.4 实现状态(2026-08-26)

- `cmd/agentd`:已实现 InfoService.ServiceInfo 与 MachineService 的
  Create/List/Delete，经 `iac/nomad/agentd-single.hcl` 以 root system job 部署。
- `internal/agent/runtime`:只 import hypeman `lib/*`（配置经 `lib/config` 类型别名
  入口）；不 import `cmd/api` 与 `lib/providers`。
- `internal/agent/state`:operation ledger（原子落盘/重启重放/同 ID 异 hash 拒绝）
  + generation fence（P0-2：machine→最高 generation 高水位，fences.json；旧代
  变更拒 FailedPrecondition，删除后高水位保留，与 ledger 共享 24h GC 窗口）。
  2026-08-26 评审后补齐：Record 记录 machine_id，machine 删除后清理同 machine
  历史 create 记录（保留 delete 自身去重记录），另有年龄 GC（默认 24h，
  `FIREPAAS_AGENT_LEDGER_RETENTION` 可配，启动+每小时执行）。
- `internal/agent/machine`:hypeman CreateInstanceRequest 映射；firepaas 业务标识
  经 hypeman tags 持久化；稳定 `machine_id` = hypeman instance Name，内部 ID 仅在
  Delete 时解析。secret_env 值进 hypeman Env（VM 启动配置），但键名记入
  `firepaas/secret_keys` tag，回显路径（mapMachine）剔除 secret 键（P0-1，
  ADR-0013 不变量 3：响应/ListMachines/ledger 持久化结果均不含 secret 值）。
- `internal/agent/server`:fencing 校验 + request hash 幂等（protojson canonical）
  + generation fence Check/Advance（幂等重放优先于 fence）；
  Delete 的 NotFound 单独映射为 codes.NotFound（控制面据此幂等收敛）。
- `internal/agent/info`:容量/用量取自 dataDir 所在文件系统；已承诺资源
  （mem/vcpu/disk allocated）为 ListInstances 实时求和（实例 Size/Vcpus/
  OverlaySize，v1.2-E 加磁盘维度）；利用率观测经 hypeman `lib/vm_metrics`
  入 OTel 指标。`lib/resources` 直接实时采集未启用。
- 身份（2026-08-26 评审后补齐）：静态 mTLS + 证书 CN 白名单——gRPC(5108) 仅
  接受 control-plane，proxy(5107) 仅接受 edge（`FIREPAAS_AGENT_GRPC_ALLOWED_CLIENTS`
  / `FIREPAAS_AGENT_PROXY_ALLOWED_CLIENTS` 可配，ADR-0006 M1 降级形态的
  授权半边；2026-09 补齐证书热重载与到期指标/告警，per-node 身份、吊销与
  完整矩阵仍为延期项，见 ADR-0006 后果更新）。

## M0.4 spike 状态(2026-08-25,运行 PASS)

`cmd/m0-spike` 不经 HTTP API,直接 import hypeman lib 实际执行 Create/List/Delete:

```bash
sudo env CONFIG_PATH=scripts/lab/hypeman-p0.yaml HYPEMAN_DOCKER_HUB_MIRROR=docker.m.daocloud.io \
  go run ./cmd/m0-spike -image docker.io/library/nginx:alpine
```

已验证的耦合点(全部需要在运行验证后回填结论):

1. `lib/instances.Manager` 与 `lib/images.Manager` 都是**已导出的稳定接口**,
   Create/List/Get/Delete/Stop/Start/Standby/Restore/Snapshot/Fork 齐全,agent 只需包
   接口而不必碰 manager 内部实现——这是“直接依赖”路径的有利证据。
2. 但装配胶水在 `lib/providers`,它 import `cmd/api/config`、builders、builds、
   ingress、registry、imagepush 等单机 API 关注点。agent 若直接用 providers,
   会拉进整套单机控制面依赖图,违背“agent 只 import lib/*”的红线。
3. `config.Config` 目前位于 `cmd/api/config` 而非 `lib/config`;agent 化必须把它
   提取到 lib(上游化),或复制一个最小 agent config 结构。
4. 构造 `instances.Manager` 需要 paths/image/system/network/device/volume 六个
   manager + limits + hypervisor + snapshot defaults + meter/tracer;`NewManagerWithConfigE`
   已经提供了非 panic 的错误路径,适合 agent 使用。
5. 命名不一致:`Instance` 的 Go 字段是 `Id`,JSON 是 `id`;agent 层要自行做风格映射。
6. hypeman-cli 仓库的 go.mod 含 replace 指令,`go install ...@v0.18.0` 会失败,
   需要 clone 后 `go build`(build-hypeman.sh 已处理)。

三选一建议(初步):**直接依赖 + 小范围上游化**。把 `config` 提取到 `lib/config` 并
新增一个 agent 专用装配函数(不引入 ingress/builds),是改动最小且不 fork 的路径;
运行冒烟与冷启动/密度数据通过后正式定论。
