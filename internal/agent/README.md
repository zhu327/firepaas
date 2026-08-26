# agent 内部结构(P1 开始填充)

```
cmd/agentd/                 # 入口:配置/启动 gRPC + 后台控制器
internal/agent/server/      # gRPC 服务实现(InfoService/MachineService/ImageService)
internal/agent/machine/     # 包装 hypeman lib/instances 的 Machine 生命周期
internal/agent/image/       # 包装 hypeman lib/images + 本地缓存
internal/agent/network/     # workload endpoint 抽象：M1 bridge adapter，M3 netns slot
internal/agent/proxy/       # M1 起唯一流量入口，edge mTLS + execution/credential 校验
internal/agent/info/        # 容量/用量采集(基于 hypeman lib/resources + lib/vm_metrics)
internal/agent/state/       # agent operation ledger + 崩溃恢复缓存(非业务源真相)
```

设计红线:
- hypeman 通过根 go.work `use ../hypeman` 引入(firepaas 与 hypeman 同级 checkout);
  独立构建用根 `go.mod replace github.com/kernel/hypeman => ../hypeman` + GOWORK=off。
  只 import `lib/*`,不改 hypeman 上游行为;需要修改时先上游化再升级。
- **版本 pin 策略**:发布/生产构建不依赖同级 checkout 的最新代码——agent 的 go.mod 将
  hypeman pin 到具体 commit/tag(定期、有意识地升级并跑 agent 回归),go.work/replace 仅用于
  本地开发联调;CI 增加一条“禁止 replace 指向 ../ 进入发布构建”的检查,防止上游漂移打穿。
- agent 本地 runtime metadata 只是 observed 恢复缓存，业务权威状态在 PG；Redis 仅为投影。operation ledger 是节点侧幂等权威，必须原子持久化 request hash/result 并可在重启后重放。
- edge/catalog 不得感知 slot IP；proxy 通过内部 workload endpoint 接口解析 bridge/slot 地址。
- proxy credential 仅由 Create 请求单向接收并保存验证材料/摘要，不进入 Machine/ListMachines/日志/operation result。
- gRPC 端口 5108、proxy 端口 5107(与 e2b 的 5008/5007 区分,避免混部冲突)。

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
