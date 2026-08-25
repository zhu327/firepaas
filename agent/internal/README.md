# agent 内部结构(P1 开始填充)

```
agent/
├── cmd/agentd/           # 入口:wire/配置/启动 gRPC + 后台控制器
├── internal/server/      # gRPC 服务实现(InfoService/MachineService/ImageService)
├── internal/machine/     # 包装 hypeman lib/instances 的 Machine 生命周期
├── internal/image/       # 包装 hypeman lib/images + 本地缓存
├── internal/network/     # workload endpoint 抽象：M1 bridge adapter，M3 netns slot
├── internal/proxy/       # M1 起唯一流量入口，edge mTLS + execution/credential 校验
├── internal/info/        # 容量/用量采集(基于 hypeman lib/resources + lib/vm_metrics)
└── internal/state/       # agent operation ledger + 崩溃恢复缓存(非业务源真相)
```

设计红线:
- hypeman 通过 go 工作区引入(根 go.work `use ../hypeman`,要求 firepaas 与 hypeman 同级 checkout;
  独立构建 agent 时用 `go.mod replace github.com/kernel/hypeman => ../../hypeman`),
  只 import `lib/*`,不改 hypeman 上游行为;需要修改时先上游化再升级。
- **版本 pin 策略**:发布/生产构建不依赖同级 checkout 的最新代码——agent 的 go.mod 将
  hypeman pin 到具体 commit/tag(定期、有意识地升级并跑 agent 回归),go.work/replace 仅用于
  本地开发联调;CI 增加一条“禁止 replace 指向 ../ 进入发布构建”的检查,防止上游漂移打穿。
- agent 本地 runtime metadata 只是 observed 恢复缓存，业务权威状态在 PG；Redis 仅为投影。operation ledger 是节点侧幂等权威，必须原子持久化 request hash/result 并可在重启后重放。
- edge/catalog 不得感知 slot IP；proxy 通过内部 workload endpoint 接口解析 bridge/slot 地址。
- proxy credential 仅由 Create 请求单向接收并保存验证材料/摘要，不进入 Machine/ListMachines/日志/operation result。
- gRPC 端口 5108、proxy 端口 5107(与 e2b 的 5008/5007 区分,避免混部冲突)。
