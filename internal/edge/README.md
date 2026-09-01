# edge 内部结构

```
cmd/edge-proxy/          # 进程装配：配置、Redis、TLS、listeners、shutdown
internal/edge/handler.go # 完整请求生命周期：route/cache、选择/并发、token、转发、一次安全重试
internal/edge/edge.go    # route/token 本地缓存与 hostname 限流
```

`internal/edge.Handler` 是数据面行为所有者。命令包不参与 backend eligibility、
pinning、least-inflight/hard limit、凭证、header 清理或 retry 状态机；每次转发的
retry 结果保存在 attempt-local state，不使用 package-global 请求映射。
`catalog.Catalog` 直接实现 edge 的窄只读接口，Redis JSON/key 格式不经包装或转换。

流量链路：

```text
client → edge(hostname) → catalog 查 route backend set
       → node agent proxy :5107（mTLS）
       → agent 校验 machine_id/execution_id → workload endpoint（M1 bridge guest IP）
```

edge 永不直连 VM，也不读取 slot IP/netns/TAP 等内部信息；节点内地址只在 agent
proxy 一侧可见。M1 proxy 使用 bridge endpoint adapter，M3 切换 slot adapter
时不改变本契约。
