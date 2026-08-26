# edge 内部结构（M1.6 最小链路已落地，M3 完整路由）

```
cmd/edge-proxy/          # 入口：Redis catalog → agent proxy（TLS 可选）
internal/edge/router/    # hostname → route 解析（M1 直接读 catalog）
internal/edge/catalog/   # Redis routing catalog 客户端（复用 internal/controlplane/catalog）
internal/edge/autoresume/# paused machine 的自动唤醒（M4 条件特性）
internal/edge/tls/       # Caddy 集成（复用 hypeman lib/ingress 的证书/ACME 部分，
                         # ACME directory 指向内部 step-ca，ADR-0011）
```

流量链路：

```text
client → edge(hostname) → catalog 查 route backend set
       → node agent proxy :5107（mTLS）
       → agent 校验 machine_id/execution_id → workload endpoint（M1 bridge guest IP）
```

edge 永不直连 VM，也不读取 slot IP/netns/TAP 等内部信息；节点内地址只在 agent
proxy 一侧可见。M1 proxy 使用 bridge endpoint adapter，M3 切换 slot adapter
时不改变本契约。
