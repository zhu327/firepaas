# edge 内部结构（M1 最小链路，M3 完整路由）

```
edge/
├── cmd/edge-proxy/     # 入口
├── internal/router/    # hostname -> machine 路由解析
├── internal/catalog/   # Redis routing catalog 客户端(带 execution_id CAS)
├── internal/autoresume/# paused machine 的自动唤醒(gRPC ResumeMachine)
└── internal/tls/       # Caddy 集成(复用 hypeman lib/ingress 的证书/ACME 部分;
                        # ACME directory 指向内部 step-ca,按需签发泛域名证书,ADR-0011)
```

流量链路:

```
client -> edge(TLS) -> catalog 查 (nodeProxyEndpoint, executionID, appPort)
                   -> node agent proxy :5107
                   -> agent 内部解析 workload endpoint -> VM
```

edge 永不直连 VM，也不读取 slot IP/netns/TAP 等内部信息；节点内地址只在 agent proxy 一侧可见。M1 proxy 使用 bridge endpoint adapter，M3 切换 slot adapter 时不改变本契约。
