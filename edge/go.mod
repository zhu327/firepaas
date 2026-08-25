module github.com/example/firepaas/edge

go 1.25.4

// P3 阶段按需添加依赖:
//   github.com/caddyserver/caddy/v2 (TLS 层,复用 hypeman lib/ingress 的 Caddy 集成)
//   github.com/redis/go-redis/v9
//   google.golang.org/grpc
