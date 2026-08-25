module github.com/example/firepaas/control-plane

go 1.25.4

// P1 阶段按需添加依赖:
//   github.com/gin-gonic/gin (或沿用 hypeman 的 chi + oapi-codegen 风格)
//   github.com/jackc/pgx/v5
//   github.com/redis/go-redis/v9
//   github.com/hashicorp/nomad/api
//   google.golang.org/grpc
