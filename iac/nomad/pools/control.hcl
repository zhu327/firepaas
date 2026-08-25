# control 池:api/edge 等基础设施 service job(3 台 Nomad server 兼任 client)
# Nomad 2.0 语法:node_pool "control" { ... },用 `nomad node pool apply` 创建/更新。
node_pool "control" {
  description = "control-plane / edge / infra service jobs"

  meta {
    role = "control"
  }
}
