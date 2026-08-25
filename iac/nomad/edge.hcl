# firepaas edge-proxy service 作业（M1 起提供最小正式流量路径，M3 完整路由）

job "firepaas-edge" {
  type      = "service"
  node_pool = "control"
  priority  = 85

  group "edge" {
    count = 2

    constraint {
      operator  = "distinct_hosts"
      value     = "true"
    }

    network {
      port "http" {
        static = 80
      }
      port "https" {
        static = 443
      }
      port "health" {
        static = 3003
      }
    }

    service {
      name     = "firepaas-edge"
      port     = "http"
      provider = "nomad"

      check {
        type     = "http"
        path     = "/health"
        port     = "health"
        interval = "5s"
        timeout  = "3s"
      }
    }

    task "edge" {
      driver = "docker"

      env {
        FIREPAAS_EDGE_PORT          = "80"
        FIREPAAS_EDGE_TLS_PORT      = "443"
        FIREPAAS_EDGE_HEALTH_PORT   = "3003"
        # api job 以 provider=consul 注册,此地址依赖 Consul DNS(见 iac/README.md 服务发现;
        # 未配置 .consul 转发时临时替换为静态地址)
        FIREPAAS_EDGE_API_GRPC_ADDR = "firepaas-api-grpc.service.consul:5009"
        # route catalog 所需;替换为实验室 Redis 地址(control 节点 systemd/docker)
        FIREPAAS_REDIS_ADDR         = "10.0.0.11:6379"
      }

      config {
        image       = "registry.internal/firepaas/edge:latest"
        network_mode = "host"
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
