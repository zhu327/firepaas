# firepaas 控制面 API service 作业(P1 起使用,参考 e2b job-api/api.hcl)

job "firepaas-api" {
  type      = "service"
  node_pool = "control"
  priority  = 90

  group "api" {
    # ADR-0007:M1 单写者 → count=1;M2a leader election(PG advisory lock)后可升 2;
    # M2b 多写路径交付后放开。副本数提升是代码能力的结果,不是部署参数自由选择。
    count = 1

    constraint {
      operator  = "distinct_hosts"
      value     = "true"
    }

    update {
      max_parallel      = 1
      canary            = 1
      min_healthy_time  = "30s"
      healthy_deadline  = "5m"
      progress_deadline = "6m"
      auto_promote      = true
      auto_revert       = true
    }

    restart {
      interval = "5s"
      attempts = 2
      delay    = "5s"
      mode     = "delay"
    }

    network {
      port "api" {
        static = 8080
      }
      port "api-grpc" {
        static = 5009
      }
    }

    # provider=consul:供 edge/CLI 经 Consul DNS 寻址(firepaas-api.service.consul)。
    # agent 的节点发现仍走 Nomad native(见 iac/README.md 服务发现)。
    service {
      name     = "firepaas-api"
      port     = "api"
      provider = "consul"

      check {
        type     = "http"
        path     = "/health"
        interval = "5s"
        timeout  = "3s"
      }
    }

    service {
      name     = "firepaas-api-grpc"
      port     = "api-grpc"
      provider = "consul"

      check {
        type     = "tcp"
        interval = "5s"
        timeout  = "3s"
      }
    }

    task "api" {
      driver = "docker"

      env {
        FIREPAAS_HTTP_PORT = "8080"
        FIREPAAS_GRPC_PORT = "5009"
        # PG/Redis/Nomad 地址由 Vault/Nomad template 注入(生产化 P4)
      }

      config {
        image = "registry.internal/firepaas/api:latest"
        ports = ["api", "api-grpc"]
      }

      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
