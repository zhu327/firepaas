# Production edge job. Required variables deliberately have no defaults.
variable "edge_image" {
  type = string
  # Nomad variable validation has no regex(); this is the index-free equivalent of
  # ^[^/]+/.+@sha256:[a-fA-F0-9]{64}$ built from split/substr/element/contains.
  validation {
    condition = (
      length(split("@sha256:", var.edge_image)) == 2
      && substr(split("@sha256:", var.edge_image)[0], 0, 1) != "/"
      && length(split("/", split("@sha256:", var.edge_image)[0])) >= 2
      && length(split("", element(split("@sha256:", var.edge_image), 1))) == 64
      && !contains(
        [
          for c in split("", element(split("@sha256:", var.edge_image), 1)) :
          contains(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"], lower(c))
        ],
        false
      )
    )
    error_message = "Edge image must be an immutable registry digest reference."
  }
}
variable "redis_addr" {
  type = string
}
variable "api_addr" {
  type = string
}
variable "api_token" {
  type = string
}
variable "edge_tls_cert" {
  type = string
}
variable "edge_tls_key" {
  type = string
}
variable "edge_tls_ca" {
  type = string
}
# Public ingress certificate/key are distinct from the edge mTLS client identity.
variable "edge_server_cert" {
  type = string
}
variable "edge_server_key" {
  type = string
}

job "firepaas-edge" {
  type      = "service"
  node_pool = "control"
  priority  = 85
  group "edge" {
    count = 2
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }
    network {
      port "http" {
        static = 80
      }
      port "https" {
        static = 443
      }
      # 指标端口：host network 下直接占用宿主 9465；仅供 observability 网段访问。
      port "metrics" {
        static = 9465
      }
    }
    service {
      name     = "firepaas-edge"
      port     = "http"
      provider = "nomad"
      # edge exposes /healthz on its HTTP listener; it does not listen on a separate health port.
      check {
        type     = "http"
        path     = "/healthz"
        interval = "5s"
        timeout  = "3s"
      }
    }
    service {
      name     = "firepaas-edge-metrics"
      port     = "metrics"
      provider = "nomad"
      # Do not expose this service outside the observability ACL boundary.
      tags = ["metrics", "prometheus-scrape"]
      # 注意：若给 edge 配置 FIREPAAS_EDGE_METRICS_TOKEN，本 check 会因 401
      # 失败（check 不带 Authorization 头）；启用 token 时须同步给 check
      # 加 header 或改用 script check，默认不设 token 则不受影响。
      check {
        type     = "http"
        path     = "/metrics"
        name     = "edge-metrics"
        interval = "30s"
        timeout  = "5s"
      }
    }
    task "edge" {
      driver = "docker"
      env {
        FIREPAAS_EDGE_PORT = "80"
        # The binary expects a listen address, not FIREPAAS_EDGE_TLS_PORT.
        FIREPAAS_EDGE_TLS_LISTEN  = ":443"
        FIREPAAS_API_ADDR         = var.api_addr
        FIREPAAS_REDIS_ADDR       = var.redis_addr
        FIREPAAS_API_TOKEN        = var.api_token
        FIREPAAS_EDGE_TLS_CERT    = "secrets/agent-client.crt"
        FIREPAAS_EDGE_TLS_KEY     = "secrets/agent-client.key"
        FIREPAAS_EDGE_TLS_CA      = "secrets/agent-ca.crt"
        FIREPAAS_EDGE_SERVER_CERT = "secrets/edge-server.crt"
        FIREPAAS_EDGE_SERVER_KEY  = "secrets/edge-server.key"
        # 固定端口以匹配上方 static port；metrics 默认只绑 loopback，
        # host network + 跨主机 Prometheus/Nomad check 必须显式绑 0.0.0.0
        # （与 agent.hcl 的 FIREPAAS_AGENT_METRICS_BIND 同一纪律），
        # 再由网络 ACL 只放行 observability 网段。
        FIREPAAS_EDGE_METRICS_PORT = "9465"
        FIREPAAS_EDGE_METRICS_BIND = "0.0.0.0"
      }
      # Variables carry PEM contents; templates materialize them inside the
      # allocation with restrictive permissions rather than relying on host paths.
      template {
        data        = var.edge_tls_cert
        destination = "secrets/agent-client.crt"
        perms       = "0400"
      }
      template {
        data        = var.edge_tls_key
        destination = "secrets/agent-client.key"
        perms       = "0400"
      }
      template {
        data        = var.edge_tls_ca
        destination = "secrets/agent-ca.crt"
        perms       = "0444"
      }
      template {
        data        = var.edge_server_cert
        destination = "secrets/edge-server.crt"
        perms       = "0400"
      }
      template {
        data        = var.edge_server_key
        destination = "secrets/edge-server.key"
        perms       = "0400"
      }
      config {
        image        = var.edge_image
        network_mode = "host"
        # 镜像以 nonroot(65532) 运行；host network 下绑定 :80/:443 需要内核
        # 能力 NET_BIND_SERVICE，不因此退回 root 容器。
        cap_add = ["NET_BIND_SERVICE"]
      }
      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
