# Production edge job. Required variables deliberately have no defaults.
variable "edge_image" {
  type = string
  validation { condition = can(regex("^[^/]+/.+@sha256:[a-fA-F0-9]{64}$", var.edge_image)); error_message = "edge_image must be an immutable registry digest reference." }
}
variable "redis_addr" { type = string }
variable "api_addr" { type = string }
variable "api_token" { type = string; sensitive = true }
variable "edge_tls_cert" { type = string; sensitive = true }
variable "edge_tls_key" { type = string; sensitive = true }
variable "edge_tls_ca" { type = string; sensitive = true }
# Public ingress certificate/key are distinct from the edge mTLS client identity.
variable "edge_server_cert" { type = string; sensitive = true }
variable "edge_server_key" { type = string; sensitive = true }

job "firepaas-edge" {
  type = "service"
  node_pool = "control"
  priority = 85
  group "edge" {
    count = 2
    constraint { operator = "distinct_hosts"; value = "true" }
    network { port "http" { static = 80 }; port "https" { static = 443 } }
    service {
      name = "firepaas-edge"
      port = "http"
      provider = "nomad"
      # edge exposes /healthz on its HTTP listener; it does not listen on a separate health port.
      check { type = "http"; path = "/healthz"; interval = "5s"; timeout = "3s" }
    }
    task "edge" {
      driver = "docker"
      env {
        FIREPAAS_EDGE_PORT = "80"
        # The binary expects a listen address, not FIREPAAS_EDGE_TLS_PORT.
        FIREPAAS_EDGE_TLS_LISTEN = ":443"
        FIREPAAS_API_ADDR = var.api_addr
        FIREPAAS_REDIS_ADDR = var.redis_addr
        FIREPAAS_API_TOKEN = var.api_token
        FIREPAAS_EDGE_TLS_CERT = "secrets/agent-client.crt"
        FIREPAAS_EDGE_TLS_KEY = "secrets/agent-client.key"
        FIREPAAS_EDGE_TLS_CA = "secrets/agent-ca.crt"
        FIREPAAS_EDGE_SERVER_CERT = "secrets/edge-server.crt"
        FIREPAAS_EDGE_SERVER_KEY = "secrets/edge-server.key"
      }
      # Variables carry PEM contents; templates materialize them inside the
      # allocation with restrictive permissions rather than relying on host paths.
      template { data = var.edge_tls_cert; destination = "secrets/agent-client.crt"; perms = "0400" }
      template { data = var.edge_tls_key; destination = "secrets/agent-client.key"; perms = "0400" }
      template { data = var.edge_tls_ca; destination = "secrets/agent-ca.crt"; perms = "0444" }
      template { data = var.edge_server_cert; destination = "secrets/edge-server.crt"; perms = "0400" }
      template { data = var.edge_server_key; destination = "secrets/edge-server.key"; perms = "0400" }
      config { image = var.edge_image; network_mode = "host" }
      resources { cpu = 500; memory = 512 }
    }
  }
}
