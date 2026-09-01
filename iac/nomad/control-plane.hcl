# Production control-plane job. Required variables deliberately have no defaults.
variable "api_image" {
  type = string
  validation { condition = can(regex("^[^/]+/.+@sha256:[a-fA-F0-9]{64}$", var.api_image)); error_message = "api_image must be an immutable registry digest reference." }
}
variable "postgres_url" { type = string; sensitive = true }
variable "redis_addr" { type = string }
variable "nomad_addr" { type = string }
variable "api_token" { type = string; sensitive = true
  validation { condition = length(var.api_token) >= 16; error_message = "api_token must be at least 16 characters." }
}
variable "secrets_master_key" { type = string; sensitive = true }
variable "traffic_token_key" { type = string; sensitive = true }
variable "agent_tls_cert" { type = string; sensitive = true }
variable "agent_tls_key" { type = string; sensitive = true }
variable "agent_tls_ca" { type = string; sensitive = true }
# The current writer model is intentionally single-active. HA validation must verify
# Nomad/Consul quorum separately; do not claim API write HA until count>1 has been
# exercised with the writer-evolution contract.
variable "api_count" { type = number; default = 1
  validation { condition = var.api_count == 1; error_message = "API count is fixed at one until multi-writer failover is accepted." }
}

job "firepaas-api" {
  type = "service"
  node_pool = "control"
  priority = 90
  group "api" {
    count = var.api_count # ADR-0007: intentionally one active writer.
    constraint { operator = "distinct_hosts"; value = "true" }
    update { max_parallel = 1; canary = 1; min_healthy_time = "30s"; healthy_deadline = "5m"; progress_deadline = "6m"; auto_promote = true; auto_revert = true }
    restart { interval = "5s"; attempts = 2; delay = "5s"; mode = "delay" }
    network { port "api" { static = 8080 } }
    service {
      name = "firepaas-api"
      port = "api"
      provider = "consul"
      check { type = "http"; path = "/v1/health"; interval = "5s"; timeout = "3s" }
    }
    task "api" {
      driver = "docker"
      env {
        FIREPAAS_HTTP_PORT = "8080"
        FIREPAAS_POSTGRES_URL = var.postgres_url
        FIREPAAS_REDIS_ADDR = var.redis_addr
        FIREPAAS_NOMAD_ADDR = var.nomad_addr
        FIREPAAS_API_TOKEN = var.api_token
        FIREPAAS_SECRETS_MASTER_KEY = var.secrets_master_key
        FIREPAAS_TRAFFIC_TOKEN_KEY = var.traffic_token_key
        FIREPAAS_AGENT_TLS_CERT = var.agent_tls_cert
        FIREPAAS_AGENT_TLS_KEY = var.agent_tls_key
        FIREPAAS_AGENT_TLS_CA = var.agent_tls_ca
        FIREPAAS_IMAGE_REQUIRE_DIGEST = "true"
        # Delete mode requires an explicit reviewed rollout after dry-run evidence.
        FIREPAAS_LOCAL_GC_MODE = "off"
        FIREPAAS_GC_MIN_AGE = "1h"
        FIREPAAS_GC_HIGH_WATERMARK = "0.85"
        FIREPAAS_GC_LOW_WATERMARK = "0.70"
        FIREPAAS_GC_INTERVAL = "5m"
      }
      config { image = var.api_image; ports = ["api"] }
      resources { cpu = 500; memory = 512 }
    }
  }
}
