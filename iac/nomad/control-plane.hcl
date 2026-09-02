# Production control-plane job. Required variables deliberately have no defaults.
variable "api_image" {
  type = string
  # Nomad variable validation has no regex(); this is the index-free equivalent of
  # ^[^/]+/.+@sha256:[a-fA-F0-9]{64}$ built from split/substr/element/contains.
  validation {
    condition = (
      length(split("@sha256:", var.api_image)) == 2
      && substr(split("@sha256:", var.api_image)[0], 0, 1) != "/"
      && length(split("/", split("@sha256:", var.api_image)[0])) >= 2
      && length(split("", element(split("@sha256:", var.api_image), 1))) == 64
      && !contains(
        [
          for c in split("", element(split("@sha256:", var.api_image), 1)) :
          contains(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"], lower(c))
        ],
        false
      )
    )
    error_message = "API image must be an immutable registry digest reference."
  }
}
variable "postgres_url" {
  type = string
}
variable "redis_addr" {
  type = string
}
variable "nomad_addr" {
  type = string
}
variable "api_token" {
  type = string
  # length() does not accept strings in this context; count runes via split().
  validation {
    condition     = length(split("", var.api_token)) >= 16
    error_message = "API token must be at least 16 characters."
  }
}
variable "secrets_master_key" {
  type = string
}
variable "traffic_token_key" {
  type = string
}
# PEM contents (not host paths) — the task templates below materialize them
# inside the allocation under secrets/, matching edge.hcl. Passing container
# paths here would point at files the image does not have.
variable "agent_tls_cert" {
  type = string
}
variable "agent_tls_key" {
  type = string
}
variable "agent_tls_ca" {
  type = string
}
# The current writer model is intentionally single-active. HA validation must verify
# Nomad/Consul quorum separately; do not claim API write HA until count>1 has been
# exercised with the writer-evolution contract.
#
# 纪律声明（生产就绪 P1#7）：上限放宽到 2 只为解锁多写者 HA 演练。生产环境在
# 完成以下演练并取得通过证据前，count 必须保持 1：
#   - scripts/lab/chaos-control-quorum.sh（控制面 quorum/leader 切换混沌）
#   - docs/runbook-control-plane-quorum.md（quorum 恢复 runbook 全步骤）
#   - docs/runbook-ha-validation.md（HA 验收矩阵）
# 未获上述证据即在生产设置 api_count=2，视为绕过 ADR-0007 单写者决策。
variable "api_count" {
  type    = number
  default = 1
  validation {
    condition     = var.api_count >= 1 && var.api_count <= 2
    error_message = "API count must stay within 1..2; count 2 is for HA rehearsals only until failover is accepted."
  }
}

job "firepaas-api" {
  type      = "service"
  node_pool = "control"
  priority  = 90
  group "api" {
    count = var.api_count # ADR-0007: intentionally one active writer.
    constraint {
      operator = "distinct_hosts"
      value    = "true"
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
      # interval must exceed delay × attempts or Nomad rejects the group.
      interval = "30s"
      attempts = 2
      delay    = "5s"
      mode     = "delay"
    }
    network {
      port "api" {
        static = 8080
      }
    }
    service {
      name     = "firepaas-api"
      port     = "api"
      provider = "consul"
      # /readyz probes real dependencies (PG SELECT 1 + Redis PING, <=1s each);
      # /v1/health stays a static liveness endpoint for cheap probes.
      check {
        type     = "http"
        path     = "/readyz"
        interval = "5s"
        timeout  = "3s"
      }
    }
    task "api" {
      driver = "docker"
      env {
        FIREPAAS_HTTP_PORT          = "8080"
        FIREPAAS_POSTGRES_URL       = var.postgres_url
        FIREPAAS_REDIS_ADDR         = var.redis_addr
        FIREPAAS_NOMAD_ADDR         = var.nomad_addr
        FIREPAAS_API_TOKEN          = var.api_token
        FIREPAAS_SECRETS_MASTER_KEY = var.secrets_master_key
        FIREPAAS_TRAFFIC_TOKEN_KEY  = var.traffic_token_key
        # The binary expects file paths; the PEM variable contents are
        # materialized by the template blocks below (edge.hcl pattern).
        FIREPAAS_AGENT_TLS_CERT       = "secrets/agent-client.crt"
        FIREPAAS_AGENT_TLS_KEY        = "secrets/agent-client.key"
        FIREPAAS_AGENT_TLS_CA         = "secrets/agent-ca.crt"
        FIREPAAS_IMAGE_REQUIRE_DIGEST = "true"
        # Delete mode requires an explicit reviewed rollout after dry-run evidence.
        FIREPAAS_LOCAL_GC_MODE     = "off"
        FIREPAAS_GC_MIN_AGE        = "1h"
        FIREPAAS_GC_HIGH_WATERMARK = "0.85"
        FIREPAAS_GC_LOW_WATERMARK  = "0.70"
        FIREPAAS_GC_INTERVAL       = "5m"
      }
      # Variables carry PEM contents; templates materialize them inside the
      # allocation with restrictive permissions rather than relying on host
      # paths (identical layout to edge.hcl).
      template {
        data        = var.agent_tls_cert
        destination = "secrets/agent-client.crt"
        perms       = "0400"
      }
      template {
        data        = var.agent_tls_key
        destination = "secrets/agent-client.key"
        perms       = "0400"
      }
      template {
        data        = var.agent_tls_ca
        destination = "secrets/agent-ca.crt"
        perms       = "0444"
      }
      config {
        image = var.api_image
        ports = ["api"]
      }
      resources {
        cpu    = 500
        memory = 512
      }
    }
  }
}
