# Production agentd system job. Submit only with -var values supplied from the approved
# release system; defaults are intentionally absent so an omitted artifact fails validation.
variable "agentd_artifact_url" {
  type = string
  # Nomad variable validation has no regex(); equivalent prefix check with substr.
  validation {
    condition     = substr(var.agentd_artifact_url, 0, 8) == "https://"
    error_message = "Agentd artifact URL must use HTTPS."
  }
}
variable "agentd_artifact_sha256" {
  type = string
  validation {
    condition = (
      length(split("", var.agentd_artifact_sha256)) == 64
      && !contains(
        [
          for c in split("", var.agentd_artifact_sha256) :
          contains(["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"], lower(c))
        ],
        false
      )
    )
    error_message = "Agentd artifact SHA256 must be a 64-hex SHA-256 digest."
  }
}
# Paths are rendered/provisioned by the host's protected secret delivery mechanism.
# They are required: production agentd never silently falls back to plaintext.
variable "agent_tls_cert" {
  type = string
}
variable "agent_tls_key" {
  type = string
}
variable "agent_tls_ca" {
  type = string
}

job "firepaas-agentd" {
  type      = "system"
  node_pool = "compute"
  priority  = 91

  group "agent" {
    network {
      port "grpc" {
        static = 5108
      }
      port "proxy" {
        static = 5107
      }
      # Prometheus must reach this only from the trusted observability network;
      # enforce that source restriction with host/network ACLs.
      port "metrics" {
        static = 9464
      }
    }
    service {
      name     = "firepaas-agentd"
      port     = "grpc"
      provider = "nomad"
      check {
        type     = "tcp"
        name     = "agentd-grpc"
        interval = "20s"
        timeout  = "5s"
      }
    }
    service {
      name     = "firepaas-agentd-proxy"
      port     = "proxy"
      provider = "nomad"
      check {
        type     = "tcp"
        name     = "agentd-proxy"
        interval = "30s"
        timeout  = "1s"
      }
    }
    service {
      name     = "firepaas-agentd-metrics"
      port     = "metrics"
      provider = "nomad"
      # Do not expose this service outside the observability ACL boundary.
      tags = ["metrics", "prometheus-scrape"]
      check {
        type     = "http"
        path     = "/metrics"
        name     = "agentd-metrics"
        interval = "30s"
        timeout  = "5s"
      }
    }
    task "agentd" {
      driver = "raw_exec"
      restart {
        attempts = 5
        delay    = "5s"
        interval = "10m"
        mode     = "fail"
      }
      resources {
        memory     = 1024
        memory_max = -1
      }
      env {
        FIREPAAS_AGENT_NODE_ID               = "${node.unique.id}"
        FIREPAAS_AGENT_BIND                  = "0.0.0.0"
        FIREPAAS_AGENT_GRPC_PORT             = "5108"
        FIREPAAS_AGENT_PROXY_PORT            = "5107"
        FIREPAAS_AGENT_METRICS_PORT          = "9464"
        FIREPAAS_AGENT_METRICS_BIND          = "0.0.0.0"
        FIREPAAS_AGENT_NODE_POOL             = "compute"
        FIREPAAS_AGENT_DATA_DIR              = "/var/lib/firepaas"
        FIREPAAS_AGENT_TLS_CERT              = var.agent_tls_cert
        FIREPAAS_AGENT_TLS_KEY               = var.agent_tls_key
        FIREPAAS_AGENT_TLS_CA                = var.agent_tls_ca
        FIREPAAS_AGENT_GRPC_ALLOWED_CLIENTS  = "control-plane"
        FIREPAAS_AGENT_PROXY_ALLOWED_CLIENTS = "edge-proxy"
        FIREPAAS_PROXY_CREDENTIAL_REQUIRED   = "true"
        FIREPAAS_SECRET_INJECTION            = "oneshot"
        FIREPAAS_ADMISSION_DISK_WATERMARK    = "0.9"
      }
      config {
        command = "/bin/bash"
        args    = ["-ec", "chmod 0755 local/agentd && exec local/agentd"]
      }
      artifact {
        source      = var.agentd_artifact_url
        destination = "local/agentd"
        mode        = "file"
        options {
          checksum = "sha256:${var.agentd_artifact_sha256}"
        }
      }
    }
  }
}
