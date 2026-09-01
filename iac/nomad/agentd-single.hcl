# M1.4 单机 agentd system job（真实可部署；ADR-0012 单机基线）。
# 生产骨架见 iac/nomad/agent.hcl（mTLS/host volume/artifact 契约）；两者不混用。
# 前提：compute 节点池已创建，Nomad client 以 root 运行（scripts/lab/start.sh）。
# 与 hypeman-p0 job 互斥（共享 data_dir 的实例状态）。

# 单机实验室默认值假定仓库 checkout 在 ~/Learn/firepaas 且 lab 工具安装在
# ~/.local/firepaas-lab；环境不同时用 -var 覆盖。
variable "repo_root" {
  type    = string
  default = "~/Learn/firepaas"
}

variable "lab_bin" {
  type    = string
  default = "~/.local/firepaas-lab/bin"
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
      # v1.1（F-2）：per-VM 指标直抓（Prometheus 抓取端点）。
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

    task "agentd" {
      driver = "raw_exec"

      restart {
        attempts = 5
        delay    = "5s"
        interval = "10m"
        mode     = "fail"
      }

      resources {
        memory     = 16384
        memory_max = -1
      }

      env {
        CONFIG_PATH               = "${var.repo_root}/scripts/lab/agentd.yaml"
        HYPEMAN_DOCKER_HUB_MIRROR = "docker.m.daocloud.io"
        FIREPAAS_AGENT_GRPC_PORT  = "5108"
        FIREPAAS_AGENT_PROXY_PORT = "5107"
        FIREPAAS_AGENT_NODE_POOL  = "compute"
        FIREPAAS_AGENT_NODE_ID    = "${node.unique.id}"
        FIREPAAS_AGENT_BIND       = "0.0.0.0"
        FIREPAAS_NETWORK_BACKEND  = "slot"
        # M5.1：镜像解包大小上限（agent 侧准入，超限永久拒绝）。
        FIREPAAS_IMAGE_MAX_UNPACK_MIB = "4096"
        # v1.1（ADR-0017）：auto-standby 空闲检测控制器（conntrack 驱动；
        # 策略 per-app 默认关闭，controller 对无策略实例零动作）。
        FIREPAAS_AGENT_AUTOSTANDBY = "true"
        # v1.1（F-2）：per-VM 指标直抓端点（Prometheus 节点 relabel）。
        FIREPAAS_AGENT_METRICS_PORT = "9464"
        # v1.1（ADR-0018）：PullImage（部署预取）磁盘水位守护（默认 0.9）。
        FIREPAAS_PREFETCH_DISK_WATERMARK = "0.9"
        # M5：secret_env 注入默认 fail-closed（hypeman 会把 Env 明文持久化到
        # metadata.json）。受信节点需注入时取消下行注释（e2e-m5 B 段会临时
        # 渲染 opt-in 副本验证双策略，见 scripts/lab/e2e-m5.sh）。
        # FIREPAAS_SECRET_INJECTION   = "unsafe-persisted-env"
        FIREPAAS_AGENT_TLS_CERT = "${var.repo_root}/scripts/lab/certs/agentd.crt"
        FIREPAAS_AGENT_TLS_KEY  = "${var.repo_root}/scripts/lab/certs/agentd.key"
        FIREPAAS_AGENT_TLS_CA   = "${var.repo_root}/scripts/lab/certs/ca.crt"
      }

      config {
        command = "${var.lab_bin}/agentd"
      }
    }
  }
}
