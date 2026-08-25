# M1+ agentd system job。此文件是安全/部署契约骨架，不可在填入真实
# artifact、mTLS、host volume 与节点池策略前用于生产。
# P0 原版 hypeman 验证请使用独立的 hypeman-p0.hcl。

job "firepaas-agent" {
  type      = "system"
  node_pool = "compute" # 前提：compute client 已实际加入 Nomad compute node pool
  priority  = 91

  group "agent" {
    network {
      port "agent-grpc" { static = 5108 }
      port "agent-proxy" { static = 5107 }
      port "health" { static = 5109 }
    }

    service {
      name = "firepaas-agent"
      port = "agent-grpc"
      provider = "nomad"
      check {
        type = "http"
        port = "health"
        path = "/health"
        name = "agent-health"
        interval = "20s"
        timeout = "5s"
      }
    }

    service {
      name = "firepaas-agent-proxy"
      port = "agent-proxy"
      provider = "nomad"
      check {
        type = "tcp"
        name = "agent-proxy-health"
        interval = "30s"
        timeout = "1s"
      }
    }

    # 具体 host volume、KVM device、cgroup/netns 权限采用组织批准的 Nomad client
    # 配置提供；提交 production job 时必须在此处以可审计的方式显式声明。
    task "agentd" {
      driver = "raw_exec"

      # agentd 是每节点数据面管理者:Nomad 必须自动拉起崩溃的 agentd,
      # 控制面对账(ADR-0003)依赖 agent 重启后重新扫描上报 observed state。
      restart {
        attempts = 5
        delay    = "5s"
        interval = "10m"
        mode     = "fail"
      }

      resources {
        memory = 1024
        memory_max = -1
      }

      env {
        NODE_ID = "${node.unique.name}"
        NODE_IP = "${attr.unique.network.ip-address}"
        FIREPAAS_AGENT_GRPC_PORT = "5108"
        FIREPAAS_AGENT_PROXY_PORT = "5107"
        FIREPAAS_AGENT_HEALTH_PORT = "5109"
        FIREPAAS_AGENT_DATA_DIR = "/var/lib/firepaas"
        # mTLS certificate/key/CA 路径必须由 template/Vault 注入；禁止明文写入 job。
      }

      config {
        command = "/bin/bash"
        args = ["-c", "chmod +x local/agentd && exec local/agentd"]
      }

      artifact {
        # 发布流程必须替换成版本化、校验过的实际地址；latest/占位 URL 禁止进入生产。
        source = "https://artifacts.internal/firepaas/agentd-REPLACE-ME"
        destination = "local/agentd"
        mode = "file"
      }
    }
  }
}
