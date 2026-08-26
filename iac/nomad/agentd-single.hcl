# M1.4 单机 agentd system job（真实可部署；ADR-0012 单机基线）。
# 生产骨架见 iac/nomad/agent.hcl（mTLS/host volume/artifact 契约）；两者不混用。
# 前提：compute 节点池已创建，Nomad client 以 root 运行（scripts/lab/start.sh）。
# 与 hypeman-p0 job 互斥（共享 data_dir 的实例状态）。

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
        memory     = 1024
        memory_max = -1
      }

      env {
        CONFIG_PATH                  = "/home/zty/Learn/firepaas/scripts/lab/agentd.yaml"
        HYPEMAN_DOCKER_HUB_MIRROR    = "docker.m.daocloud.io"
        FIREPAAS_AGENT_GRPC_PORT     = "5108"
        FIREPAAS_AGENT_PROXY_PORT    = "5107"
        FIREPAAS_AGENT_NODE_POOL     = "compute"
        FIREPAAS_AGENT_NODE_ID       = "${node.unique.name}"
      }

      config {
        command = "/home/zty/.local/firepaas-lab/bin/agentd"
      }
    }
  }
}
