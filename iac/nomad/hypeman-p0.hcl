# M0 专用：原版 hypeman 的单节点数据面验证。
# 单机实验室默认值指向 scripts/lab/build-hypeman.sh 的产物与配置（ADR-0012）。
# 多机实验室用 -var 覆盖：nomad job plan -var=hypeman_artifact=https://... \
#   -var=hypeman_config=/etc/firepaas-p0/hypeman.yaml iac/nomad/hypeman-p0.hcl
# 该 job 不代表 M1+ agentd 的端口、身份或安全模型。

variable "hypeman_artifact" {
  type    = string
  default = "file:///home/zty/.local/firepaas-lab/bin/hypeman"
}

variable "hypeman_config" {
  type    = string
  default = "/home/zty/Learn/firepaas/scripts/lab/hypeman-p0.yaml"
}

variable "hypeman_data_dir" {
  type    = string
  default = "/var/lib/firepaas-p0/hypeman"
}

job "firepaas-hypeman-p0" {
  type      = "system"
  node_pool = "compute"

  group "hypeman" {
    network {
      port "api" {
        static = 4973
      }
    }

    service {
      name     = "firepaas-hypeman-p0"
      port     = "api"
      provider = "nomad"
      check {
        type     = "http"
        path     = "/health"
        interval = "20s"
        timeout  = "5s"
      }
    }

    task "hypeman" {
      driver = "raw_exec"
      # P0 基准需要崩溃恢复观测:Nomad 自动重启,人工 kill -9 场景对照脚本分辨。
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
        CONFIG_PATH      = var.hypeman_config
        HYPEMAN_DATA_DIR = var.hypeman_data_dir
        HYPEMAN_PORT     = "4973"
        # 重要:hypeman 配置文件中不得设置任何 ingress(api.hostname 留空),
        # 内嵌 Caddy/DNS 不启动,避免与 Nomad/Consul 及未来 edge 端口冲突。
        # P0 只验证数据面(pull/run/exec/logs/snapshot)。
      }

      config {
        command = "/bin/bash"
        args    = ["-c", "chmod +x local/hypeman && exec local/hypeman"]
      }

      artifact {
        source      = var.hypeman_artifact
        destination = "local/hypeman"
        mode        = "file"
      }
    }
  }
}
