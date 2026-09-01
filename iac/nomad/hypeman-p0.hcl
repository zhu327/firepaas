# M0 专用：原版 hypeman 的单节点数据面验证。
# 单机版默认直接执行构建产物（Nomad 2.0 不支持 file:// artifact，故不用 artifact）。
# 多机版需另写 hypeman-p0-remote.hcl：artifact 用 http(s):// 源 + 校验 checksum。
# 该 job 不代表 M1+ agentd 的端口、身份或安全模型。

variable "hypeman_command" {
  type    = string
  default = "~/.local/firepaas-lab/bin/hypeman"
}

variable "hypeman_config" {
  type    = string
  default = "~/Learn/firepaas/scripts/lab/hypeman-p0.yaml"
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
        # 受限网络下 Docker Hub 镜像站（hypeman lab 补丁）；无此限制的环境可置空。
        HYPEMAN_DOCKER_HUB_MIRROR = "docker.m.daocloud.io"
        # 重要:hypeman 配置文件中不得设置任何 ingress(api.hostname 留空),
        # 内嵌 Caddy/DNS 不启动,避免与 Nomad/Consul 及未来 edge 端口冲突。
        # P0 只验证数据面(pull/run/exec/logs/snapshot)。
      }

      config {
        command = var.hypeman_command
      }
    }
  }
}
