# M0 专用：原版 hypeman 的单节点数据面验证。
# 使用前必须把 artifact、checksum（若由发布系统支持）、config 和 data_dir 调整为实验环境真实值。
# 该 job 不代表 M1+ agentd 的端口、身份或安全模型。

job "firepaas-hypeman-p0" {
  type = "system"
  node_pool = "compute"

  group "hypeman" {
    network {
      port "api" { static = 4973 }
    }

    service {
      name = "firepaas-hypeman-p0"
      port = "api"
      provider = "nomad"
      check {
        type = "http"
        path = "/health"
        interval = "20s"
        timeout = "5s"
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
        memory = 1024
        memory_max = -1
      }

      env {
        HYPEMAN_DATA_DIR = "/var/lib/firepaas-p0/hypeman"
        HYPEMAN_PORT = "4973"
        # 重要:hypeman 配置文件中不得设置任何 ingress(api.hostname 留空),
        # 内嵌 Caddy/DNS 不启动,避免与 Nomad/Consul 及未来 edge 端口冲突。
        # P0 只验证数据面(pull/run/exec/logs/snapshot)。
      }

      config {
        command = "/bin/bash"
        # 发布前替换为实际 config 路径与启动参数。
        args = ["-c", "chmod +x local/hypeman && exec local/hypeman --data-dir ${HYPEMAN_DATA_DIR}"]
      }

      artifact {
        source = "https://artifacts.internal/hypeman/hypeman-REPLACE-ME"
        destination = "local/hypeman"
        mode = "file"
      }
    }
  }
}
