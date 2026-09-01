# firepaas 单机 Nomad 配置（M0 基线，ADR-0012）。
# 与 scripts/bootstrap-lab.sh 写出的 3-server 配置互斥；本文件只服务单机实验室。
# 多机实验室请继续使用 bootstrap-lab.sh 生成的 /etc/nomad.d/nomad.hcl。

datacenter = "dc1"
region     = "global"
name       = "firepaas-lab-1"
data_dir   = "$HOME/.local/firepaas-lab/run/nomad"
bind_addr  = "127.0.0.1"

advertise {
  http = "127.0.0.1"
  rpc  = "127.0.0.1"
  serf = "127.0.0.1"
}

server {
  enabled          = true
  bootstrap_expect = 1
}

client {
  enabled   = true
  node_pool = "compute"

  server_join {
    retry_join = ["127.0.0.1:4647"]
    retry_max  = 0
  }

  # raw_exec 在 root 客户端下用于 hypeman-p0 / agentd（KVM/netns/cgroup 需要 root）。
  options = {
    "driver.raw_exec.enable" = "1"
  }
}

plugin "raw_exec" {
  config {
    enabled = true
  }
}
