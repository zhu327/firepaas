# LAYER: l1-base  PREREQ: -  DESTRUCTIVE: -  FROZEN: no
# ADR-0040 G1 spike：第二 Nomad client（firepaas-lab-2），与 nomad-single.hcl
# （server+client，bind 127.0.0.1）同主机并存。bind 127.0.0.2 → 默认端口
# （4646/4648）不冲突。用法（root，与主 Nomad 同一 data 磁盘约定）：
#   sudo nomad agent -config=scripts/lab/nomad-client2.hcl \
#        -data-dir=/var/lib/firepaas-p0/nomad-client2
datacenter = "dc1"
region     = "global"
name       = "firepaas-lab-2"
bind_addr  = "127.0.0.2"

# Nomad 2.x 拒绝隐式 localhost advertise，显式指定（客户端不服务 RPC/
# serf，但三键全填最省心）。
advertise {
  http = "127.0.0.2"
  rpc  = "127.0.0.2"
  serf = "127.0.0.2"
}

client {
  enabled   = true
  node_pool = "compute"

  # ADR-0040 G1 spike：node-b 标记（agentd-fabric-b.hcl 按此约束放置；
  # node-a job 用 `!= "b"` 排斥，无标记的单机实验室不受影响）。
  meta {
    fabric_node = "b"
  }

  server_join {
    retry_join = ["127.0.0.1:4647"]
    retry_max  = 0
  }

  # raw_exec 以 root client 运行（agentd 需要 KVM/netns/eBPF）。
  options = {
    "driver.raw_exec.enable" = "1"
  }
}

plugin "raw_exec" {
  config {
    enabled = true
  }
}
