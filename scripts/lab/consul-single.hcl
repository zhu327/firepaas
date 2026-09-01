# firepaas 单机 Consul 配置（M0 可选，M1 服务发现必需）。
# 与 bootstrap-lab.sh 的 3-server 配置互斥。端口默认：8300-8302 / 8500 / 8600。

datacenter       = "dc1"
data_dir         = "$HOME/.local/firepaas-lab/run/consul"
bind_addr        = "127.0.0.1"
client_addr      = "127.0.0.1"
advertise_addr   = "127.0.0.1"

server           = true
bootstrap_expect = 1

ui_config {
  enabled = true
}
