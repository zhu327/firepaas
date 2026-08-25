#!/usr/bin/env bash
# firepaas 实验室引导脚本(P0.1)
# 在每台 Ubuntu 24.04 节点上运行:安装基础依赖、Nomad/Consul、系统调优、写入节点池配置。
#
# 节点池方案(唯一方案,禁止与 -meta constraint 混用,见 iac/README.md):
#   server  : Nomad server + client(加入 control 池),共 3 台
#   compute : Nomad client(加入 compute 池),KVM 必需,共 2 台
#
# 用法:
#   sudo bash scripts/bootstrap-lab.sh server                 # 3 台 server/control 节点
#   sudo bash scripts/bootstrap-lab.sh compute <server-ip>    # 2 台 compute 节点
#
# 集群就绪后(任一 server):
#   nomad node pool create iac/nomad/pools/control.hcl
#   nomad node pool create iac/nomad/pools/compute.hcl
set -euo pipefail

ROLE="${1:-}"
JOIN_IP="${2:-}"

if [[ "$ROLE" != "server" && "$ROLE" != "compute" ]]; then
  echo "用法: sudo bash scripts/bootstrap-lab.sh <server|compute> [server-ip]" >&2
  exit 1
fi
if [[ "$ROLE" == "compute" && -z "$JOIN_IP" ]]; then
  echo "ERROR: compute 角色需要提供 Nomad/Consul server IP 作为第二个参数" >&2
  exit 1
fi

echo "==> 安装基础包"
apt-get update -y
apt-get install -y --no-install-recommends curl wget unzip gnupg lsb-release ca-certificates \
  erofs-utils jq

echo "==> 安装 HashiCorp apt 仓库(Nomad + Consul)"
wget -qO- https://apt.releases.hashicorp.com/gpg | gpg --dearmor > /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
  > /etc/apt/sources.list.d/hashicorp.list
apt-get update -y
apt-get install -y nomad consul

echo "==> 安装 Docker(docker driver / 镜像构建)"
curl -fsSL https://get.docker.com | sh

if [[ "$ROLE" == "compute" ]]; then
  echo "==> 检查 KVM"
  if [ ! -e /dev/kvm ]; then
    echo "ERROR: /dev/kvm 不存在。物理机请开启 VT-x/AMD-V;云上请使用裸金属或嵌套虚拟化实例。" >&2
    exit 1
  fi

  echo "==> 系统调优(compute)"
  sysctl -w net.ipv4.ip_forward=1
  echo 'net.ipv4.ip_forward=1' > /etc/sysctl.d/99-firepaas.conf
  sysctl -w net.netfilter.nf_conntrack_max=2097152 || true
  echo 'net.netfilter.nf_conntrack_max=2097152' >> /etc/sysctl.d/99-firepaas.conf

  echo "==> 巨页配置(compute,P0.2 基准后再调)"
  echo 2048 > /proc/sys/vm/nr_hugepages || true
  echo 'vm.nr_hugepages=2048' >> /etc/sysctl.d/99-firepaas.conf
fi

echo "==> 安装 Go(如无)"
if ! command -v go >/dev/null; then
  curl -fsSL https://go.dev/dl/go1.25.4.linux-amd64.tar.gz | tar -C /usr/local -xz
  ln -sf /usr/local/go/bin/go /usr/local/bin/go
fi

echo "==> 写入 Consul 配置"
mkdir -p /etc/consul.d
if [[ "$ROLE" == "server" ]]; then
  cat > /etc/consul.d/consul.hcl <<'EOF'
datacenter = "dc1"
server     = true
bootstrap_expect = 3
client_addr = "0.0.0.0"
# 3 台 server 互相发现:填入全部 3 台 server IP 后取消注释
# retry_join = ["10.0.0.11", "10.0.0.12", "10.0.0.13"]
EOF
else
  cat > /etc/consul.d/consul.hcl <<EOF
datacenter = "dc1"
server     = false
retry_join = ["${JOIN_IP}"]
EOF
fi

echo "==> 写入 Nomad 配置(节点池由本文件唯一决定,禁止 -meta 方案)"
mkdir -p /etc/nomad.d
if [[ "$ROLE" == "server" ]]; then
  cat > /etc/nomad.d/nomad.hcl <<'EOF'
datacenter = "dc1"
region     = "global"

server {
  enabled          = true
  bootstrap_expect = 3
}

# server 节点兼任 control 池 client(api/edge 等基础设施 service job)
client {
  enabled   = true
  node_pool = "control"
}

plugin "raw_exec" {
  config { enabled = true }
}
EOF
else
  cat > /etc/nomad.d/nomad.hcl <<EOF
datacenter = "dc1"
region     = "global"

client {
  enabled   = true
  node_pool = "compute"
  server_join {
    retry_join = ["${JOIN_IP}:4647"]
    retry_max  = 0
  }
}

# agentd/hypeman 以 raw_exec 运行(需要 root:KVM/netns/cgroup)
plugin "raw_exec" {
  config { enabled = true }
}
EOF
fi

echo "==> 完成"
echo "下一步:"
echo "  1. [server] 编辑 /etc/consul.d/consul.hcl,填入 retry_join(全部 3 台 server IP)"
echo "  2. 启动: systemctl enable --now consul nomad"
echo "  3. 集群就绪后(任一 server)创建节点池:"
echo "       nomad node pool create iac/nomad/pools/control.hcl"
echo "       nomad node pool create iac/nomad/pools/compute.hcl"
echo "  4. 验证: nomad server members && nomad node status"
echo "  5. Consul DNS(可选,*.service.consul 解析): 将 .consul 域转发到 127.0.0.1:8600"
echo "     (dnsmasq: server=/consul/127.0.0.1#8600;注意与 systemd-resolved 的端口冲突,"
echo "      也可先在 job env 中使用静态地址,详见 iac/README.md 服务发现)"
