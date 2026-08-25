# compute 池:Firecracker 数据面节点(agentd / hypeman-p0 system job,KVM 必需)
# Nomad 2.0 语法:node_pool "compute" { ... },用 `nomad node pool apply` 创建/更新。
node_pool "compute" {
  description = "Firecracker compute nodes (KVM required)"

  meta {
    role = "compute"
  }
}
