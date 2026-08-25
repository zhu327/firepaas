# Terraform:实验室与生产集群引导

P0 阶段先手动 bootstrap(见 iac/README.md),验证通过后(P4)再 Terraform 化。

参考实现(本地 `../infra`):

- GCP:`infra/iac/provider-gcp/nomad-cluster/`(server pool / api pool / worker-cluster)
- AWS:`infra/iac/provider-aws/`(ASG + Packer AMI)
- Nomad 作业模块:`infra/iac/modules/job-*/`

firepaas 私有化裁剪点:

| e2b 依赖 | firepaas 替代 |
|---|---|
| GCS/S3 模板桶 | MinIO / Ceph RGW / 本地 S3(`file://` provider) |
| NFS/Filestore 卷 | 本地盘 + 可选 CephFS(延迟到 P3 之后) |
| GCP Ops Agent autoscaler 指标 | 自有节点池控制器 + Nomad APM 插件 |
| Cloudflare DNS/证书 | 内部 DNS + 内部 CA(或 cert-manager) |
| LaunchDarkly | 默认值 + 自建 flag 表(P2 只做静态配置) |

P0 实验环境最小资源(建议):

- 3x control 节点(4C/8G,Nomad+Consul server,可 VM)
- 2x compute 节点(≥16C/64G,**必须 KVM**,若云上需嵌套虚拟化或裸金属)
- 共享对象存储:MinIO 1 节点
