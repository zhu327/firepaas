# iac：Nomad + Terraform

原则见 [ADR-0001](../docs/adr/0001-nomad-infra-only.md)：Nomad 只编排基础设施；用户 VM 由 control-plane 通过 agent gRPC 创建。

## 作业分层

```text
iac/nomad/
  pools/               # node pool 定义（control / compute）
  hypeman-p0.hcl      # M0 专用：原版 hypeman 数据面验证（必须提供真实 artifact/config）
  agent.hcl           # M1+：带 mTLS 的 agentd system job
  control-plane.hcl   # API/controller service job（count 受 ADR-0007 约束）
  edge.hcl            # edge-proxy service job
```

P0 job 与未来 agentd job **不得共用**：它们的端口、健康检查、状态目录、权限和配置语义不同。

## 节点池（唯一方案）

统一使用**真实 Nomad node pool**，禁止 `-meta` constraint 方案、禁止两者混用：

- `compute`：Firecracker 数据面节点（agentd / hypeman-p0 system job）；
- `control`：api/edge 等基础设施 service job，由 3 台 Nomad server 兼任 client。

`scripts/bootstrap-lab.sh` 按 role 写入各节点 client 的 `node_pool` 配置；集群就绪后创建池（Nomad 2.x）：

```bash
nomad node pool apply iac/nomad/pools/control.hcl
nomad node pool apply iac/nomad/pools/compute.hcl
```

## 服务发现

两种 provider 并存是**有意的设计**：

- **agent（节点发现）**：Nomad native service registration（`provider = "nomad"`），控制面经 Nomad API 查询（ADR-0001，对应 e2b `nodediscovery`）；
- **api/edge 互访**：Consul DNS（`*.service.consul`），相关 job 用 `provider = "consul"` 注册。

注意：Nomad native service **不进 Consul DNS**，两者不可混用同一域名。Consul DNS 需主机 resolver 将 `.consul` 域转发到 127.0.0.1:8600（dnsmasq `server=/consul/127.0.0.1#8600`，注意与 systemd-resolved 冲突）；未配置时可在 job env 中临时使用静态地址。

## 状态存储与镜像仓库（M1 起）

PG/Redis/MinIO/registry 不作为 Nomad job 管理（避免在 Nomad 里再编排一套有状态服务），部署形态：

| 组件 | 实验室 | 生产 |
|---|---|---|
| PostgreSQL | systemd/docker 于 control 节点 | 独立 VM + 备份（M5 演练） |
| Redis | systemd 单实例（AOF） | 单实例；是否 sentinel 依据 M4 验收决定 |
| MinIO | docker 于 control 节点 | 独立节点 |
| registry | docker `registry:2` 于 control 节点 | 独立服务或企业 registry |

地址经 `.env` / Nomad template 注入；本机开发用 `make dev-up`（`iac/dev/docker-compose.yaml`，已含 registry）。

## M0 前置验证

```bash
nomad node pool apply iac/nomad/pools/control.hcl
nomad node pool apply iac/nomad/pools/compute.hcl
nomad fmt -check iac/nomad/hypeman-p0.hcl
nomad job validate iac/nomad/hypeman-p0.hcl
nomad job plan iac/nomad/hypeman-p0.hcl
nomad job run iac/nomad/hypeman-p0.hcl
```

M0 job 必须显式提供：真实 artifact checksum、hypeman 配置、持久 host data dir、KVM/Firecracker 前置检查、HTTP health endpoint 和卸载/重启 smoke test。不得把占位 artifact URL 视作可部署配置。hypeman 配置中**不设置任何 ingress**（内嵌 Caddy/DNS 不启动，避免与 Nomad/Consul 及未来 edge 端口冲突）；基准驱动用 hypeman-cli 或 REST+JWT（`cmd/gen-jwt` 生成 token）。

## agentd 安全要求（M1+）

- 5108 gRPC 仅接受 control-plane mTLS identity；
- 5107 proxy 仅接受 edge mTLS identity，另校验 traffic token；
- 作业通过安全的 secret/template 机制注入证书和短期凭证；
- host firewall 限制来源，agent data dir 持久化，升级先 drain/rebuild；
- 提交前运行 `nomad job validate` 与至少一次真实 alloc smoke test。
