# iac：Nomad + Terraform

原则见 [ADR-0001](../docs/adr/0001-nomad-infra-only.md)：Nomad 只编排基础设施；用户 VM 由 control-plane 通过 agent gRPC 创建。

## 作业和发现契约

| 作业 | Nomad job / service | 发现与 health |
|---|---|---|
| agentd | `firepaas-agentd` / `firepaas-agentd`, `firepaas-agentd-proxy` | Nomad native；均为 TCP 5108/5107 |
| API | `firepaas-api` / `firepaas-api` | Consul；HTTP `GET /v1/health` on 8080 |
| edge | `firepaas-edge` / `firepaas-edge` | Nomad native；HTTP `GET /healthz` on 80 |

控制面的节点发现固定查询 `firepaas-agentd` job，因此生产 `agent.hcl` 与实验室
`agentd-single.hcl` 使用同一个 job/service 名。agent 的 native service 不进入 Consul
DNS；仅 API 经 Consul DNS 注册。edge 的 `FIREPAAS_API_ADDR` 必须是可实际解析的 URL，
例如 `http://firepaas-api.service.consul:8080`（需配置 `.consul` DNS 转发）。

## 生产发布：拒绝占位值与浮动 tag

`agent.hcl`、`control-plane.hcl`、`edge.hcl` 的 artifact、镜像和依赖地址都是**无默认值
required variable**。这使 `nomad job validate/plan` 在漏传发布输入时失败，而非部署
`latest`、`REPLACE-ME` 或示例地址。镜像变量必须是 allowlisted registry 的 digest 引用
（`registry.example/firepaas/api@sha256:<64-hex>`）；agent artifact 必须是版本化 HTTPS URL
和对应 64-hex SHA-256。不要在 HCL、`-var` 历史或 Nomad job spec 写入密码；敏感连接串、API token、secret/traffic signing keys 与 mTLS cert/key/CA 路径通过受控变量
文件或 Vault template 注入。生产 HCL 为这些输入声明了 required sensitive variables；漏传会使
`nomad job validate/plan` 失败。agentd 也会在缺少 TLS 时拒绝启动，除非明确设置仅限本地的
`FIREPAAS_ALLOW_INSECURE_DEV=true`。

示例（值只作格式说明，不能直接投产）：

```bash
nomad job validate \
  -var='agentd_artifact_url=https://artifacts.example/firepaas/agentd-1.2.3-linux-amd64' \
  -var='agentd_artifact_sha256=<64-hex-sha256>' iac/nomad/agent.hcl
nomad job plan \
  -var='api_image=registry.example/firepaas/api@sha256:<64-hex>' \
  -var='postgres_url=<vault-rendered-url>' -var='api_token=<vault-rendered-token>' \
  -var='secrets_master_key=<vault-rendered-key>' -var='traffic_token_key=<vault-rendered-key>' \
  -var='agent_tls_cert=/secure/control-plane.crt' -var='agent_tls_key=/secure/control-plane.key' \
  -var='agent_tls_ca=/secure/ca.crt' -var='redis_addr=redis.internal:6379' \
  -var='nomad_addr=https://nomad.internal:4646' iac/nomad/control-plane.hcl
nomad job plan \
  -var='edge_image=registry.example/firepaas/edge@sha256:<64-hex>' \
  -var='api_addr=http://firepaas-api.service.consul:8080' -var='redis_addr=redis.internal:6379' \
  -var='api_token=<vault-rendered-token>' -var='edge_tls_cert=/secure/edge.crt' \
  -var='edge_tls_key=/secure/edge.key' -var='edge_tls_ca=/secure/ca.crt' iac/nomad/edge.hcl
```

Before `run`, use `nomad job plan` with the exact release variables and verify no `:latest`,
`REPLACE-ME`, or placeholder hostname appears in the rendered plan. The lab-only
`agentd-single.hcl` remains separate and must never be promoted as production configuration.

## 节点池

统一使用真实 Nomad node pool，禁止 `-meta` constraint 方案和混用：

- `compute`：Firecracker 数据面节点（agentd system job）；
- `control`：api/edge 基础设施 service job。

```bash
nomad node pool apply iac/nomad/pools/control.hcl
nomad node pool apply iac/nomad/pools/compute.hcl
```

PG/Redis/MinIO/registry 不作为 Nomad job 管理；生产部署于独立受管服务，地址由安全
变量/template 注入。提交前运行 `nomad fmt -check iac/nomad/*.hcl`、带完整变量的
`nomad job validate`，并执行至少一次真实 alloc smoke test。
