# ADR-0011:私有化 edge 入口——DNS/VIP 分层与内部 CA 信任链

状态:已接受(2026-08-25)
依据:骨架评审补充——e2b 的 client-proxy 入口依赖云 LB + Cloudflare 证书,
私有化裁剪点中这是唯一没有替代方案的外部依赖;mvp-plan 中"*.<domain> → edge VIP"
与"edge 高可用"此前均无实现主体。

## 决策

### 1. 入口高可用分两步

| 阶段 | 形态 | 说明 |
|---|---|---|
| 实验室 / M1–M3 | DNS 轮询 | `*.<domain>` 与 `<domain>` 指向各 edge 实例静态端口地址(control 池节点 IP,edge job 静态 80/443);接受单 edge 故障时部分解析失败 |
| M4 | keepalived VIP | 2 个 edge 所在 control 节点间 keepalived(或等价二层 VIP)托管唯一入口地址,泛域名只解析 VIP;DNS 轮询保留为私有云无二层环境(BGP 不可用时)的降级路径 |

- edge 部署形态保持 `iac/nomad/edge.hcl`(静态端口 + host 网络),VIP 属节点层
  配置,不进 Nomad job;bootstrap 脚本在 M4 增加 keepalived role。
- 不做 L4 全局 LB 自研;超大入口带宽或 DDoS 场景不属于受信内部 MVP。

### 2. 证书与信任链

- 内部 CA 选型:**step-ca**(轻量、支持 ACME 与 provisioner,M1.3 身份选型一并
  确立,ADR-0006 降级路径共享同一 CA 基础设施)。
- edge TLS 终止沿用 hypeman 的 Caddy 集成(lib/ingress 的证书/ACME 部分),
  ACME directory 指向内部 step-ca,按需签发 `*.<domain>` 泛域名证书;
  不引入 cert-manager(避免 Kubernetes 依赖)。
- **客户端信任链分发是运维前置条件**而非平台功能:组织内部机器镜像/配置管理
  (Ansible/MDM)统一预置根证书;文档列入"内部用户 Onboarding"与 runbook。
- mTLS workload 证书(ADR-0006)与 app 流量证书共用 step-ca,两类
  provisioner 与策略分离。

## 理由

1. VIP/DNS 轮询分层让 M1–M3 不依赖任何新组件即可演示 U1–U3,把 HA 收敛到
   一个周知、可演练的组件(keepalived),而不是提前引入复杂度。
2. step-ca 同时覆盖"内部 app 证书"与"mTLS workload 身份"两个需求,避免
   M1.3 与 M4 各选一套 CA 的基础设施分裂。
3. 信任链预置写进运维前置,防止 MVP 交付时才发现"证书有效但客户端不信任"
   这类边界外失败没有负责人。

## 后果

- `edge/internal/README.md` 补充:Caddy 的 ACME directory 指向 step-ca。
- mvp-plan:M3 工作项增加"DNS 轮询形态入口落地";M4 工作项明确 keepalived
  VIP 交付;风险表保留 sentinel 之外的可用性降级条目。
- bootstrap-lab.sh 在 M4 增加 keepalived role(M1–M3 不需要)。
