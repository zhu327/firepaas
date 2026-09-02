# ADR-0006：root agent 使用 mTLS workload identity 与 RPC 授权

状态：已接受（2026-08-25；后果部分 2026-09 更新）

依据：agent 是 KVM/netns/cgroup root 控制面的安全评审

## 决策

- control-plane→agent、edge→agent proxy 都必须使用 mTLS workload identity；证书由部署环境的 CA/工作负载身份系统签发并可轮换。
- agent proxy 是从 M1 vertical slice 起唯一正式 workload 入口；M1 可通过 bridge adapter 定位 guest，M3 切换为 netns slot。edge 永不持有或使用 slot IP。
- agent gRPC（5108）只允许 control-plane identity 调用；agent proxy（5107）只允许 edge identity 调用，后者还校验 execution-bound traffic token。
- 节点防火墙仅向允许来源开放上述端口；agent 不信任“在内网”这一条件。
- 所有变更 RPC 要求 operation/execution/generation fencing；agent 记录操作结果，拒绝过期调用方或旧 generation。
- registry 凭证采用短期 scoped token 或 agent-side credential provider；禁止长期密码出现在 proto、Redis、审计事件与日志中。
- execution-bound proxy credential 只允许通过 `CreateMachineRequest` 单向下发，不能作为 `MachineSpec` 的一部分回显；agent 仅保存验证材料/摘要，并在 execution 被替换或删除时撤销。

## 后果

- mTLS、身份授权、证书轮换和拒绝测试属于 P1 退出条件，而不是 P4 的增强项。
- P0 原版 hypeman 验证可以在隔离实验网络临时放宽，但不得复制到 agentd 生产 job。
- 降级路径（评审补充）：M1 出口允许静态证书 + 主机端口 ACL；证书轮换与完整授权矩阵最迟 M5 完成。CA 选型（step-ca 或等价轻量方案）在 M1.3 决定。
- 2026-09 生产就绪更新：静态证书 + CN 白名单之上补齐证书热重载（`internal/security/mtls` CertManager，agentd/edge-proxy/agentclient 均接入）与到期观测——各进程导出 `firepaas_tls_cert_not_after_seconds` gauge，配 30d/7d 告警（`iac/observability/prometheus-alerts.yml`）。
- 新记录横向风险：edge 持有 write-scope API key，可经 `GET /v1/machines/{id}/traffic-token` 为**任意** machine 现场铸造 traffic token（execution-bound HMAC 派生）。裁决（2026-09-02）：保持 write scope，风险接受并文档化；edge 本属数据面可信边界，后续以 edge 专用窄 scope token 收口。
- 仍为延期项（用户裁决最小方案）：step-ca/ACME 签发、per-node agentd server 身份、证书吊销（CRL/OCSP）、完整授权矩阵。
