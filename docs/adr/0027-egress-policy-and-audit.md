# ADR-0027：Egress policy、Host/SNI 信任边界与审计

状态：已接受（v1.3）
补充：ADR-0004、ADR-0016。

## 背景

现有 slot 网络可限制 CIDR 和私网访问，但域名策略不能安全地简化为“解析一次域名并放行 IP”。CDN、共享 IP、guest DNS 和 DNS rebinding 都会破坏这种授权。

## 决策

1. egress policy 属于不可变 deployment；修改产生 rollout，并以 generation 全量替换节点规则。
2. 支持 `unrestricted`、`deny_all`、`allowlist`，以及 allowed/denied CIDR、exact domain、单层 wildcard domain。
3. TCP/80 校验 HTTP Host；TCP/443 校验 TLS ClientHello SNI，不解密 TLS。其他 TCP、UDP、无 Host/SNI 和 ECH 只能按 CIDR；allowlist-only 模式默认拒绝。
4. agent 以可信 resolver 解析规范化 Host/SNI（含完整 CNAME 链）；代理忽略 guest 原始目标 IP，并只连接本次可信解析得到、且通过保留段检查的 A/AAAA 集合。若实现必须保留原始目标，则该 IP 必须属于同一解析集合，否则拒绝。
5. 每次新连接重新按受限 TTL 解析；解析失败、集合为空或所有地址连接失败时拒绝，不回退 guest DNS。连接建立后 DNS 变化不迁移既有连接。
6. connect 前拒绝 loopback、link-local、metadata、平台与私网保留段；guest DNS 与 `/etc/hosts` 不构成授权依据。端口必须同时匹配策略声明，Host/SNI 不能授权任意目标端口。
7. 域名通过透明 TCP proxy 执行；nftables 强制相关流量经过代理，不能允许 direct egress 旁路。
8. 每 execution 有 TCP connection limit，生命周期键不能只使用可复用的 slot IP。
7. 审计记录 project/app/machine/execution、policy generation、协议、目标端口、Host/SNI、resolved IP、decision、match type 和 reason；不记录 path、query、header、body 或 credential。
8. 高频连接审计写日志/事件 sink；PG 只保存策略事实和聚合摘要。域名和 IP 不作为 Prometheus 高基数 label。

## 理由

在不引入 mesh 或 TLS MITM 的情况下，Host/SNI inspection 是域名 allowlist 的最小可信实现；CIDR 和应用层名称各自覆盖不同协议边界。

## 后果

- 需要 agent 透明代理、nftables redirect、连接 limiter 和审计 pipeline；
- ECH 或无 SNI 流量在域名 allowlist 下被拒；
- 不承诺 UDP 域名控制、L7 DLP 或 HTTP body 检查；
- 需要 DNS rebinding、无 SNI、非标准端口和连接耗尽测试。
