# ADR-0016:网络方向裁决——维持 L3 隔离与 edge 路由,不引入 6PN/WireGuard mesh

状态:已接受(2026-08-28)
依据:对 `docs/fp.md`(独立规划,提议 fly.io 风格 WireGuard full mesh + per-org ULA
IPv6(6PN)+ L3 直达)与 firepaas 现有架构(ADR-0004/ADR-0005)及 M3–M5 已验收实现的
对照评审。本 ADR 记录"已评审并拒绝"的备选方案,防止重复评审。

## 决策

1. v1.1 及后续演进**维持 ADR-0004**:节点间 L3 隔离、节点内地址可复用、所有跨节点
   流量统一走 `edge → Redis catalog → agent proxy → slot → VM`。不引入 WireGuard
   mesh、per-org ULA IPv6(6PN)、三级 IPAM(org /48 → node /64 → instance /128)。
2. 连带不引入的依赖项:`.internal` 内部权威 DNS(fp-dns)、用户 WireGuard 接入、
   hypeman 侧 bridge IPv6(H1)与 TAP MTU(H2)上游补丁、v6 反欺骗/租户隔离规则、
   wg peer 编排组件。
3. 演进门不关闭:若未来出现**真实的东西向互访需求**(同 project 内 app 互访私有
   db/cache、实例间低延迟 RPC),按以下顺序评估,每步独立报 ADR:
   1. 节点内 slot 互通 + 内部服务发现(最便宜,不引入 overlay);
   2. 跨节点受控互通(仍经 proxy,按 service 声明放行);
   3. mesh/overlay(6PN 或等价物)——只有前两步证明不够时才进入,且需重新评估
      "edge 永不读取 slot IP"的契约让渡。

## 理由

1. **无消费场景**:MVP/v1.1 面向无状态、受信内部工作负载(architecture §1、
   mvp-plan §1.3);东西向 L3 直达的全部价值是免 proxy 的实例间互访,当前没有
   该需求,`app 连自家 db` 用 edge hostname 或外部服务发现即可满足。
2. **沉没成本与契约冲突**:slot 数据面已通过 M3/M5 验收(1000 次分配/释放无泄漏、
   隔离断言、kill -9 回收、72h soak 排练)。6PN 要求重做节点网络面(wg/MTU/
   nftables v6/反欺骗),并让 edge/catalog 感知实例地址——直接违反已冻结的
   "Redis/edge 不感知 slot IP,只使用 node_proxy_endpoint"契约(architecture §4.3)。
3. **成本结构最差**:H1 是 hypeman 唯一硬 fork 风险点(fp.md 自认"上游不接受则
   fork 维护");wg full mesh、peer 编排、段回收冷却是持续运维负担;收益(少一跳
   proxy)在内部规模下不是瓶颈。当前实测瓶颈是未缓存镜像冷启动(p95 7.6s),由
   v1.1-B(镜像亲和与预取)解决。
4. **e2b 与 fly.io 场景差异**:e2b 模型(短生命周期沙箱、无东西向)正是 firepaas
   已验证路线的来源;fly.io 的 6PN 服务于长驻服务的东西向互访,是该场景下的正确
   设计,但不是本平台当前形态的必要能力。

## 后果

- `docs/fp.md` 中依赖 mesh 的能力(6PN、fp-dns、用户 WG、IPAM)全部不采纳;
  其余可吸收项已拆入 v1.1 计划(auto-standby、镜像亲和、edge 语义、evacuate、
  多端口)。
- "跨节点私网/service mesh"继续留在移出范围清单;用户侧东西向需求出现时按
  决策 3 的阶梯评估。
- 本决策被推翻的唯一路径:未来 ADR 明确论证东西向需求无法用阶梯 1/2 满足,
  并接受网络面重构与契约变更成本。
