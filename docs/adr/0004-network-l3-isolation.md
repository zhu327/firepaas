# ADR-0004:节点间 L3 隔离,节点内地址可复用,流量统一走 edge

状态:已接受(2026-08-25)
依据:可行性分析附录 D.1

## 决策

1. 不做跨节点扁平网络/overlay。VM 地址只在节点内可达;
   所有跨节点流量走 `edge → catalog → agent proxy → slot → VM`。
2. 每个节点内部使用同一套私有地址规划(如 `10.11.0.0/16` slot + `10.12.0.0/16` veth),
   由控制面分配的 node_cidr 参数化,slot idx 由 agent 原子分配。
3. 每 VM 一个 netns slot(veth + tap + NAT + nftables),slot 池化、延迟复用、启动回收。
4. 主机防火墙用 nftables set 保持 O(1) 规则,默认拒 guest 访问宿主机/私网。
5. 网络后端做成 feature flag(`bridge | slot`),与旧 hypeman bridge 实现并行灰度。

## 理由

- 把"集群网络问题"降级为"单机网络问题 + 一个 routing catalog"。
- e2b `pkg/sandbox/network/v2` 的模式已在生产验证;hypeman `lib/network`
  的 TAP/tc/iptables 组件可继续复用。

## 后果

- VM 间没有节点内直连的"私有网络"语义(MVP 明确不支持 service mesh;
  同 app 多副本间通信走 edge 或后续加内部 DNS)。
- 跨节点迁移 VM 时 IP 会变,需由控制面统一分配与更新 catalog。
