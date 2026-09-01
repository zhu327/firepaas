# ADR-0023：Runtime capability discovery 与协议协商

状态：已接受（v1.2，2026-08-31）

Review 修订：能力覆盖必须按证据分层。代码与单元测试支持 capability 投影、
启动硬过滤及 action-time 检查；单节点 smoke 不能证明混合版本多节点调度。
因此混跑升级仍是发布门禁，不得由 capability 并集或单机结果替代。

## 背景

节点在 agent、hypeman、guest agent、hypervisor 和内核版本上可能不同。继续通过版本号、label 或调用失败猜测能力，会让 exec、one-shot secret、snapshot 和未来 GPU 在滚动升级期间误调度。

## 决策

1. `ServiceInfoResponse` 报告 `protocol_version`、`feature_ids[]` 和 `snapshot_compatibility_key`。
2. feature ID 是稳定、小写、带版本后缀的字符串，例如 `guest.exec.v1`；未知 ID 必须忽略，已发布 ID 不改变语义。
3. 能力分为两类：启动正确性能力进入 placement 硬过滤；logs/exec/cp 等按需运维能力在 action-time 对目标 execution 检查。若某版本承诺所有新 machine 都支持运维能力，应以旧节点 drain 的集群升级门禁实现，而不是隐式污染 deployment spec。
4. 控制面只从 deployment 语义推导启动 `required_features`，客户端不得直接声明内部 feature 绕过产品校验。
5. cluster capability API 返回每项能力的 eligible node count，不把节点能力并集伪装成“整个集群支持”。
6. capability 只说明功能可调用，不代替资源准入、snapshot compatibility、授权和运行时健康检查。
7. 缺少必需启动能力时 fail closed 并产生 scheduler event；缺少按需运维能力时返回明确 capability error；不静默降级，除非具体功能 ADR 明确允许。

## 理由

能力协商将功能与版本号解耦，允许新旧 agent 混跑，并为多 hypervisor、snapshot 和 guest 通道建立统一调度入口。

## 后果

- 修改 agent proto、node PG 投影、nodemanager 和 scheduler；
- 需要混合版本契约测试；
- 新能力必须先分配 feature ID，再开放 API；
- label 继续表达运维属性，不再承担 runtime capability 语义。
