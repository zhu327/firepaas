# ADR-0037：Image Prewarm、Coverage 与 Pin 的临时 Operator 治理

状态：已接受（v1.4，2026-09-01）
关联：ADR-0018、ADR-0035、ADR-0036。

## 背景

v1.x 尚无 project 到 node pool 的授权模型。向普通 project key 暴露 coverage 或允许选择节点池，会泄漏拓扑并形成未经授权的共享节点资源操作。

## 决策

1. 在 node-pool ACL 有独立 ADR 和数据模型之前，prewarm、coverage、pin/list/unpin 全部要求 root/admin scope；project ID 仍写入 PG 用于配额、审计和未来迁移。
2. HTTP mutation 接受 project-scoped `Idempotency-Key`。相同 key 和相同规范化请求返回原 operation；相同 key 不同请求返回冲突。
3. active prewarm、pin count 和 pinned bytes 在 project 级事务锁内检查并写入。批量 selector 必须全成或全败。
4. node-pool selector 在创建时解析并固化为显式 node selector，避免节点池扩容静默突破 pinned bytes 配额。
5. target 保存 attempt、deadline 和最后错误分类；超出预算后终态 FAILED。prewarm 使用独立有界工作预算，不得长期占用通用运行态 operation 队列。
6. completion event 与 operation 终态在同一事务中提交，并以 operation ID 去重。
7. coverage 返回调用 project 的最新 target observation 与缓存 observation；不得用历史 `bool_or`，不得把 node heartbeat 冒充 digest cache observation。没有 deployment 上下文时只称为基础 eligible。
8. pin 创建与 GC claim 对 `(node,digest)` 使用同一锁域；存在 active delete claim 时 pin 失败并要求重试。pin 成功返回后，不允许旧 claim继续删除。

## 回滚

停止新 prewarm/pin，等待在途 target 达到终态，关闭 delete GC 后回滚二进制。PG 中 operation、target 和 pin 事实保留；旧二进制忽略新增字段。
