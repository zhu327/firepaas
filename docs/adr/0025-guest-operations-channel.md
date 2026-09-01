# ADR-0025：Guest 运维通道的权限、传输与会话边界

状态：已接受（v1.2，2026-08-31）

Review 修订：当前 hypeman guest 通道尚不提供通用 exec signal，故 v1.2 对 signal
帧明确返回不支持，不得宣称已交付 signal。身份校验同时覆盖 unary 与 streaming
RPC；会话并发、总字节、空闲和总时长均有界。单节点 smoke 覆盖基础
logs/exec/cp，不替代断连、背压、资源泄漏和多节点故障矩阵。

## 背景

`Exec` 和 `StreamLogs` 已在 proto 中占位但未实现。hypeman guest agent 和 E2B envd 都证明了进程/文件 API 的价值；firepaas 需要保留自身控制面授权、execution fencing 和审计边界。

## 决策

1. 路径固定为 `client/CLI → control-plane → mTLS agent → vsock guest agent`；客户端不得直连 agent 或 guest。
2. v1.2 稳定能力为 logs、单会话 exec 和单文件 cp；不移植完整 envd process/filesystem API。
3. 每个请求绑定 project、machine、execution；旧 execution 立即拒绝。建立后的 stream 在 execution 替换时终止。
4. exec 支持 argv、cwd、非敏感 env、TTY、resize、stdin、signal 和 exit code。客户端断开即终止会话，不支持 reattach 或输出续传。
5. cp 只允许普通文件，默认不跟随 symlink；guest agent 做路径 clean、根目录约束和最终文件类型检查。
6. project/session 并发、frame、总字节、速率、空闲时间和总时长均有限制。
7. 审计记录 caller、目标 identity、命令/路径摘要、字节、耗时和退出结果；不记录内容、stdin/stdout 或环境变量。
8. `read` key 只能读历史元数据；实时 logs/exec/cp 使用独立 `debug`（或等价 write）scope。
9. 控制面只流式代理，不归档文件正文；历史日志存储另立设计。

## 理由

复用节点内 guest primitive，同时保持 API 是唯一租户授权入口，避免扩大 agent 暴露面和形成第二套身份体系。

## 后果

- 修改 proto、agent server、API streaming handler 和 fpctl；
- 必须测试 backpressure、取消、FD/goroutine/vsock 泄漏；
- 用户主动通过 exec 读取自身 secret 属授权行为，平台只保证不自动记录；
- process reattach、filesystem watch、目录保真复制和 guest-agent live upgrade 后置。
