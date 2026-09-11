# shared

跨服务公共库：

- `gen/`：protobuf 生成代码（`make proto` 重新生成，已跟踪入库）
- `pkg/id/`：ID 生成与校验（machine/deployment/execution）
- `pkg/durablewrite/`：崩溃安全文件写入（write temp → fsync → rename → fsync dir）
- `pkg/ulanet/`：RFC 4193 ULA（`fd00::/8`）前缀校验
- `pkg/logging/`：api/agentd/edge-proxy 共用的 slog 初始化（`FIREPAAS_LOG_LEVEL`/`FIREPAAS_LOG_FORMAT`）

以下为规划中、尚未实现，不要引用：

- `pkg/storage/`：对象存储客户端（S3/MinIO/Local）
- `pkg/catalog/`：Redis routing catalog 读写 + execution_id CAS
- `pkg/telemetry/`：OTel 初始化与中间件
- `pkg/auth/`：JWT/API key 校验

原则：shared 不允许 import control-plane/agent/edge；契约只通过 `protos/` 演进。
