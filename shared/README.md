# shared

跨服务公共库(P1 开始填充):

- `gen/`:protobuf 生成代码(`make proto`)
- `id/`:ID 生成与校验(machine/deployment/execution)
- `storage/`:对象存储客户端(S3/MinIO/Local)
- `catalog/`:Redis routing catalog 读写 + execution_id CAS
- `telemetry/`:OTel 初始化与中间件(参考 hypeman `lib/otel`)
- `auth/`:JWT/API key 校验(参考 hypeman `lib/scopes` + e2b `packages/auth`)

原则:shared 不允许 import control-plane/agent/edge;契约只通过 `protos/` 演进。
