# ADR-0010:secret 以引用存储与下发,值不落日志/审计/Redis

状态:已接受(2026-08-25)
依据:骨架评审补充——`MachineSpec.env` 是明文 map,M1 起就会有用户把数据库密码
等敏感值写进去,而"secrets 加密"原排在 M5,中间存在裸奔窗口。

## 决策

### 1. 值与引用分离

- 控制面维护 secrets 表(PG,architecture.md §4.2 模型增加):
  `secrets(id, project_id, name, version, value_ciphertext, created_at, ...)`。
- **值在写入 PG 前用信封加密**(MVP:KMS 主密钥或部署注入的对称密钥 + 每值随机
  DEK,实现细节 M4 冻结);PG 中绝不出现明文。
- `MachineSpec` 同时提供两类字段:
  `env`(明文 map,用于非敏感配置)与 `secret_refs`
  (key → `{secret_id, version}`,或按 project 内 name 解析)。

### 2. 注入与防护规则

- 控制面在 `CreateMachine` 时解析 `secret_refs` 为值,经 **mTLS + fencing** 的
  agent 通道以一次性字段 `secret_env` 下发;该字段只存在于请求内存与 VM 启动
  配置中。
- `secret_env` **不进入**:agent 返回的 `Machine`/`MachineSpec`、ListMachines、
  Redis 任何键、审计事件、日志(含 agent 与 OTel 属性)、操作结果持久化。
  proto 字段上标注,代码评审与 lint(字段黑名单)双重把关。
- agent 重启扫描重建 `Machine` 时无法恢复 secret 值,**这是有意设计**:
  observed state 不携带秘密;重建/restart 需要值时由控制面重新下发。
- 更新 secret 触发新 deployment version(不改在运 VM),与滚动发布语义一致
  (ADR-0005)。

### 3. 里程碑切分

- M1:proto 增加 `secret_refs`/`secret_env` 与注释;可暂无实现,但**约定
  `env` 仅用于非敏感值**并写入 API 文档。
- M4:secrets 表 + 信封加密 + CLI `apps env set --secret`;审计记录
  `secret_id/version` 而非值。
- M5:轮换、访问审计明细、主密钥管理硬化(沿用 §11 原排期)。

## 理由

1. "M5 才加密"意味着 M1–M4 所有真实使用都在 PG/日志/Redis 里留明文,事后
   清理与合规成本远高于提前建模。
2. 引用模型与 registry 凭证的"短期 scoped token"(ADR-0006)同一思想:
   秘密只在需要时、经加密通道、以最小生命周期存在。
3. observed state 不含秘密,让 agent 本地状态(崩溃恢复日志,ADR-0003)
   天然无敏感数据,减少一个泄漏面。

## 后果

- proto:`MachineSpec` 增加 `repeated SecretRef secret_refs`、`map<string,string>
  secret_env`(请求向,响应不回填);`env` 注释改为"仅非敏感值"。
- architecture.md §4.2 模型增加 secrets 表;mvp-plan §11 风险表增加降级条目。
- 审计/日志中间件需实现字段黑名单,列入 M1 工程基线或 M4 工作项。
