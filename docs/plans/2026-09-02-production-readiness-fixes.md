# 生产就绪缺口修复计划（P0–P3）

> 来源：2026-09-02 全仓架构评审（控制面 / agent / edge·安全 / 运维四路审查）。
> 用户裁决：①改动只落工作树，不由代理提交；②证书仅做**热重载 + 到期指标/告警**，step-ca 与 per-node 身份留作延期项；③P2 只做 CI/镜像/告警链路，**跳过 Terraform**；④edge 保持 write-scope 凭证，横向风险仅文档化。

## Goal

消除评审列出的 P0（正确性/安全）、P1（可用性/韧性）、P2（交付链路，按裁决裁剪）、P3（观测/测试/卫生）缺口，使仓库达到"可进入 GA observation scorecard 演练"的状态。

## Architecture

不改变任何冻结架构不变量（AGENTS.md 9 条 + docs/architecture.md）。所有修复是向已冻结契约的对齐（fail-closed、ledger 崩溃恢复、route 语义），不引入新抽象层；新增能力（证书热重载、边缘 LRU）落在既有 seam 内（`internal/security/mtls`、`internal/edge`）。

## Non-Goals（已裁决的延期项）

- Terraform（保持 `iac/terraform/README.md` 现状）
- step-ca/ACME 集成、per-node agentd 身份（仅热重载+告警止血）
- 分布式 tracing（无 collector 基础设施决策；记录为延期项）
- edge 专用窄 scope token（保持 write scope，风险写入 ADR-0006 后果）
- Redis sentinel（架构 §4.3 已接受单实例 + serve-stale）

## Validation

```bash
make check          # build + vet + test + tidy-check（GOWORK=off）
make sim            # 调度器归一化改动后必跑
go test ./internal/agent/... ./internal/edge/... ./internal/contracts/...
bash -n scripts/lab/*.sh
```

实验室 e2e（e2e-m2/m3/m4）依赖实验室主机与 root 权限，未获授权不执行；相应改动在报告中声明未验证面。

## 跨 slice 契约（先于派发冻结）

### C-1 mtls 证书热重载（Slice B 实现，Wave 2 消费）

```go
// internal/security/mtls
// CertManager 管理一张证书的热重载与到期观测。
func NewCertManager(certFile, keyFile string, reloadEvery time.Duration,
    logger *slog.Logger, notAfterHook func(expiry time.Time)) (*CertManager, error)
```

- `GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)`（server 侧）
- `GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error)`（client 侧）
- `NotAfter() time.Time`；内部 RWMutex 保护当前证书；重读失败保留旧证书并 log warn；`Close()` 停止后台 goroutine。
- 各进程把 `notAfterHook` 接到自己的 metrics 体系，导出 gauge `firepaas_tls_cert_not_after_seconds`（label 至少含 `file`）。

### C-2 边缘/dev 模式 env（Slice B 定义，Slice D 的脚本消费）

`FIREPAAS_EDGE_ALLOW_INSECURE_DEV=true`：edge→agent mTLS 材料缺失时唯一允许明文降级的显式开关（对齐 agentd 的 `FIREPAAS_ALLOW_INSECURE_DEV`）。

### C-3 调度超售比（Slice C2 内部）

`reservations` Lua 中硬编码的 `* 4` 改为脚本入参，值来自 `placement` 持有的 `BestOfKConfig.R`（不改 cmd/api 装配签名；若必须改则回报协调者）。

## 依赖表

| Task | Type | Blocked by | Parallelizable with |
|---|---|---|---|
| A agent 修复 | AFK | — | B, C1, C2, D |
| B edge + mtls 修复 | AFK | — | A, C1, C2, D |
| C1 api/db/leader/store/rollout 修复 | AFK | — | A, B, C2, D |
| C2 scheduler/reservations/placement/controller 修复 | AFK | — | A, B, C1, D |
| D CI/镜像/告警/备份/脚本/hcl | AFK | — | A, B, C1, C2 |
| Wave 2 证书热重载接入 agentd/agentclient + 集成验证 | AFK | B(契约 C-1) | — |
| Wave 3 零测试包补测 + 文档回写 | AFK | Wave 1 | — |
| Wave 4 独立 code-reviewer | HITL 输出 | Wave 2/3 | — |

写集隔离：
- A：`internal/agent/**`、`cmd/agentd/**`
- B：`internal/edge/**`、`cmd/edge-proxy/**`、`internal/security/mtls/**`
- C1：`cmd/api/**`、`internal/controlplane/{db,leader,apikeys,store}/**`、`internal/controlplane/controller/apps.go`、`internal/controlplane/controller/prewarm` 不动、db migrations 新文件
- C2：`internal/scheduler/**`、`internal/controlplane/{reservations,placement}/**`、`internal/controlplane/controller/controller.go`
- D：`.github/**`、Dockerfile*、`.golangci.yml`、`Makefile`（仅追加）、`iac/observability/**`、`iac/nomad/control-plane.hcl`、`scripts/lab/pg-backup.sh`、`scripts/lab/migration-rehearsal.sh`(新)、`docs/runbook-*.md`

**共同规则**：工作树含用户未提交修改（ci.yml、cmd/agentd/main.go、cmd/agentctl/main.go、e2e 脚本、architecture.md、部分 ADR、nodemanager 等被用户改过）。一律在用户当前内容基础上追加修改，**不得回退任何与本任务无关的现有内容**；一切改动留在工作树，不执行 git commit。

---

### Task A：agent 侧正确性与清理【高风险】

Goal：修复 create 崩溃窗口永久失败（P0#1）；补 creds/slots fsync、运行期 slot 周期 reconcile、回收错误处理、cgroup CPU 准入、杂项卫生（P1#15、P3）。
Acceptance criteria：
1. `RunCreateMachine` 升级为 durable claim 模型（参照同文件 `runClaimed`）：Effect 前持久化 claim（含 request_hash）；重试同 operation_id 命中未完成 claim 时经 hypeman inventory（按 instance Name=machine_id 查实例）恢复——存在则补 Complete 返回成功，不存在则清 claim 重跑 Effect；不会再出现"重放撞 hypeman 同名拒绝 → 永久失败"。
2. replay 路径补写 credential（creds.json 丢失后重放 create 恢复 5107 可用）。
3. `state/creds.go`、`network/slot/slot.go` 落盘对齐 ledger 全链路 fsync（temp+fsync+rename+fsync(dir)，0600）。
4. agentd 启动后周期性（env `FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL`，默认 5m，<=0 禁用）执行 slot Reconcile。
5. create 回收路径的 `DeleteInstance` 错误不再吞掉（log + metric/log 警告级）；`PruneMachineExcept` 错误处理同理。
6. `Admit` CPU 准入读 cgroup v2 CPU quota（对齐现有 `readCgroupMemMax` 模式）。
7. agent proxy 502 收敛（P0#4 agent 半）：`internal/agent/proxy/proxy.go:86-88` transport 错误不再把含 guest IP:port 的拨号错误写进响应 body，统一固定文案（保留 retryable 头语义与内部 log 细节）。
8. 卫生：`volumes.go` init panic 注释语义化；`slot.go` logf 支持级别（非全 Error）；`envInt/envFloat` 非法值 log warn；修 `envFloat` 头注释笔误。
Files：Modify `internal/agent/mutation/protocol.go`、`internal/agent/state/{ledger,creds}.go`、`internal/agent/network/slot/slot.go`、`internal/agent/machine/adapter.go`、`internal/agent/info/info.go`、`internal/agent/machine/volumes.go`、`cmd/agentd/main.go`；Add 对应 `*_test.go` 用例。
Tests：create 崩溃窗口 replay 恢复（claim 有/无实例两分支）、credential 重放补写、fsync 调用路径、cgroup CPU 解析边界。
Validation：`go test ./internal/agent/... ./cmd/agentd/...`
Risk controls：ledger 格式向后兼容（旧 record 可加载）；fence 高水位语义不变（Claim 不得越过 generation fence）；改动仅限本 slice 文件；失败回滚 = git restore 本 slice 文件（不动用户既有改动）。

### Task B：edge + mtls 修复

Goal：mTLS fail-closed（P0#2）、错误信息收敛（P0#4 边缘半）、证书热重载契约 C-1（P0#5）、缓存内存上限（P1#16）、P3 观测修复与卫生。
Acceptance criteria：
1. `loadAgentTLS` 缺材料时返回错误；仅 `FIREPAAS_EDGE_ALLOW_INSECURE_DEV=true`（契约 C-2）允许明文，启动日志显式告警。
2. `handleProxyError` 兜底分支不再回传 `err.Error()`，统一固定 502 文案（细节进 log）；`X-Firepaas-Machine-ID` 只在 token 获取成功后写入。
3. `internal/security/mtls` 实现契约 C-1（CertManager）+ 测试；edge 的 server cert 与 agent client cert 接入 CertManager 并导出 `firepaas_tls_cert_not_after_seconds`。
4. RouteCache（正/负缓存）与 RateLimiter buckets 加容量上限（默认 10000，env 可调），LRU 或惰性过期清理；补增长有界性测试。
5. 修死指标 `firepaas_edge_token_stale_serves_total`（TokenClient 回传 stale 标记并打点）；补 route lookup / token fetch / upstream RTT 的延迟 histogram；edge 新增结构化请求日志（slog，host/结果/耗时，不含凭证）。
6. 修 `pinHits` 重试双计数、`proxiedReqs` 只在有 cred 时计数的口径问题；TokenClient 空 token+nil err 防御分支返回显式错误。
7. edge 全部 `http.Server` 设 `ReadHeaderTimeout`（WS/SSE 语义不破坏：只设 header 超时与 IdleTimeout，WriteTimeout 不设或保守大值+注释）。
8. 卫生：`edge.go` 孤注释修正、`fmt.Errorf`→`errors.New`、test-only 的 `acquire/load` 移入测试文件或内联、`defaultTokenStale` 声明位置。
Files：Modify `internal/edge/{edge,handler}.go(*_test)`、`cmd/edge-proxy/main.go`、`internal/security/mtls/mtls.go`
Tests：fail-closed 启动分支、502 body 不含内部地址、LRU 上限、CertManager 重载/到期 hook、直方图打点。
Validation：`go test ./internal/edge/... ./internal/security/... ./cmd/edge-proxy/...`

### Task C1：API/DB/leader/store/rollout 修复

Goal：客户端不可操纵 fence 字段（P0#3）、错误映射收敛（P0#4 控制面半）、HTTP 超时+recover（P1#8/#11）、PG 池治理（P1#9）、apikey 缓存与 503 语义（P1#12）、rollout 时间解析（P1#13）。
Acceptance criteria：
1. createMachine（M2 legacy 端点）拒绝客户端提交的 `machine_id`/`execution_id`/`generation`（400，服务端正生成）；检查 `scripts/lab/e2e-m2.sh` 是否依赖传参，若依赖则在当前内容基础上更新脚本（该脚本含用户改动，不得回退）。
2. 统一错误映射 helper：PG/内部错误 log 全文、500 body 为固定文案不吐原文；`setNodeDraining`、`getOperation` 等仅在确证 not-found 时 404，其余错误 500/503；扫描 cmd/api 全部 handler 的裸 `err.Error()` 回吐并收敛。
3. API `http.Server` 设 `ReadHeaderTimeout`（留 streaming 端点语义注释）；HTTP 中间件 panic recover（log + 500）。
4. `db` 包：连接池显式配置（MaxConns/MinConns/MaxConnLifetime/HealthCheck，env 可调带默认）；leader 选主改为**独立专用连接**（不经业务池，池耗尽不卡选主）；`leader.go` 解锁加超时。
5. apikey 认证：hash→identity 短 TTL 缓存（默认 60s，写入路径失效）；PG 查询失败返回 503（带 metric/日志），不再降级成 401。
6. rollout 时间列：核查迁移中列类型；若为 `timestamptz` 直接改 store 扫描为 `time.Time`（删除 `parsePGTime` 的 text 解析路径）；若为 text 则新增顺序 migration 0032 转 `timestamptz`（USING 转换，兼容回填），并修复 S3 超时静默失效（解析失败必须告警而非跳过）。
7. controller goroutine 与 leader onLeader 的 panic recover（log 后按既有重启语义处理，Nomad/systemd 接管）。
Files：Modify `cmd/api/**`、`internal/controlplane/{db,leader,apikeys,store}/**`、`internal/controlplane/controller/apps.go`；必要时 Add `internal/controlplane/db/migrations/0032_*.sql`。
Tests：fence 字段 400、错误映射（404/500/503 分界）、apikey 缓存命中与 503、timestamptz 扫描、recover 中间件；PG 依赖测试沿用 `FIREPAAS_TEST_POSTGRES` gate 模式。
Validation：`go test ./cmd/api/... ./internal/controlplane/...`
Risk controls：migration 只新增不改写；fence 字段拒绝是对 legacy 端点的行为变化，errbody 明示；失败回滚 = git restore 本 slice 文件。

### Task C2：scheduler/reservations/placement/controller 修复

Goal：超售比归一化（P1#14）、controller 主循环串扰（P1#10）。
Acceptance criteria：
1. `scheduler.New` 归一化 `DiskR`（默认 1.0）与 `WeightImage` 默认填充条件（部分配置不丢镜像亲和）；补默认值单元测试（含 R/MemR/DiskR/权重全零与半配置输入）。
2. `reservations` Lua 的 `* 4` 改为脚本入参，值来自 placement 持有的 `BestOfKConfig.R`（契约 C-3）；超售比一致性测试：R=2 时调度与预约同时按 R=2 判定。
3. `controller.go` syncObserved：各节点 `List` 改为有界并发（每节点保留独立 10s 超时），worst case 从 10N→~10s；合并结果后决策逻辑保持单线程（单写者不变量不动）；补并发 fetch 的单测（fake client，失联节点不阻断健康节点）。
Files：Modify `internal/scheduler/**`、`internal/controlplane/{reservations,placement}/**`、`internal/controlplane/controller/controller.go`。
Validation：`go test ./internal/scheduler/... ./internal/controlplane/... && make sim`（100k 次仿真必须通过）

### Task D：CI/镜像/告警/备份/脚本/hcl

Goal：CI 门禁（P2#17/#18/#24）、镜像构建链（P2#19）、告警通知面（P2#21）、备份加固（P2#22）、升级 runbook 与 migration 演练（P2#22）、API count 解锁（P1#7）。
Acceptance criteria：
1. `ci.yml`（用户已改，追加不回退）：加 `services: postgres:16/redis:7` 并设置 `FIREPAAS_TEST_POSTGRES/REDIS` 使 8 个 gate 包真实运行；`go test` 加 `-race`（分 job 控制时长）；新增 `govulncheck` step、覆盖率收集（不设硬阈值，出报告）。
2. 新增 `.golangci.yml`（基线：errcheck/govet/staticcheck/unused/ineffassign/misspell，节奏保守争取一次性全绿）+ CI lint job；本机装 golangci-lint 实测，装不上则保守配置并显式报告未验证。
3. `.github/dependabot.yml`：gomod + github-actions 周更。
4. 核查 `iac/nomad/*.hcl` 使用的 driver：docker driver 的组件（api/edge 等）补 `Dockerfile`（api、edge-proxy；agentd 若 raw_exec 则不加并注释原因）；CI 新增 image job：build+digest push（registry 变量化）、tag 推送时 cosign keyless 签名 + syft SBOM（pinned actions）。
5. `iac/observability/`：新增 `alertmanager.yml`（route 树 + web 通知 receiver 示例）与 `prometheus` 告警规则增量：SLO burn-rate 多窗口规则（数据来源 `docs/slo-spec.yaml` 各指标）、证书到期规则（`firepaas_tls_cert_not_after_seconds - time() < 30d`）、edge stale/token 指标告警补全；`iac/nomad/edge.hcl` 暴露 `FIREPAAS_EDGE_METRICS_PORT` 并将 prometheus edge job 指向 Nomad 端口。
6. `scripts/lab/pg-backup.sh`：可选 gpg 对称加密（env 开启）与可选外送 hook（`FIREPAAS_BACKUP_UPLOAD_CMD` 模式，同 dr-rehearsal 风格）；cron/systemd timer 调度写入 `docs/runbook-operations.md`；新增 `scripts/lab/migration-rehearsal.sh`（scratch 库顺序重演全部迁移 + 幂等 + 回滚注释）。
7. 新增 `docs/runbook-upgrade-control-plane.md`（API/edge canary 升级 + 回滚步骤），`runbook-operations.md` 补回滚章节。
8. `iac/nomad/control-plane.hcl`：把 `api_count==1` 放宽为 `1 <= count <= 2`，注释保留"多写者 HA 以 runbook-control-plane-quorum / ha-validation 演练证据为前置"的纪律声明。
9. 若 Slice B 落地契约 C-2：edge 启动脚本（lab 中无明文降级使用者）不改动；仅在相关 runbook 记录该开关。
Validation：`bash -n scripts/lab/*.sh`；`nomad fmt -check iac/nomad` 与 `nomad job validate`（本机无 nomad 则报验证缺口）；Dockerfile 本机 `docker build` 可行则实测，否则静态审查并报告。
Risk controls：不执行 nomad apply/destroy；CI 改动先行 `bash -n`/yaml 语法检查。

---

## Wave 2（Wave 1 完成后串行）

1. 用契约 C-1 把证书热重载接入 `cmd/agentd`（server + proxy 两侧）与 `internal/controlplane/agentclient`（client cert）；各自导出 `firepaas_tls_cert_not_after_seconds`。
2. 集成验证：`make check`、`make sim`、目标包测试、`bash -n scripts/lab/*.sh`；修复跨 slice 集成失败（最窄 owner 原则）。
3. 申报未验证面（golangci-lint 本机缺失、nomad validate、实验室 e2e）。

## Wave 3（与 Wave 2 串行）

1. 零测试包补测：`internal/controlplane/traffic`（签发/校验/过期/execution 绑定）、`internal/controlplane/agentclient`（fake gRPC server 往返 + 错误映射）、`cmd/agentctl`（flag 解析/基本路径，基于用户当前 main.go）；`internal/agent/runtime` 评估后补最小装配测试或记录理由。
2. 文档回写（全部在用户现有改动基础上追加）：
   - `docs/architecture.md`：§6 "mTLS workload identity" 注明当前形态（静态证书+CN 白名单+热重载，轮换/矩阵见 ADR-0006 遗留）与 §4.2 replica 唯一约束实际形态（generation 三元组）。
   - ADR-0006 后果更新：热重载+到期告警已交付、edge write-scope token 横向风险记录、step-ca/per-node 身份/吊销仍为延期项。
   - `internal/agent/README.md`：hypeman 消费方式更新为 fork tag 远程 module（对齐 AGENTS.md），README.md 修"生成代码不提交"与 CI 实际规则的冲突。
   - 延期项清单写入本计划文末（见下）。
3. 运行 `make check` 终验。

## Wave 4

独立 `code-reviewer` 审查整个 changeset（含高风险协议/安全改动，一次性审查）；按审查结论回流修复，终版报告。

## 延期项登记（裁决结果）

| 项 | 理由 | 去向 |
|---|---|---|
| step-ca/ACME、per-node agentd 身份、吊销机制 | 用户裁决最小方案 | 后续版本计划 |
| Terraform | 用户裁决跳过 | iac/terraform/README 维持现状 |
| 分布式 tracing | 需 collector 基础设施决策，不宜顺带引入 | 本文件登记 |
| edge 专属窄 scope token | 用户裁决保持 write | ADR-0006 风险记录 |
| Redis sentinel | M4 验收结论不引入 | architecture.md §4.3 已载 |

---

## 执行结果（2026-09，Waves 1–2 已落地工作树，未提交）

### Wave 1（代码，slice A/B/C1/C2/D 并行）

- **A（agent）**：create 崩溃窗口升级为 durable-claim 恢复（hypeman inventory 对账后补 Complete 或重跑）；replay 补写 credential；creds/slot 落盘全链路 fsync；`FIREPAAS_AGENT_SLOT_RECONCILE_INTERVAL` 周期 slot reconcile；回收/Prune 错误不再吞掉；`Admit` CPU 准入读 cgroup v2 quota；agent proxy 502 收敛为固定文案。
- **B（edge + mtls）**：edge→agent mTLS 材料缺失 fail-closed（唯一明文降级开关 `FIREPAAS_EDGE_ALLOW_INSECURE_DEV`，契约 C-2）；502 不再回传内部错误原文；`internal/security/mtls` CertManager（契约 C-1）落地并接入 edge server 与 agent client cert；RouteCache/RateLimiter 容量上限；死指标与统计口径修复、延迟 histogram、结构化请求日志；`ReadHeaderTimeout`。
- **C1（API/DB/leader/store/rollout）**：M2 legacy create 拒绝客户端提交的 fence 字段（400）；错误映射统一收敛（500 固定文案、404 仅确证 not-found）；`ReadHeaderTimeout` + panic recover；PG 连接池治理与 leader 专用连接；apikey hash→identity 短 TTL 缓存、PG 失败返 503；rollout 时间列 timestamptz 直接扫描（删除 text 解析路径）。
- **C2（scheduler/reservations/placement/controller）**：`DiskR` 与 `WeightImage` 归一化；预约超售比入参化（`AcquireR`，契约 C-3，与 `BestOfKConfig.R` 同源）；`syncObserved` 节点 fetch 改有界并发（决策仍单写者）。
- **D（CI/镜像/告警/备份）**：ci.yml 接入 services: postgres/redis 使 8 个 gate 包真实运行、独立 `-race` job、govulncheck step、覆盖率报告；`.golangci.yml` + lint job + dependabot；`Dockerfile.api`/`Dockerfile.edge-proxy` 与 image job（digest push、tag 推送时 cosign keyless + syft SBOM）；`alertmanager.yml` 与告警规则增量（SLO burn-rate、证书 30d/7d、edge stale/token）；pg-backup 可选 gpg 加密与外送 hook；新增 `scripts/lab/migration-rehearsal.sh`、`docs/runbook-upgrade-control-plane.md`；control-plane `count` 放宽为 1–2。

### Wave 2（接线与集成）

- 契约 C-1 接入 `cmd/agentd`（server + proxy 两侧）与 `internal/controlplane/agentclient`（client cert），各进程导出 `firepaas_tls_cert_not_after_seconds`。
- 顺带修复：egress 测试 DNS 桩的 miekg/dns `PacketConn` 数据竞争（全树 `-race` 实测暴露）；`scheduler.Options.ImageAffinityDisabled` 显式关闭入口（WeightImage=0 不再被默认值覆盖）；placement 测试改用独立 Redis 逻辑 DB（DB 7），与 reservations 测试在 DB0 的 `resv:*` 键空间隔离。

### 验证证据

- `make check`（build + vet + test + tidy-check，`GOWORK=off`）：PASS。
- 全树 `go test -race ./...`：PASS。
- PG/Redis gate 测试（`FIREPAAS_TEST_POSTGRES`/`FIREPAAS_TEST_REDIS`）：PASS。
- `make sim`（100,000 次调度仿真）：PASS。
- Nomad：`nomad fmt -check iac/nomad` 与 `nomad job validate`（`scripts/lab/validate-nomad.sh`）：PASS。
- `scripts/lab/migration-rehearsal.sh`（scratch 库顺序重演全部迁移 + 幂等校验）：PASS。

### 残留与已知限制

- 实验室 e2e（e2e-m2/m3/m4 等）未运行：需主机 root 权限，未获授权。
- golang 1.25.4 工具链下 govulncheck 有发现项：需 Go 工具链/依赖升级解决，已报告未修复（toolchain 升级超出本计划范围）。
- cosign keyless 签名、syft SBOM 与 registry digest push 仅写入 CI job，未在本机实测。
- promtool/amtool 本机不可用，告警规则未经工具级 lint。
- volume 预约仍走 `Acquire` 兼容默认 R=4（vcpu=0，超售比不参与判定，当前无害）。
- edge 保持 write-scope traffic-token 凭证：横向风险已记录（ADR-0006 后果更新）。
- 分布式 tracing 维持延期裁决（无 collector 基础设施决策）。
- `internal/controlplane/store/prewarm.go` 存在既有 gofmt 偏差，与本计划无关，未触碰。

### Wave 4（独立 code-review-expert 审查及回流修复）

审查结论：除一条 P1 外全部验收准则成立，无架构不变量违反。回流修复：

- **P1 错误映射残留**：`putProjectQuota`（governance.go）、`prewarmImage`/`createImagePin` 的 replay/默认分支、`getSecretMeta`、6 处 volume/dataset enqueue 端点——此前会把 PG 故障以 4xx+driver 原文回吐。已改为哨兵分支（域冲突 → 固定文案 4xx，其余 → `writeInternalErr`/`writeVolumeEnqueueErr`）；`store.ErrSecretNotFound` 导出供 errors.Is 使用。新增 closed-pool 回归测试（`TestPutProjectQuotaPGFailureIs5xx`、`TestGetSecretMetaPGFailureIs5xx`）。
- **P2 fsync 序列三副本去重**：收敛为 `shared/pkg/durablewrite.WriteFileAtomic`，`state.writeFileDurable`/`ledger.persistLocked`/`slot.persistLocked` 均委托。
- **P3 清理**：删除误落的 `cmd/api/api` 36MB 构建产物并加入 `.gitignore`；`envFloat` 头注释修正；`FIREPAAS_API_KEY_CACHE_TTL=0` 可显式禁用缓存（envDur 不再吞非正值语义，且非法值 log warn）；agentctl 测试 stdout 经 `t.Cleanup` 恢复；`info.go` 亚核 cgroup 配额收敛为 1 vCPU 的注释补全（保留行为，注明多核假设）。

补充记录（审查中确认但非本次改动引入）：

- `internal/agent/runtime` 无测试的理由：包为 hypeman `lib/*` 纯装配 glue，唯一逻辑分支需真实 hypeman manager/全局 hypervisor 状态后方可执行，测试将构成 AGENTS.md 禁止的 test-only 脚手架；能力由实验室 e2e 覆盖。
- `cmd/agentd` 的 `FIREPAAS_AGENT_FIRECRACKER_VERSION` 默认由 "v1.14.2" 改为 `firecracker.Starter.GetVersion` 自动探测，新主机将安装 fork 默认版本（V1_16_1）——属用户既有未提交改动，新主机冷装时数据面二进制版本变化需知晓。
