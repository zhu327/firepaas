# Runbook：控制面（API）与 edge 升级与回滚

适用范围：`iac/nomad/control-plane.hcl` 的 `firepaas-api`（docker driver）与
`iac/nomad/edge.hcl` 的 `firepaas-edge`。agentd 升级走
`scripts/lab/upgrade-agentd.sh`（raw_exec 二进制链路），不在本文范围。

镜像来源：CI `images` job 在 tag（v*）推送时构建 `Dockerfile.api` /
`Dockerfile.edge-proxy`、推送 digest 引用并 cosign keyless 签名 + syft SBOM
（artifact `image-provenance-<tag>`）。生产 job 只接受
`<registry>/<repo>@sha256:<digest>` 形式的不可变引用（hcl 变量校验强约束）。

## 前置检查（全部通过才可开始）

1. 备份：`sudo bash scripts/lab/pg-backup.sh`（最近 7 份保留；如需外送多副本，
   配 `FIREPAAS_BACKUP_UPLOAD_CMD`）。确认最新备份与 `.rowcounts` sidecar 存在。
2. 迁移重演：`bash scripts/lab/migration-rehearsal.sh` 必须通过
   （ledger 完整 + 幂等）；本次发布若含新迁移，确认其满足回滚兼容纪律（见下）。
3. 当前状态健康：`nomad job status firepaas-api` / `firepaas-edge` 无 failed
   alloc；`GET /v1/health` 200；`firepaas_operations_pending` ≈ 0、
   `firepaas_nodes_unhealthy` = 0；证书告警
   （`FirePaasTLSCertExpiringSoon/Critical`）静默。
4. 版本记录：`nomad job history firepaas-api`、`firepaas-edge` 记下当前
   稳定 version 号；从 `digests.txt` 记录新旧 image digest。
5. 窗口确认：升级期间写路径短暂不可用是可接受的（canary 期间 leader 锁会
   切换）；安排低峰窗口并通告。

## 发布流程（API）

`control-plane.hcl` 已声明 `update { canary=1, max_parallel=1,
min_healthy_time=30s, healthy_deadline=5m, auto_promote=true,
auto_revert=true }`：Nomad 先起 1 个 canary alloc，健康检查
（`GET /v1/health`，5s 间隔）连续通过 `min_healthy_time` 后自动 promote 并
逐台替换；任何阶段 canary 失健则 **auto_revert 自动回到旧版本**。

```bash
nomad job run -var-file=<env>.vars.hcl \
  -var="api_image=<registry>/firepaas/api@sha256:<new-digest>" \
  iac/nomad/control-plane.hcl
nomad job status -evals firepaas-api   # 跟进 deployment：healthy canary → promoted
```

- 含新迁移的发布：leader 选举后由新 leader 在执行迁移**之前**不接管写路径
  （`db.Migrate` 内 advisory lock 串行化）；canary 与旧版本可能短暂并存，
  因此新迁移必须向前兼容（见"回滚兼容纪律"）。
- `api_count=1` 时替换过程存在秒级写不可用；PENDING 操作不受影响，
  恢复后由新 leader 继续调度。
- `api_count` 放宽到 2 仅用于 HA 演练——生产 count>1 的前置条件是
  `scripts/lab/chaos-control-quorum.sh`、`docs/runbook-control-plane-quorum.md`、
  `docs/runbook-ha-validation.md` 三份证据齐备（hcl 内注释有同样声明）。

## 发布流程（edge）

`edge.hcl` 未显式声明 update 块，采用 Nomad 默认滚动
（`max_parallel=1`，非摧毁式滚动；健康检查为 `GET /healthz`）：

```bash
nomad job run -var-file=<env>.vars.hcl \
  -var="edge_image=<registry>/firepaas/edge@sha256:<new-digest>" \
  iac/nomad/edge.hcl
```

- count=2 + distinct_hosts：任一时刻至多一半 edge 在替换；配合 edge 副本
  级 VIP/LB 摘除完成零中断；差异配置（证书、metrics 9465）随 job spec 生效。
- edge 镜像为非 root + `NET_BIND_SERVICE`；若部署环境禁止该能力导致绑定
  :80/:443 失败，属于环境差异，不要临时退回 root 镜像替代能力审批。

## 后置检查（升级宣告完成前）

1. `nomad job status` 全部 alloc running/healthy；deployment 状态
   `successful`。
2. 冒烟：`fpctl ops ls --status FAILED | head` 无新增；对一个 app 走完
   create → HTTP 200（可复用 `scripts/lab/e2e-m2.sh` 的最短路径）。
3. 观测：`firepaas_operations_pending` 回落 0；edge
   `firepaas_edge_token_errors_total`/`firepaas_edge_beyond_stale_total`
   无异常增长；新进程导出 `firepaas_tls_cert_not_after_seconds`。
4. 归档：deployment eval、新旧 digest、soak/e2e 证据落入 results/。

## ROLLBACK

原则：**先回滚 job 版本（二进制/镜像），不回滚 schema**。迁移是只增不改的
前向历史（AGENTS.md），`db.Migrate` 不支持 down migration。

```bash
nomad job revert firepaas-api <recorded-stable-version>
nomad job revert firepaas-edge <recorded-stable-version>
```

- `nomad job revert` 把 job spec 恢复到指定历史 version 并触发新的
  deployment（同一 canary/auto_revert 语义）；旧 image digest 必须仍在
  registry 可拉取（依赖不可变 digest 交付，禁止覆盖 tag 内容）。
- **回滚兼容纪律（发布评审门禁）**：新迁移必须与上一 release 的二进制兼容，
  即"先升级后回滚"时旧代码可继续读写新 schema。具体：只允许新增表/列
  （带默认值或可空）、新增索引、放宽约束；禁止同 release 内 drop/rename
  列、收紧 NOT NULL、变更列类型。确需破坏性 schema 变更时拆成两个 release
  （先扩后收），中间间隔一个完整观察周期。
- schema 级纠正走"新的前向迁移"（不回收版本号）；数据级恢复走
  `scripts/lab/pg-restore-rehearsal.sh` 演练过的备份恢复链路。
- 回滚后按"后置检查"逐项复核，并把事故时间线归档为 events。

## 关联

- `docs/runbook-operations.md`：告警归因与 ops 决策表、备份调度。
- `docs/runbook-control-plane-quorum.md` / `docs/runbook-ha-validation.md`：
  多写者 HA 的前置演练证据。
- `scripts/lab/migration-rehearsal.sh`：发布前迁移必演。
- `docs/slo-spec.yaml`：升级后 30d 观察期 SLO 判定依据。
