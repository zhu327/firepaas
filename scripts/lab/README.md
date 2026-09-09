# firepaas 实验室脚本（分层导航）

> 脚本测试体系按层组织；每层前置不同，入口统一为 [`verify.sh`](#统一入口-verifysh)。
> 每个脚本头部有机器可读元数据：`LAYER / PREREQ / DESTRUCTIVE / FROZEN`。
> 状态：ADR-0040 fabric（G1–G3）为当前主战场；单机基线（ADR-0012）与
> 多节点 HA（runbook-ha-validation）互斥使用，见各层说明。

## 分层

```
L0 CI 门禁        无状态，push 即跑（.github/workflows/ci.yml）
L1 无状态集成     make check-lab（dev-up 后：PG/Redis-gated 测试 + 调度仿真）
L2 单机回归       smoke-p0 → e2e-m3/m4/m5（真机，需单机 agentd）
L3 fabric 实验室  spike stage1/2/3（→ chaos-fabric → soak-fabric，需授权）
L4 多节点 HA      provisioned 环境，发布前（bootstrap-lab → failover/quorum/vip → dr → soak-ha → observe-30d）
```

## 统一入口 verify.sh

```bash
sudo bash scripts/lab/verify.sh --layer l2                 # 单机回归全链
sudo bash scripts/lab/verify.sh --layer l3                 # fabric spike 全链
sudo bash scripts/lab/verify.sh --layer l3 --with-chaos --soak-cycles 20
bash  scripts/lab/verify.sh --layer l4                     # 仅打印清单（不自动执行）
```

- fail-closed：任一步 FAIL 即退出，不伪造 PASS；逐步结果落
  `/var/lib/firepaas-p0/verify/<runid>.log`。
- 破坏性步骤（chaos、soak）必须显式授权（`--with-chaos` / `--soak-cycles`）。
- 节点 ID / API token / 镜像引用自动从实验室约定派生（wg_peers、
  `/tmp/fabric-api-token`、本地 registry tag），可用环境变量覆盖。

## L1 基座（单机 Nomad + P0，ADR-0012）

| 脚本 | 用途 |
|---|---|
| `env.sh` | PATH / NOMAD_ADDR 环境变量（`source` 使用） |
| `start.sh` / `stop.sh` / `status.sh` | Nomad（可选 Consul）生命周期 |
| `root-setup.sh` / `m0-root-verify.sh` | root 一次性准备与验证 |
| `run-p0.sh` / `smoke-p0.sh` | P0 hypeman job 部署与冒烟 |
| `build-hypeman.sh` | 构建 hypeman/CLI/token 工具（用户态） |
| `install-protoc.sh` | protoc 安装（`make proto` 前置） |
| `gen-certs.sh` | 静态 mTLS 证书（产物 gitignore） |
| `nomad-single.hcl` / `consul-single.hcl` / `hypeman-p0.yaml` / `agentd.yaml` / `agentd-b.yaml` | 配置 |
| `scripts/bench-hypeman.sh` | M0 基准（结果入 `results/` 与 docs/benchmarks.md） |

## L2 单机功能回归（有状态，需授权）

| 脚本 | 用途 |
|---|---|
| `run-agentd.sh` | 部署单机 agentd job（配 `iac/nomad/agentd-single.hcl`） |
| `e2e-m3.sh` | slot 数据面 / 发布回滚 / 1000 次 slot 无泄漏 |
| `e2e-m4.sh` | secrets / execution-bound 凭证 / 限流 / Redis 宕机 serve-stale |
| `e2e-m5.sh` | M5 六段验收（安全/时钟/指标/备份/重投影/升级/泄漏） |
| `pg-backup.sh` / `pg-restore-rehearsal.sh` / `minio-backup-rehearsal.sh` | 备份与恢复演练 |
| `migration-rehearsal.sh` / `upgrade-agentd.sh` | schema migration / evacuate 升级演练 |
| `host-hardening-check.sh` | 只读安全审计 |
| `soak-m5.sh` / `run-soak.sh` | M5 72h soak runner / 60min 排练专用 API+edge |

## L3 fabric 实验室（ADR-0040，当前主战场）

| 脚本 | 用途 |
|---|---|
| `push-ontime.sh` | 构建/推送 ontime 探针镜像（digest 输出供 e2e/soak 引用） |
| `e2e-fabric-spike.sh` | G1–G3 验收：stage1 host→guest / stage2 跨节点 WG+eBPF 策略 / stage3 edge mesh direct+回落；`--check-only` 为前置门禁 |
| `chaos-fabric.sh` | G3 混沌：peer-loss / partition / key-rotation / stale-replay / split-brain（破坏性，须授权） |
| `soak-fabric.sh` | G3 soak：分配/释放循环无泄漏 + 快照/握手新鲜度长跑窗 |
| `nomad-client2.hcl` / `agentd-b.yaml` | 同主机第二 client/node-b（与 `iac/nomad/agentd-fabric-dual.hcl` 配套） |

## L4 多节点 HA（provisioned 环境，runbook-ha-validation）

| 脚本 | 用途 |
|---|---|
| `scripts/bootstrap-lab.sh` | 3 server + 2 compute 标准实验室引导 |
| `ha-lib.sh` | 多节点共享 fail-closed helper（无默认拓扑，source 使用） |
| `e2e-multinode-scheduler.sh` | 双 compute 放置与反亲和（API 证据） |
| `chaos-node-failover.sh` | fence 持副本节点 → 特定序号在新节点重建 |
| `chaos-control-quorum.sh` | server quorum（API writer count=1，不作 HA 证据） |
| `e2e-vip-failover.sh` | 双 edge VIP 迁移 |
| `dr-rehearsal.sh` | 隔离目标恢复 + 恢复后流量证明 |
| `soak-ha-72h.sh` / `observe-30d.sh` | 72h 连续探针 / 30 天观察门（assert-slo.py 断言） |

## 证据链（跨层）

`capture-evidence.sh`（采集）→ `assert-slo.py`（SLO 断言）→ `archive-run.sh`
（结果全 PASS 后打包 SHA256SUMS）。产物在 `results/` 与 `/var/lib/firepaas-p0/`
各 RUN_DIR；CI 只做 shell 语法 + `lib/runtime_test.sh` + Nomad validate。

## archive/（冻结里程碑，仅供历史验收复现）

```
archive/e2e-m1.sh          # M1 身份/mTLS 一键验收
archive/e2e-m2.sh          # M2 派发/R3 重建/对账
archive/chaos-m2.sh        # M2 混沌（ACK 丢失 / agent crash）
archive/e2e-v11.sh         # v1.1 evacuate + 双端口
archive/e2e-v12.sh         # v1.2 配额/限流/事件/GC
archive/e2e-v13-egress.sh  # v1.3 egress CIDR/domain
archive/e2e-v13-snapshot.sh# v1.3 快照
archive/e2e-v13-volume.sh  # v1.3 卷 + v1.4 overlay 语义
archive/e2e-v14.sh         # v1.4 验收
```

冻结脚本不再随当前代码演进；路径引用已更新为 `scripts/lab/archive/`，内部
`$HERE` 依赖已改指上级 lab 目录。历史验收叙述（implementation-notes/plans）
中的段位引用（如"e2e-v11 D 段"）指向这些脚本。

## 互斥与端口

- `iac/nomad/agentd-single.hcl`（单机，ADR-0040 起即单节点 mesh 形态）与
  `iac/nomad/agentd-fabric-dual.hcl` **互斥**：同 WG 端口（51820）与网段，
  跑单机 e2e 前先 `nomad job stop firepaas-agentd-dual`，反之亦然。
- 与 `hypeman-p0` job 互斥（共享 data_dir 实例状态）。
- 端口表：Nomad 4646/4647/4648；agentd 5108/5107；API 8080（单机）/8083
  （fabric）；edge 8081（单机）/8084（fabric）；WG 51820/51920；mesh ingress
  5109；dev 依赖 5432/6379/9000-9001/5000。

## 受限网络（Docker Hub 被劫持/超时）

- Docker 镜像经 `docker.m.daocloud.io` 拉取后 retag；hypeman 直连
  `index.docker.io` 的基件拉取用 `HYPEMAN_DOCKER_HUB_MIRROR`（已默认配置）。
- ontime 镜像用 `push-ontime.sh` 自建自推（不再依赖手工预推 tag——P1-6 教训）。
