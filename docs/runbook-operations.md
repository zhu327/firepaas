# Runbook：operations 处置（告警 → 归因）

告警：`FirePaasOpsBacklog`（积压>20）/ `FirePaasAgentRPCErrors`（速率>0.1/s）。

## 快速归因（一条命令链）

```bash
export FP_API_ADDR=http://127.0.0.1:8081 FP_API_TOKEN=...
fpctl ops ls --status FAILED | head    # 最近终态失败
fpctl ops show <operation-id>          # 完整 request/result（已脱敏）+ attempts
```

## 决策表

| 现象 | 归因 | 动作 |
|---|---|---|
| kind=create 反复 FAILED 且 attempts 大 | 镜像不可解析（永久错误）或 agent 容量拒 | `fpctl ops show` 看 error；镜像问题改镜像；容量问题缩容 |
| CLAIMED 卡死 | 写者 crash/leader 切换 | 自动回收（30s RequeueStaleClaimed）；`/metrics` `firepaas_operation_stale_claims_recovered_total` 应增长 |
| RPC errors 上升 + nodes_unhealthy>0 | 节点失联 | 节点替换 runbook |
| 全部 PENDING 不执行 | controller 非 leader 或 loop 停滞 | `GET /v1/nodes` 确认 API 仍 leader；重启 firepaas-api（leader 锁自动接管）|
| pause/resume FAILED（FailedPrecondition） | 快照缺失/损坏 | controller 自动转 cold-start 重建（design）；观察新 create 是否成功 |

## soak/升级的严格验收

- `scripts/lab/soak-m5.sh` 每轮必须真实完成 create → HTTP 200 → scale 到 2 →
  deploy（不同 digest）→ pause/resume fault → delete-state 收敛。任一 API、SQL 状态或
  流量断言失败即清理后以非零退出；不能把 curl 成功或累计三次失败视为通过。
- delete-state 的通过条件是该 app 没有 `desired_state <> 'DELETED'` 的 machine，且没有
  关联的 `PENDING`/`CLAIMED` operation。`summary.csv` 只是证据，非替代断言。
- `upgrade-agentd.sh`（v1.1 evacuate 版）只在 drain+evacuate（machine 归零）、
  Nomad 重启、READY/非 DRAINING 复原和 PENDING/CLAIMED backlog 均为零时成功；
  失败节点保持排水，按节点替换流程处理。

## v1.1 增补：多副本排障（ADR-0019）与 edge 并发（ADR-0020）

多副本 app 排障流程（"这个请求命中了哪个副本"）：

```bash
# 1) 取命中实例标识（edge → 客户端方向的响应头）：
curl -sk https://<app>.internal/ -D - -o /dev/null | grep -i X-Firepaas-Machine-ID
# 2) 把调试会话钉在该实例上（复现问题；实例换代后 404 = 客户端重新取 id）：
curl -sk -H "X-Firepaas-Pin-Machine: <machine_id>" https://<app>.internal/
```

注意两个方向的 `X-Firepaas-Machine-ID`：edge→客户端是**响应头**（命中标识）；
edge→agent proxy 是**请求头**（路由寻址）。钉扎是调试契约——平台不承诺钉扎
请求的路由稳定性；钉错 id 返回 404（与 503"平台侧不可用"显式区分）。

edge 并发控制（per-edge 本地视角，多 edge 各自计数——集群容量上限 ≈
N×edge 数）：

| 现象 | 归因 | 动作 |
|---|---|---|
| 503 + `all backends at hard concurrency limit` | per-machine inflight ≥ `FIREPAAS_EDGE_HARD_CONCURRENCY`（默认 256） | 扩副本（least-inflight 已自动偏移）；确认慢依赖/长连接占用 |
| 503 + `pinned machine at hard concurrency limit` | 钉扎目标超限（调试流量打死实例的防线） | 换实例或等 inflight 回落 |
| `firepaas_edge_backend_inflight` 长期高位 | 长连接（WS/SSE）持续占用（预期语义） | 按业务确认；app 级并发配置是 v1.2 候选 |

auto-standby（ADR-0017）相关：app 声明
`auto_standby: {enabled, idle_timeout_seconds}` 后空闲实例自动 standby
（释放 VMM 内存；readiness 冻结、保留 route backends），首请求经 proxy
autoresume 唤醒（<5s）。`/metrics` 关注
`firepaas_machine_standby_total`（控制面）与
`firepaas_agent_autostandby_wakes_total`/`..._wake_seconds`（agent）。

## v1.4 本地完整性、GC 与镜像治理

- `FIREPAAS_LOCAL_GC_MODE` 的安全默认值是 `off`：不扫描、不 claim、不删除。`dry-run`
  只报告 image cache 候选；delete 仅在 agent 广告 lock-aware、path-free quarantine
  capability 后执行 claim → quarantine → grace → 再检查 → finalize/rollback。不得用直接
  文件操作或 `DeleteImage` 绕过 ADR-0036。
- inventory 以控制面已接受的 `(node_id, resource_type, epoch, generation)` 为准。同一
  epoch generation 必须递增；agent 重启产生新 epoch；旧 agent、缺字段、不完整列表、
  迟到的 retired epoch 只能 UNKNOWN。heartbeat 不能证明本地产物存在。
- `MISSING`/`CORRUPT` 会阻止 restore/attach。snapshot scrub 默认关闭，开启前需完成 IO
  基准并确认 agent checksum capability；`METADATA_VERIFIED` 不等于内容校验，presence
  不得清除 CORRUPT。
- `local_gc_claims` 长时间停留在 `CLAIMED`、`QUARANTINED` 或
  `ROLLBACK_REQUESTED` 属于异常：立即将 local GC 设为 off，并按 claim token 完成
  rollback 或人工核对。
- prewarm、coverage、pin/list/unpin 按 ADR-0037 仅限 root/admin。project read/write key
  返回 403 是预期行为；node-pool ACL 落地前不得为租户放宽。mutation 应携带稳定的
  `Idempotency-Key`，同 key 不得更换请求正文。

### 发布与回滚顺序

1. 发布时保持 local GC off，先滚动兼容控制面，再滚动 agent；确认每个健康节点的
   snapshot/volume observation epoch 非空、generation 持续推进且 age 在阈值内。
2. 如需观测候选，只按节点审批切到 dry-run；先核对 PG roots、active attachments、
   instance/ledger、prewarm target 与 active pin。当前版本不得批准 physical delete。
3. 回滚前先恢复 `FIREPAAS_LOCAL_GC_MODE=off`，停止新 prewarm/pin/scrub，等待 prewarm
   target 终态。检查并将 CLAIMED/QUARANTINED 收敛 ABORTED/ROLLED_BACK；若出现无法
   rollback 的 quarantine，停止回滚并升级处理。
4. 再回滚二进制；保留新增表列，不执行 down migration。已 finalize 的 node-local 内容
   无法靠二进制回滚恢复，因此不可恢复资源永不进入自动删除。

72h 门禁的每个 probe 必须收集 inventory age/drift、scrub failure、quarantine active、
attachment drift、prewarm pending 和 active pin；详见 `docs/runbook-72h-soak.md`。

## 证书到期处置

告警：`FirePaasTLSCertExpiringSoon`（<30d，warning）/
`FirePaasTLSCertExpiryCritical`（<7d，critical），数据源各进程导出的
gauge `firepaas_tls_cert_not_after_seconds{file=...}`（热重载契约 C-1；
未导出该指标的进程本身就是接入缺口）。

处置顺序：

1. 确认哪个进程的哪张证书（label `file`）；实验室证书由
   `scripts/lab/gen-certs.sh` 产出，生产证书由部署秘密通道下发。
2. 准备新证书（同 CA、同 CN——mTLS 身份白名单按 CN 生效，换 CN 等同换身份）。
3. 替换证书文件；运行中的进程按 CertManager 周期自动热重载（重读失败保留
   旧证书并 warn——watch `/metrics` 中该 gauge 是否前进到新 notAfter 确认生效）。
4. 证书已过期导致 mTLS 断链时：先恢复证书再重连，不得用
   `FIREPAAS_EDGE_ALLOW_INSECURE_DEV=true` / `FIREPAAS_ALLOW_INSECURE_DEV=true`
   明文降级绕过（两开关仅限本地开发，生产部署不得设置）。
5. 轮换完成且告警静默后，把新 certificate 的 notAfter 录入容量/证书台账。

备份注意：edge 公共入口证书与 edge→agent mTLS 客户端证书是两套
（edge.hcl 模板变量分离），轮换别混用。

## 备份与恢复（例行）

- 例行备份：`scripts/lab/pg-backup.sh`（默认保留最近 7 份；可选 gpg 对称
  加密 `FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE` 与外送 hook
  `FIREPAAS_BACKUP_UPLOAD_CMD`，默认关闭）。
- 恢复演练：`scripts/lab/pg-restore-rehearsal.sh`；DR 链路见
  `scripts/lab/dr-rehearsal.sh`。

### 定时调度（cron / systemd timer 二选一）

`FIREPAAS_BACKUP_UPLOAD_CMD` 以"命令前缀"方式调用：脚本把最终产物路径追加为
最后一个参数（`eval "$FIREPAAS_BACKUP_UPLOAD_CMD" <artifact>`）；目的地、远端
凭证写在接受单参数的目的端 wrapper 脚本里（例中
`/usr/local/bin/firepaas-backup-upload`，可用 rclone/aws cli 封装）。

cron（以 root 或具备 docker 权限的备份账户）：

```cron
# 每日 03:30 备份并外送；输出进 journal 无关时落日志文件
30 3 * * * FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE=/etc/firepaas/backup.pass \
  FIREPAAS_BACKUP_UPLOAD_CMD='/usr/local/bin/firepaas-backup-upload' \
  /usr/bin/bash scripts/lab/pg-backup.sh /var/lib/firepaas-p0/backups/postgres \
  >> /var/log/firepaas/pg-backup.log 2>&1
```

systemd（pg-backup.service + pg-backup.timer）：

```ini
# /etc/systemd/system/firepaas-pg-backup.service
[Unit]
Description=firepaas PostgreSQL backup
[Service]
Type=oneshot
Environment=FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE=/etc/firepaas/backup.pass
ExecStart=/usr/bin/bash scripts/lab/pg-backup.sh /var/lib/firepaas-p0/backups/postgres

# /etc/systemd/system/firepaas-pg-backup.timer
[Unit]
Description=daily firepaas pg backup
[Timer]
OnCalendar=*-*-* 03:30:00
Persistent=true
[Install]
WantedBy=timers.target
```

凭证文件（gpg passphrase、上传命令内含的远端凭证）权限 0400 且不提交仓库。
备份失败以下一轮告警周期人工发现过晚——对 oneshot 失败配置
`OnFailure=` 通知或外送 hook 成功性监控。

## 发布回滚

控制面（API）与 edge 的版本回滚（`nomad job revert`）、迁移回滚兼容纪律
（新迁移须保持同 release 内 rollback-compatible）见
`docs/runbook-upgrade-control-plane.md` 的 ROLLBACK 章节；发布前必须完成
`scripts/lab/pg-backup.sh` + `scripts/lab/migration-rehearsal.sh` 两项前置。
agentd 回滚沿用 `docs/runbook-operations.md` v1.4 节的发布与回滚顺序。

## 复盘要求（压测后一次，mvp-plan §9.3）

任一压测结束：导出 `GET /v1/operations?limit=500` 与 `/metrics` 快照；
按 machine_id 汇总 created→claimed→completed 时间轴（op trace 字段全在），
把 P50/P95 dispatch 延迟记入 results/，异常即归档为 events。
