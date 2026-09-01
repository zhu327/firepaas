# 可观测栈（M5.3，optional profile）

单机实验室默认**不启动**；需要时：

```bash
docker compose -f iac/dev/docker-compose.yaml --profile observability up -d
# Prometheus  http://<host>:9090（targets 自检）
# Grafana     http://<host>:3000（匿名 Viewer；dashboard 自动 provisioning）
```

组件与数据流：

- `firepaas-api` 在 `/metrics` 暴露 Prometheus 文本指标（宿主 gauge 由
  `cmd/api/host_linux.go` 每 15s 采样；平台 gauge 由 controller 每 sync 刷新）。
- `prometheus.yml`：抓取 `host.docker.internal:8083`（对齐 e2e-m5 常驻 API
  端口；e2e-m4 为 8081、裸装为 8080，按实际修改 target）。
- `prometheus-alerts.yml`：host（mem/FD/conntrack/entropy/load）+ platform
  （PENDING 积压/节点不健康/RPC 错误）两组规则；阈值与
  `docs/capacity-model.md` 同步维护。
- Grafana provisioning：datasource 指向 prometheus 服务；`firepaas.json`
  包含容量/调度与机器/发布状态机/对账与 RPC，以及 v1.4 inventory integrity、orphan、
  prewarm/pin 和 local GC 候选视图。

v1.4 运维注意：

- `FIREPAAS_LOCAL_GC_MODE` 默认且推荐保持 `off`。`dry-run` 才会产生
  `firepaas_gc_candidates`；当前无 agent quarantine/finalize 能力，dashboard 出现候选
  不表示允许删除，更不表示回收成功。
- `firepaas_local_inventory_support` 只说明节点声明支持；完整性分布见
  `firepaas_local_integrity`，orphan bytes 见 `firepaas_local_orphan_bytes`。当前尚无直接的
  inventory age、scrub job、quarantine/claim、attachment drift、pending prewarm 或 active
  pin gauge；72h collector 必须从 PostgreSQL/API 补采，见 `docs/runbook-72h-soak.md`。
- `firepaas_prewarm_targets_total` 与 `firepaas_image_pins_expired_total` 是累计 counter，不能
  代替当前 pending/active 数。prewarm/coverage/pin API 仅允许 root/admin。

注意：

- API 设置 `FIREPAAS_METRICS_TOKEN` 后，需在 prometheus.yml 的 job 里追加
  `authorization: credentials: <token>`（P3-15）。
- 该栈只读 API，不参与数据面；停止不影响业务。
