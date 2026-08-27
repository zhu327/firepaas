# iac/observability：可观测栈（M5.3）

Prometheus 风格文本指标已由 firepaas-api 原生提供（`GET /metrics`，含宿主
gauge）。本目录是可选的完整栈：

```
/iac/observability
├── prometheus.yml            # scrape firepaas-api /metrics（host.docker.internal:8081）
├── prometheus-alerts.yml     # 告警规则（阈值对齐 docs/runbook-capacity.md）
└── grafana/provisioning/     # datasource + firepaas 单机总览 dashboard
```

## 拉起（实验室）

```bash
docker compose --profile observability -f iac/dev/docker-compose.yaml up -d
# Grafana: http://127.0.0.1:3000（anonymous Viewer 已启用）
```

要求 firepaas-api 正监听 8081（e2e-m5 端口）或按需改 prometheus.yml 的
targets。指标缺失时优先检查 `curl /metrics` 是否含 `firepaas_host_*`。

## 生产注意

- scrape_timeout 10s，宿主的 keepalive/防火墙需放行 8081。
- 告警阈值 = capacity model 单行来源；修改必须同步 docs/runbook-capacity.md。
- 多 edge 场景的 `firepaas-*` 汇编方式不随本栈变化（边缘自身 /healthz 即可）。
