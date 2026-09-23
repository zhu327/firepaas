# Runbook：节点扩容信号消费（scale_up_signal）

placement 无候选（`ErrNoCandidates`）时，controller 发出节点扩容信号
（`internal/controlplane/controller/scaleup_signal.go`）：

1. durable 事件：`GET /v1/system/scheduler-events` 中 `kind=scale_up_signal`
  （载荷 `pool/vcpu/mem/disk + 主导拒绝原因`；同 pool 5 分钟节流一次）；
2. 指标：`firepaas_scale_up_signals_total{pool}`；
3. 可选外部通知：`NodeScaleUpNotifier`（`ScaleUpNotifier` cfg 注入）。

默认只到第 2 步——集群不会自己长出节点。消费方式二选一：

## A. webhook（推荐起步）

`WebhookScaleUpNotifier{URL, Token}` 把信号 POST 为 JSON 到接收端。
接收方契约：

- 以 `(pool, operation_id)` 去重（op 重入列 + leader 切换可重发）；
- 非 2xx 即记为投递失败（controller 侧只记日志，已落库事件为准）；
- Token 校验 Bearer；无 Token 时依赖网络 ACL（与 agent metrics 端点同纪律）。

## B. 轮询 scheduler-events（无外部写权限时）

外部 autoscaler（如 Nomad Autoscaler 的自定义 APM/check）轮询
`GET /v1/system/scheduler-events`，按 `kind=scale_up_signal` + `pool`
聚合后调节点池 API。

## Nomad Autoscaler policy 示例意

```hcl
scaling "compute" {
  # check: пром firepaas_scale_up_signals_total{pool="compute"} 5m 增量 > 0
  # action: nomad scaling API 调整 compute 池 count（上限在 policy 写死，
  #   防止无界扩容；缩容走节点 draining 流程，不在本 runbook）。
}
```

配额上限必须写在 policy/云 API 侧（controller 信号不限流总量，只限频）。
缩容（节点回收）不在本信号范围：走节点 draining + evacuate 流程。
