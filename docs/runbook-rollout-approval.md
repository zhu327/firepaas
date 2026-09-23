# Runbook：发布审批 gate（PAUSED_FOR_APPROVAL）处置

适用：`approval_required=true` 的 deploy，或 rolling + `canary_weight>0`
（隐含进 gate）。新代全 READY 后 rollout 停在 PAUSED 等人工放行，
旧代继续按 PREPARING 混合窗口服务（不断流）。

## 正常放行

```bash
export FP_API_ADDR=... FP_API_TOKEN=...
fpctl rollout approve <rollout_id>   # → CUTOVER，走既有观察窗/回收路径
```

## 发现坏代（审批等待中回滚）

```bash
fpctl app rollback <app_id>          # PAUSED 可直接回滚 → ROLLING_BACK
```

## 卡住排查（stuck PAUSED）

- 指标：`firepaas_rollouts_paused_for_approval` 持续大于 0 即有人没点放行；
  租户事件有 `approval_pending`（谁发起的 deploy 可查）。
- 该 app 后续 deploy 一直 409（`ErrRolloutBusy`）是符合预期的互斥，不是 bug：
  先 approve 或 rollback 终结当前 rollout。
- 误配（不想要 gate 却进了）：回滚本轮后重发不带 `approval_required`
  的 deploy；rolling canary 去掉 `canary_weight` 即不隐含进 gate。

## 降级/回滚到旧二进制前（必做）

旧二进制看不见 PAUSED 行但新索引仍计其为 active，残留会导致该 app 永久
409。降级前终结所有 PAUSED：

```sql
UPDATE rollouts SET status='COMPLETE', failed=false, completed_at=now(),
  updated_at=now() WHERE status='PAUSED_FOR_APPROVAL';
```

## 告警建议

- `firepaas_rollouts_paused_for_approval > 0` 持续 30m：提醒审批人。
- CUTOVER 自动回滚仍看既有 `firepaas_rollout_auto_rollback_total{stage="cutover"}`。
