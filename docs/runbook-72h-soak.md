# 72-hour HA soak

Run only after focused HA gates pass. The gate is exactly 72 hours and requires the standard topology (at least two compute nodes, two edges, and three control servers). Use a durable runner, monitor evidence-disk capacity, and keep `FIREPAAS_LOCAL_GC_MODE=off` unless a separately approved single-node quarantine/rollback rehearsal is in progress. A soak is not permission to enable destructive GC.

## Probe contract

Set `SOAK_PROBE_COMMAND` to a command that emits **exactly one JSON object per invocation**, `SLO_SPEC=docs/slo-spec.yaml`, `EVIDENCE_DIR`, and `SOAK_DURATION=72h`. The script rejects shortened runs, malformed output, missing fields, negative values, and probe command failures.

Every observation must include the SLO timing metrics plus these v1.4 safety signals:

| Field | Meaning / source |
|---|---|
| `timestamp` | UTC timestamp for this sample |
| `inventory_age_seconds` | Maximum age, by control-plane receive time, of the latest accepted snapshot/volume observation across expected healthy nodes/types; missing observations must fail the probe, not become zero |
| `inventory_drift_count` | Current `MISSING` + `CORRUPT` snapshot/volume count (or a stricter documented drift query) |
| `scrub_failed_count` | `local_scrub_jobs` in `FAILED`; also report stuck `PENDING/RUNNING` in probe diagnostics |
| `quarantine_active_count` | `local_gc_claims` in `CLAIMED`, `QUARANTINED`, or `ROLLBACK_REQUESTED`; with local GC off this should remain zero |
| `attachment_drift_count` | Non-`DETACHED` PG attachments absent from the corresponding complete agent inventory; do not substitute total attachments |
| `prewarm_pending_count` | `image_prewarm_targets` still `PENDING`; probe diagnostics should distinguish overdue deadlines |
| `image_pin_active_count` | `image_pins` with `expires_at > now()` |

The collector may query PostgreSQL, API responses, and Prometheus, but must use bounded timeouts and fail closed on any unavailable source. Never infer artifact presence from heartbeat. Preserve `inventory_epoch`, generation, node/type, scrub status, quarantine status, attachment identity, prewarm deadline/result, and pin expiry as supplemental fields or companion evidence so a non-zero aggregate is diagnosable.

Example invocation shape (the site-specific collector is intentionally not embedded because credentials/topology differ):

```bash
export EVIDENCE_DIR=/var/lib/firepaas/evidence/soak-$(date -u +%Y%m%dT%H%M%SZ)
export SLO_SPEC=docs/slo-spec.yaml
export SOAK_DURATION=72h
export SOAK_PROBE_COMMAND='/opt/firepaas/bin/collect-ha-observation --format=json'
bash scripts/lab/soak-ha-72h.sh
```

## Acceptance and triage

- `inventory_age_seconds` must satisfy the SLO and each expected `(node,type)` must continue advancing within its epoch. An epoch change is expected only after agent restart; reuse of a retired epoch or generation regression is a failure.
- Any unexpected inventory drift, scrub failure, attachment drift, active quarantine, overdue prewarm target, or pin that outlives its expiry requires investigation. Do not hide these by resetting counters or deleting rows during the run.
- `prewarm`, coverage, and pin are operator-only under ADR-0037. The collector uses an admin credential; tenant credentials must not gain topology visibility.
- An interrupted run, missing sample, shortened duration, or failed objective is a failed gate. `soak-result.json` says only that this run completed and passed the configured evaluator; it is not production acceptance by itself.

Run `bash scripts/lab/soak-ha-72h.sh`. Archive `soak-observations.jsonl`, `soak-slo-result.json`, `soak-result.json`, initial captured evidence, collector version/configuration, and incident notes. Do not execute chaos, root, restore, or destructive GC actions unless separately authorized by their runbooks.
