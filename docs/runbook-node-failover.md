# Compute-node failover validation

This is a **real two-compute-node** gate, not a process-crash simulation. Deploy
a digest-pinned, stateless app with at least two replicas and record the target
logical replica before injecting failure.

Set these inputs before running:

- `FAILED_COMPUTE_HOST`: a member of `COMPUTE_HOSTS` that currently hosts the
  target ordinal;
- `FAILOVER_APP_ID` and `FAILOVER_REPLICA_ORDINAL`;
- `NODE_FENCE_COMMAND`: power/network/process fencing that prevents the target
  host from serving or accepting work;
- `NODE_FAILOVER_EVIDENCE_COMMAND`: accepts `before`, `detect`, or `after` as
  `$1` and queries authority state (API/PG plus Nomad host mapping). For
  `detect`, it must emit the fenced `node_host` and `node_status` of
  `UNHEALTHY` or `UNKNOWN`; for `before`/`after`, it must emit:

```json
{
  "app_id": "...",
  "replica_ordinal": 0,
  "node_host": "compute-a",
  "execution_id": "...",
  "state": "RUNNING",
  "readiness": "READY"
}
```

Run `bash scripts/lab/chaos-node-failover.sh`. It rejects a result unless the
fenced node becomes `UNHEALTHY`/`UNKNOWN` within **60 seconds** and the same
app/ordinal moves to a different compute host with a **new execution ID** and
reaches `RUNNING` plus `READY` within **120 seconds**. It writes both durations
to a standard SLO observation JSONL; an unrelated healthy replica cannot satisfy
the gate.

Restore the host only after collecting forensic logs. This validates stateless
replacement only; node-local volume/snapshot recovery has no RTO/RPO promise.
