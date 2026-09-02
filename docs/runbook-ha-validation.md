# HA validation runbook

## Preconditions

A provisioned, isolated lab has two compute hosts, two edge hosts, three control hosts, a client outside the failed host, backups, and approved fault windows. Do not run on production. Use a clean committed checkout and a redacted topology inventory.

Export `COMPUTE_HOSTS`, `EDGE_HOSTS`, `CONTROL_HOSTS`, `VIP_ADDRESS`, `WORKLOAD_URL`, `WORKLOAD_HOSTNAME`, `FIREPAAS_CONFIG_PATHS`, `FIREPAAS_TOPOLOGY_FILE`, `SSH_USER`, `SSH_IDENTITY_FILE`, and `EVIDENCE_DIR`. Host lists are comma-separated; secrets must be supplied through the deployment's secret mechanism, never config archives.

Run `bash scripts/lab/capture-evidence.sh` first. Execute the focused gates below, then call `archive-run.sh` with every result JSON in `RESULT_FILES`. Any missing variable, topology role, raw evidence, or failed probe is a failure. A PASS archive describes the tested lab run only.

## Multi-node scheduler gate

Provision at least two healthy compute nodes and deploy a workload requesting two replicas with the applicable anti-affinity policy. Set `FP_API_URL`, `FP_API_TOKEN`, `SCHEDULER_TEST_REQUEST` (JSON file), and `SCHEDULER_EVIDENCE_COMMAND`. The evidence command must emit `{"replica_count":2,"node_ids":["compute-a","compute-b"]}` from the authority store/API.

Run `bash scripts/lab/e2e-multinode-scheduler.sh`. It fails unless two replicas are running on distinct nodes. Save cleanup output separately; request acceptance is not placement proof.

## VIP failover gate

Use two edge nodes with keepalived or equivalent and run the probe from a third client. Set `VIP_ACTIVE_HOST`, `VIP_FAILOVER_COMMAND`, and `VIP_OWNER_COMMAND`; the owner command must return one host identifier. `WORKLOAD_URL` must traverse the VIP, not a node address.

Run `bash scripts/lab/e2e-vip-failover.sh`. It proves baseline ownership and HTTP 200, injects failure, then requires continued HTTP 200 and a different owner. Restore the failed edge after collecting keepalived and proxy logs; a successful probe does not waive restoration.

## DR recovery gate

Use a fresh isolated recovery target; never overwrite the source environment. A restored control plane must not point at the source fleet's Nomad/agents before workloads are recovered there: leader election is per-Redis and agent fencing rejects stale generations, but a second writer still races the source control plane on shared agents (multinode acceptance finding D4). Wire the restored API only to the recovery fleet's Nomad address, or keep the data plane disconnected until the fleet is rebuilt. Record immutable backup URI, encryption/key-access approval outside the evidence archive, target topology, and source commit/config. Set `DR_BACKUP_URI`, `DR_RESTORE_COMMAND`, and `DR_VALIDATION_COMMAND`.

The validation command must emit booleans `restore_isolated`, `schema_valid`, `data_integrity_valid`, and `traffic_valid`. Run `bash scripts/lab/dr-rehearsal.sh`. It fails closed on any false or missing field. Measure RTO from restore start and document data timestamp separately. This does not create RPO/RTO promises for node-local data.

## 72-hour soak and 30-day observation

Follow [the 72-hour soak runbook](runbook-72h-soak.md). After it passes, start the calendar-duration observation gate. Set `DAILY_OBSERVATION_COMMAND`, `OBSERVATION_DAYS=30`, `OBSERVATION_INTERVAL_SECONDS=86400`, `OBSERVATION_MIN_GAP_SECONDS=82800`, SLO spec, and evidence path.

Run `bash scripts/lab/observe-30d.sh` on a durable monitored runner. The script rejects compressed days, command failures, and insufficient SLO samples. Interruptions make the period failed/incomplete; restart it and never backfill observations.

## Other focused gates

- [Control-plane quorum](runbook-control-plane-quorum.md)
- [Compute node failover](runbook-node-failover.md)
- [Node replacement](runbook-node-replacement.md)
- [Object storage](runbook-object-storage.md)
