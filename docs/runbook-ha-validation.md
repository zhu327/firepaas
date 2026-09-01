# HA validation runbook

## Preconditions

A provisioned, isolated lab has two compute hosts, two edge hosts, three control hosts, a client outside the failed host, backups, and approved fault windows. Do not run on production. Use a clean committed checkout and a redacted topology inventory.

Export `COMPUTE_HOSTS`, `EDGE_HOSTS`, `CONTROL_HOSTS`, `VIP_ADDRESS`, `WORKLOAD_URL`, `WORKLOAD_HOSTNAME`, `FIREPAAS_CONFIG_PATHS`, `FIREPAAS_TOPOLOGY_FILE`, `SSH_USER`, `SSH_IDENTITY_FILE`, and `EVIDENCE_DIR`. Host lists are comma-separated; secrets must be supplied through the deployment’s secret mechanism, never config archives.

Run `bash scripts/lab/capture-evidence.sh` first. Execute the focused runbooks, then call `archive-run.sh` with every result JSON in `RESULT_FILES`. Any missing variable, topology role, raw evidence, or failed probe is a failure. A PASS archive describes the tested lab run only.
