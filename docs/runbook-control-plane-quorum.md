# Nomad/Consul quorum validation (not API HA)

This runbook validates **only Nomad/Consul's three-server quorum**. The current
production `firepaas-api` job deliberately has one active writer (`api_count=1`;
ADR-0007), so it must **not** be used to claim API write high availability.

Use exactly three independently placed Nomad/Consul server members. Before the
run, capture `nomad server members`, `consul members`, `nomad operator raft
list-peers`, and the API allocation host. Configure:

- `CONTROL_HOSTS`: the three server hosts;
- `QUORUM_FAILED_HOST`: a server that does **not** host the single API alloc;
- `CONTROL_STOP_COMMAND`: a remote command that stops only that server's
  Nomad/Consul processes;
- `CONTROL_QUORUM_EVIDENCE_COMMAND`: a read-only command that derives
  `healthy_members=2` from Nomad/Consul membership and includes raw output in
  the evidence directory.

Run `bash scripts/lab/chaos-control-quorum.sh`. It may assert that existing API
traffic remains available when a non-API server is stopped, but it must not issue
or report a successful API write as a quorum/HA proof. A test that stops the API
host is expected to demonstrate controlled unavailability until Nomad restarts
it; record that result separately.

Restart and reconcile the stopped member after evidence collection. API write HA
requires a future ADR and a separately implemented multi-writer deployment; this
runbook does not waive that requirement.
