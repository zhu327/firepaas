# Multi-node scheduler validation

Provision at least two healthy compute nodes and deploy a workload requesting two replicas with the applicable anti-affinity policy. Set `FP_API_URL`, `FP_API_TOKEN`, `SCHEDULER_TEST_REQUEST` (JSON file), and `SCHEDULER_EVIDENCE_COMMAND`. The evidence command must emit `{"replica_count":2,"node_ids":["compute-a","compute-b"]}` from the authority store/API.

Run `bash scripts/lab/e2e-multinode-scheduler.sh`. It fails unless two replicas are running on distinct nodes. Save cleanup output separately; do not treat scheduling request acceptance as placement proof.
