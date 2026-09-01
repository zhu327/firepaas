#!/usr/bin/env bash
# Proves scheduler placement across two compute nodes and anti-affinity from API evidence.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_check_topology; ha_require FP_API_URL FP_API_TOKEN SCHEDULER_TEST_REQUEST SCHEDULER_EVIDENCE_COMMAND EVIDENCE_DIR
"$HERE/capture-evidence.sh"
ha_require_cmd curl
out="$EVIDENCE_DIR/multinode-scheduler.json"
response=$(curl --fail-with-body -sS -H "Authorization: Bearer $FP_API_TOKEN" -H 'Content-Type: application/json' -X POST "$FP_API_URL/v1/apps" --data-binary "@$SCHEDULER_TEST_REQUEST") || ha_die "scheduler test request failed"
printf '%s\n' "$response" > "$EVIDENCE_DIR/scheduler-create-response.json"
# Operator supplies a non-secret query command that emits JSON with distinct node_ids and replica_count.
eval "$SCHEDULER_EVIDENCE_COMMAND" > "$EVIDENCE_DIR/scheduler-placement.json" || ha_die "placement evidence query failed"
python3 - "$EVIDENCE_DIR/scheduler-placement.json" "$out" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); nodes=set(x.get('node_ids',[])); replicas=x.get('replica_count',0)
ok=replicas>=2 and len(nodes)>=2
json.dump({'result':'PASS' if ok else 'FAIL','replica_count':replicas,'distinct_nodes':len(nodes)},open(sys.argv[2],'w'),indent=2); print()
if not ok: raise SystemExit(1)
PY
ha_event scheduler_multinode "two replicas placed on distinct compute nodes"
