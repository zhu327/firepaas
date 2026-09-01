#!/usr/bin/env bash
# Fences the node holding a known logical replica and proves that exact ordinal
# is detected and recreated under a new execution on a different compute node.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_check_topology
ha_require FAILED_COMPUTE_HOST NODE_FENCE_COMMAND NODE_FAILOVER_EVIDENCE_COMMAND EVIDENCE_DIR FAILOVER_APP_ID FAILOVER_REPLICA_ORDINAL
[[ ",$COMPUTE_HOSTS," == *",$FAILED_COMPUTE_HOST,"* ]] || ha_die "FAILED_COMPUTE_HOST is not in COMPUTE_HOSTS"
"$HERE/capture-evidence.sh"; ha_wait_http_200 "$WORKLOAD_URL" "$WORKLOAD_HOSTNAME" 30
# The authority query takes `before`, `detect`, or `after` as $1. It must emit
# app_id, replica_ordinal, node_host, execution_id, state, readiness; `detect`
# additionally emits node_status=UNHEALTHY|UNKNOWN for the fenced node.
eval "$NODE_FAILOVER_EVIDENCE_COMMAND before" > "$EVIDENCE_DIR/node-failover-before.json" || ha_die "pre-fault authority query failed"
python3 - "$EVIDENCE_DIR/node-failover-before.json" "$FAILED_COMPUTE_HOST" "$FAILOVER_APP_ID" "$FAILOVER_REPLICA_ORDINAL" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); want_host,app,ordinal=sys.argv[2:]
if not (x.get('app_id') == app and str(x.get('replica_ordinal')) == ordinal and x.get('node_host') == want_host and x.get('execution_id')):
 raise SystemExit('pre-fault evidence must identify target app/ordinal on FAILED_COMPUTE_HOST with execution_id')
PY
start=$(date +%s); eval "$NODE_FENCE_COMMAND" || ha_die "node fencing command failed"; ha_event node_fenced "$FAILED_COMPUTE_HOST"
# Detect must be established independently of surviving replica traffic.
detect_deadline=$((start + 60)); detected=0
while (( $(date +%s) <= detect_deadline )); do
  eval "$NODE_FAILOVER_EVIDENCE_COMMAND detect" > "$EVIDENCE_DIR/node-failover-detect.json" || ha_die "detection authority query failed"
  if python3 - "$EVIDENCE_DIR/node-failover-detect.json" "$FAILED_COMPUTE_HOST" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); raise SystemExit(0 if x.get('node_host') == sys.argv[2] and x.get('node_status') in ('UNHEALTHY','UNKNOWN') else 1)
PY
  then detected=1; break; fi
  sleep 2
done
(( detected == 1 )) || ha_die "fenced node was not detected UNHEALTHY/UNKNOWN within 60s"
detect_elapsed=$(( $(date +%s)-start ))
# The exact logical replica must be READY by the documented 120s target.
ready_deadline=$((start + 120)); replacement=0
while (( $(date +%s) <= ready_deadline )); do
  eval "$NODE_FAILOVER_EVIDENCE_COMMAND after" > "$EVIDENCE_DIR/node-failover-after.json" || ha_die "replacement authority query failed"
  if python3 - "$EVIDENCE_DIR/node-failover-before.json" "$EVIDENCE_DIR/node-failover-after.json" "$FAILED_COMPUTE_HOST" <<'PY'
import json,sys
before=json.load(open(sys.argv[1])); after=json.load(open(sys.argv[2])); failed=sys.argv[3]
ok=(after.get('app_id') == before.get('app_id') and str(after.get('replica_ordinal')) == str(before.get('replica_ordinal'))
    and after.get('execution_id') and after.get('execution_id') != before.get('execution_id')
    and after.get('node_host') and after.get('node_host') != failed
    and after.get('state') == 'RUNNING' and after.get('readiness') == 'READY')
raise SystemExit(0 if ok else 1)
PY
  then replacement=1; break; fi
  sleep 2
done
(( replacement == 1 )) || ha_die "same ordinal did not reach new execution RUNNING/READY on another node within 120s"
ready_elapsed=$(( $(date +%s)-start )); ha_wait_http_200 "$WORKLOAD_URL" "$WORKLOAD_HOSTNAME" 15
python3 - "$EVIDENCE_DIR/node-failover-result.json" "$EVIDENCE_DIR/node-failover-observations.jsonl" "$EVIDENCE_DIR/node-failover-before.json" "$EVIDENCE_DIR/node-failover-after.json" "$detect_elapsed" "$ready_elapsed" <<'PY'
import datetime,json,sys
before=json.load(open(sys.argv[3])); after=json.load(open(sys.argv[4])); detect,ready=map(int,sys.argv[5:])
out={'result':'PASS','node_failure_detect_seconds':detect,'node_failover_seconds':ready,'before':before,'after':after}
json.dump(out,open(sys.argv[1],'w'),indent=2)
with open(sys.argv[2],'a') as f: f.write(json.dumps({'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(), **out})+'\n')
PY
