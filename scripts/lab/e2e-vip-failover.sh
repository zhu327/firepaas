#!/usr/bin/env bash
# Validates two-edge VIP transfer using a client path independent of either edge host.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_check_topology; ha_require VIP_ACTIVE_HOST VIP_FAILOVER_COMMAND VIP_OWNER_COMMAND EVIDENCE_DIR
"$HERE/capture-evidence.sh"; ha_wait_http_200 "$WORKLOAD_URL" "$WORKLOAD_HOSTNAME" 30
before=$(eval "$VIP_OWNER_COMMAND") || ha_die "cannot determine initial VIP owner"; [[ "$before" == "$VIP_ACTIVE_HOST" ]] || ha_die "VIP owner $before does not match VIP_ACTIVE_HOST"
start=$(date +%s); eval "$VIP_FAILOVER_COMMAND" || ha_die "VIP failover injection failed"; ha_event vip_failover "$VIP_ACTIVE_HOST"
ha_wait_http_200 "$WORKLOAD_URL" "$WORKLOAD_HOSTNAME" "${VIP_FAILOVER_TIMEOUT_SECONDS:-60}"
after=$(eval "$VIP_OWNER_COMMAND") || ha_die "cannot determine post-failover VIP owner"; elapsed=$(( $(date +%s)-start ))
[[ "$after" != "$before" ]] || ha_die "VIP did not move to a distinct edge host"
python3 - "$EVIDENCE_DIR/vip-failover-result.json" "$before" "$after" "$elapsed" <<'PY'
import json,sys
json.dump({'result':'PASS','old_owner':sys.argv[2],'new_owner':sys.argv[3],'recovery_seconds':int(sys.argv[4])},open(sys.argv[1],'w'),indent=2)
PY
