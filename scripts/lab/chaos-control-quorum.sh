#!/usr/bin/env bash
# Stops one of three Nomad/Consul servers and verifies server quorum only.
# The current API writer deployment is intentionally count=1; this script must
# never report an API write as high-availability evidence.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_check_topology; ha_require QUORUM_FAILED_HOST CONTROL_STOP_COMMAND CONTROL_QUORUM_EVIDENCE_COMMAND EVIDENCE_DIR
[[ ",$CONTROL_HOSTS," == *",$QUORUM_FAILED_HOST,"* ]] || ha_die "QUORUM_FAILED_HOST is not in CONTROL_HOSTS"
"$HERE/capture-evidence.sh"; ha_require_cmd curl
start=$(date +%s); eval "$CONTROL_STOP_COMMAND" || ha_die "control-plane stop command failed"; ha_event control_member_stopped "$QUORUM_FAILED_HOST"
# The evidence command must derive membership from Nomad/Consul and preserve raw
# output beside this JSON. We deliberately do not assert API write success here:
# the current architecture has a single API writer, not API write HA.
eval "$CONTROL_QUORUM_EVIDENCE_COMMAND" > "$EVIDENCE_DIR/control-quorum.json" || ha_die "control quorum evidence query failed"
python3 - "$EVIDENCE_DIR/control-quorum.json" "$EVIDENCE_DIR/control-quorum-result.json" "$(( $(date +%s)-start ))" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); ok=x.get('healthy_members') == 2 and x.get('nomad_quorum') is True and x.get('consul_quorum') is True
json.dump({'result':'PASS' if ok else 'FAIL','recovery_seconds':int(sys.argv[3]),'scope':'Nomad/Consul quorum only; API write HA is not asserted','evidence':x},open(sys.argv[2],'w'),indent=2)
if not ok: raise SystemExit(1)
PY
