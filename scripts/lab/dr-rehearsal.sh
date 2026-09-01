#!/usr/bin/env bash
# Executes a documented restore into an isolated target and proves post-restore traffic.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_require DR_BACKUP_URI DR_RESTORE_COMMAND DR_VALIDATION_COMMAND EVIDENCE_DIR FIREPAAS_CONFIG_PATHS FIREPAAS_TOPOLOGY_FILE
"$HERE/capture-evidence.sh"; start=$(date +%s)
eval "$DR_RESTORE_COMMAND" || ha_die "restore command failed"; ha_event dr_restore "$DR_BACKUP_URI"
eval "$DR_VALIDATION_COMMAND" > "$EVIDENCE_DIR/dr-validation.json" || ha_die "post-restore validation command failed"
python3 - "$EVIDENCE_DIR/dr-validation.json" "$EVIDENCE_DIR/dr-result.json" "$(( $(date +%s)-start ))" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); required=('restore_isolated','schema_valid','data_integrity_valid','traffic_valid')
ok=all(x.get(k) is True for k in required)
json.dump({'result':'PASS' if ok else 'FAIL','restore_seconds':int(sys.argv[3]),'evidence':x},open(sys.argv[2],'w'),indent=2)
if not ok: raise SystemExit(1)
PY
