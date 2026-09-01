#!/usr/bin/env bash
# Capture immutable, environment-linked evidence. Does not imply an acceptance result.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ha-lib.sh
source "$HERE/ha-lib.sh"
ha_require EVIDENCE_DIR FIREPAAS_CONFIG_PATHS
ha_require_cmd git; ha_require_cmd sha256sum; ha_require_cmd python3
mkdir -p "$EVIDENCE_DIR/config"
REPO_ROOT="${FIREPAAS_REPO_ROOT:-$(cd "$HERE/../.." && pwd)}"
RUN_ID="${RUN_ID:-$(ha_run_id)}"
TOPOLOGY_FILE="${FIREPAAS_TOPOLOGY_FILE:-}"
[[ -n "$TOPOLOGY_FILE" && -r "$TOPOLOGY_FILE" ]] || ha_die "FIREPAAS_TOPOLOGY_FILE must be a readable topology inventory (no secrets)"
[[ "$FIREPAAS_CONFIG_PATHS" != *secret* && "$FIREPAAS_CONFIG_PATHS" != *credential* ]] || ha_die "refuse to archive paths named secret/credential"
COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null) || ha_die "repository commit unavailable"
git -C "$REPO_ROOT" diff --quiet || ha_die "working tree is dirty; commit or explicitly produce a clean build first"
IFS=',' read -r -a paths <<<"$FIREPAAS_CONFIG_PATHS"
for path in "${paths[@]}"; do
  [[ -n "$path" && -f "$path" ]] || ha_die "config path is not a readable file: $path"
  base=$(basename "$path")
  cp -- "$path" "$EVIDENCE_DIR/config/$base"
done
cp -- "$TOPOLOGY_FILE" "$EVIDENCE_DIR/topology.txt"
sha256sum "$EVIDENCE_DIR"/config/* "$EVIDENCE_DIR/topology.txt" > "$EVIDENCE_DIR/SHA256SUMS"
python3 - "$EVIDENCE_DIR/manifest.json" "$RUN_ID" "$COMMIT" <<'PY'
import json, os, platform, sys, datetime
out, run_id, commit = sys.argv[1:]
keys = ('COMPUTE_HOSTS','EDGE_HOSTS','CONTROL_HOSTS','VIP_ADDRESS','WORKLOAD_URL','WORKLOAD_HOSTNAME','SLO_SPEC')
with open(out, 'w', encoding='utf-8') as f:
 json.dump({'schema_version':'1.0','run_id':run_id,'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'git_commit':commit,'runner':platform.node(),'environment':{k:os.environ.get(k) for k in keys if os.environ.get(k)}},f,indent=2,sort_keys=True)
 f.write('\n')
PY
ha_log "captured evidence metadata in $EVIDENCE_DIR"
