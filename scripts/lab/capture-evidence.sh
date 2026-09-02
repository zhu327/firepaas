#!/usr/bin/env bash
# Capture immutable, environment-linked evidence. Does not imply an acceptance result.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ha-lib.sh
source "$HERE/ha-lib.sh"
ha_require EVIDENCE_DIR FIREPAAS_CONFIG_PATHS
ha_require_cmd git; ha_require_cmd sha256sum; ha_require_cmd python3
REPO_ROOT="${FIREPAAS_REPO_ROOT:-$(cd "$HERE/../.." && pwd)}"
RUN_ID="${RUN_ID:-}"
if [[ -e "$EVIDENCE_DIR" ]]; then
  [[ -f "$EVIDENCE_DIR/manifest.json" && -f "$EVIDENCE_DIR/SHA256SUMS" ]] || ha_die "existing EVIDENCE_DIR is not a captured evidence directory"
  (cd "$EVIDENCE_DIR" && sha256sum -c SHA256SUMS >/dev/null) || ha_die "existing evidence metadata or configuration changed"
  python3 - "$EVIDENCE_DIR/manifest.json" "$RUN_ID" "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null)" <<'PY' || ha_die "existing evidence belongs to another run or commit"
import json, sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
run_matches = not sys.argv[2] or manifest.get("run_id") == sys.argv[2]
raise SystemExit(0 if run_matches and manifest.get("git_commit") == sys.argv[3] else 1)
PY
  ha_log "verified existing evidence metadata in $EVIDENCE_DIR"
  exit 0
fi
RUN_ID="${RUN_ID:-$(ha_run_id)}"
mkdir -p "$EVIDENCE_DIR/config"
TOPOLOGY_FILE="${FIREPAAS_TOPOLOGY_FILE:-}"
[[ -n "$TOPOLOGY_FILE" && -r "$TOPOLOGY_FILE" ]] || ha_die "FIREPAAS_TOPOLOGY_FILE must be a readable topology inventory (no secrets)"
case "${FIREPAAS_CONFIG_PATHS,,}" in
  *secret*|*credential*|*private*|*token*) ha_die "refuse to archive paths named secret/credential/private/token" ;;
esac
COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null) || ha_die "repository commit unavailable"
[[ -z "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=all)" ]] || ha_die "working tree is dirty; commit or explicitly produce a clean build first"
IFS=',' read -r -a paths <<<"$FIREPAAS_CONFIG_PATHS"
declare -A copied_names=()
for path in "${paths[@]}"; do
  [[ -n "$path" && -f "$path" && ! -L "$path" ]] || ha_die "config path must be a readable regular non-symlink file: $path"
  base=$(basename "$path")
  [[ -z "${copied_names[$base]:-}" ]] || ha_die "duplicate config basename would overwrite evidence: $base"
  copied_names[$base]=1
  cp -- "$path" "$EVIDENCE_DIR/config/$base"
done
cp -- "$TOPOLOGY_FILE" "$EVIDENCE_DIR/topology.txt"
python3 - "$EVIDENCE_DIR/manifest.json" "$RUN_ID" "$COMMIT" <<'PY'
import json, os, platform, sys, datetime
out, run_id, commit = sys.argv[1:]
keys = ('COMPUTE_HOSTS','EDGE_HOSTS','CONTROL_HOSTS','VIP_ADDRESS','WORKLOAD_URL','WORKLOAD_HOSTNAME','SLO_SPEC')
with open(out, 'w', encoding='utf-8') as f:
 json.dump({'schema_version':'1.0','run_id':run_id,'captured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'git_commit':commit,'runner':platform.node(),'environment':{k:os.environ.get(k) for k in keys if os.environ.get(k)}},f,indent=2,sort_keys=True)
 f.write('\n')
PY
(
  cd "$EVIDENCE_DIR"
  sha256sum config/* topology.txt manifest.json > SHA256SUMS
)
ha_log "captured evidence metadata in $EVIDENCE_DIR"
