#!/usr/bin/env bash
# Package evidence only after every required result is explicitly PASS.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_require EVIDENCE_DIR ARCHIVE_DIR
[[ -f "$EVIDENCE_DIR/manifest.json" && -f "$EVIDENCE_DIR/SHA256SUMS" ]] || ha_die "capture-evidence.sh has not completed"
RESULT_FILES="${RESULT_FILES:?RESULT_FILES must be comma-separated result JSON files}"
IFS=',' read -r -a results <<<"$RESULT_FILES"
for result in "${results[@]}"; do
 [[ -f "$result" ]] || ha_die "missing result file: $result"
 python3 - "$result" <<'PY'
import json,sys
try: value=json.load(open(sys.argv[1])).get('result')
except Exception as e: raise SystemExit('invalid result JSON: '+str(e))
if value != 'PASS': raise SystemExit('result is not PASS: '+sys.argv[1])
PY
done
mkdir -p "$ARCHIVE_DIR"
name="${RUN_ID:-$(ha_run_id)}"; stage=$(mktemp -d "$ARCHIVE_DIR/.${name}.XXXXXX")
trap 'rm -rf "$stage"' EXIT
cp -a "$EVIDENCE_DIR/." "$stage/"
mkdir -p "$stage/results"; cp -- "${results[@]}" "$stage/results/"
( cd "$stage" && sha256sum $(find . -type f -print | sort) > ARCHIVE-SHA256SUMS )
tar -C "$ARCHIVE_DIR" -czf "$ARCHIVE_DIR/$name.tar.gz" "$(basename "$stage")"
sha256sum "$ARCHIVE_DIR/$name.tar.gz" > "$ARCHIVE_DIR/$name.tar.gz.sha256"
ha_log "archived PASS-only evidence: $ARCHIVE_DIR/$name.tar.gz"
