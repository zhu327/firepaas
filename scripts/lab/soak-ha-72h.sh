#!/usr/bin/env bash
# Runs continuous HA probes for exactly the requested duration; any failed probe fails the gate.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_check_topology; ha_require EVIDENCE_DIR SOAK_PROBE_COMMAND SLO_SPEC
[[ "${SOAK_DURATION:-72h}" == "72h" ]] || ha_die "the HA soak gate is exactly 72h; SOAK_DURATION overrides are forbidden"
"$HERE/capture-evidence.sh"; seconds=$((72 * 3600)); start=$(date +%s); obs="$EVIDENCE_DIR/soak-observations.jsonl"; : > "$obs"
probe_tmp="$EVIDENCE_DIR/.soak-probe.json"
trap 'rm -f "$probe_tmp"' EXIT
while (( $(date +%s)-start < seconds )); do
  : > "$probe_tmp"
  eval "$SOAK_PROBE_COMMAND" > "$probe_tmp" || ha_die "soak probe command failed"
  # Fail closed: each invocation emits exactly one object. Besides SLO timings,
  # retain the v1.4 safety signals required to diagnose inventory/GC/prewarm.
  python3 - "$probe_tmp" >> "$obs" <<'PY' || ha_die "soak probe output is incomplete or invalid"
import json, math, sys
lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if len(lines) != 1:
    raise SystemExit("probe must emit exactly one JSON object")
row = json.loads(lines[0])
required = (
    "timestamp", "inventory_age_seconds", "inventory_drift_count",
    "scrub_failed_count", "quarantine_active_count", "attachment_drift_count",
    "prewarm_pending_count", "image_pin_active_count",
)
for key in required:
    if key not in row:
        raise SystemExit(f"probe missing {key}")
for key in required[1:]:
    value = row[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise SystemExit(f"probe {key} must be a finite non-negative number")
print(json.dumps(row, separators=(",", ":")))
PY
  remaining=$((seconds - ($(date +%s) - start))); (( remaining > 0 )) || break
  sleep_for=${SOAK_INTERVAL_SECONDS:-60}; (( sleep_for > remaining )) && sleep_for=$remaining
  sleep "$sleep_for"
done
elapsed=$(( $(date +%s) - start )); (( elapsed >= seconds )) || ha_die "soak ended before the required 72h elapsed"
python3 "$HERE/assert-slo.py" --spec "$SLO_SPEC" --observations "$obs" --output "$EVIDENCE_DIR/soak-slo-result.json"
python3 - "$EVIDENCE_DIR/soak-result.json" "$elapsed" <<'PY'
import json,sys
json.dump({'result':'PASS','duration_seconds':int(sys.argv[2]),'note':'PASS means this run completed; it does not assert production acceptance.'},open(sys.argv[1],'w'),indent=2)
PY
