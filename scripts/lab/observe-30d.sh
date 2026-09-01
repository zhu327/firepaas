#!/usr/bin/env bash
# Daily 30-day observation gate. State is append-only; missed days fail rather than being backfilled.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/ha-lib.sh"
ha_check_topology; ha_require EVIDENCE_DIR DAILY_OBSERVATION_COMMAND SLO_SPEC
[[ "${OBSERVATION_DAYS:-30}" == "30" ]] || ha_die "the observation gate is exactly 30 days; OBSERVATION_DAYS overrides are forbidden"
DAYS=30
"$HERE/capture-evidence.sh"; obs="$EVIDENCE_DIR/30d-observations.jsonl"; : > "$obs"; previous=0
for day in $(seq 1 "$DAYS"); do
 start=$(date +%s); eval "$DAILY_OBSERVATION_COMMAND" >> "$obs" || ha_die "day $day observation failed"
 now=$(date +%s); (( previous == 0 || now-previous >= 86400 )) || ha_die "day $day was recorded too soon; cannot compress 30-day gate"
 previous=$now
 if [[ "$day" != "$DAYS" ]]; then
   interval=${OBSERVATION_INTERVAL_SECONDS:-86400}; (( interval >= 86400 )) || ha_die "OBSERVATION_INTERVAL_SECONDS must be at least 86400; cannot compress 30-day gate"
   sleep "$interval"
 fi
done
python3 "$HERE/assert-slo.py" --spec "$SLO_SPEC" --observations "$obs" --output "$EVIDENCE_DIR/30d-slo-result.json"
python3 - "$EVIDENCE_DIR/30d-result.json" "$DAYS" <<'PY'
import json,sys
json.dump({'result':'PASS','observation_days':int(sys.argv[2]),'note':'PASS applies only to the captured observation period.'},open(sys.argv[1],'w'),indent=2)
PY
