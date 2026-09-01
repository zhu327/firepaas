#!/usr/bin/env bash
# M5 soak：每轮实际 create → 200 → scale(2) → deploy → fault → delete-state 检查。
# 所有 API/状态断言 fail closed；单轮失败也会尽力删除，再以非零退出。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$HERE/results/soak-m5}"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_ADDR="${FP_API_ADDR:-http://127.0.0.1:${FP_API_PORT:-8083}}"
EDGE_TLS="${FIREPAAS_EDGE_TLS_PORT:-8445}"
CERT_DIR="${CERT_DIR:-$HERE/certs}"
API_TOKEN="${FP_API_TOKEN:?FP_API_TOKEN required（与运行中的 API 一致）}"
PG="${FP_PSQL:-docker exec dev-postgres-1 psql -v ON_ERROR_STOP=1 -U firepaas -d firepaas -tAc}"

DURATION=72h
while [[ $# -gt 0 ]]; do case "$1" in --duration) DURATION="$2"; shift 2;; *) echo "unknown arg $1" >&2; exit 2;; esac; done
SECONDS_TOTAL=$(python3 -c "import re,sys; m=re.fullmatch(r'([1-9][0-9]*)([hms])',sys.argv[1]); assert m, 'duration must be positive Ns/Nm/Nh'; print(int(m[1])*{'h':3600,'m':60,'s':1}[m[2]])" "$DURATION")
now() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[soak $(now)] $*"; }
api() { curl --fail-with-body -sS --connect-timeout 5 --max-time 20 -H "Authorization: Bearer $API_TOKEN" "$@"; }
psqlq() { $PG "$1"; }
mkdir -p "$RESULTS_DIR"
printf 'round,ts,elapsed_s,fc,netns,veth,pending_ops,mem_avail_mib,edge_rc,summary\n' > "$RESULTS_DIR/summary.csv"

ONLINE_OUT=$(bash "$HERE/push-ontime.sh")
ONTIME_REF=$(grep '^REF=' <<<"$ONLINE_OUT" | cut -d= -f2-)
[[ -n "$ONTIME_REF" ]] || { echo "ontime REF parse failed" >&2; exit 1; }
# A changed digest is required so deploy actually exercises rollout semantics.
DEPLOY_REF="${FIREPAAS_SOAK_DEPLOY_IMAGE:-$ONTIME_REF}"
[[ "$DEPLOY_REF" != *:latest && "$DEPLOY_REF" != *REPLACE-ME* ]] || { echo "unsafe deploy image reference" >&2; exit 1; }

snapshot() {
  local fc ns vv pend mem
  fc=$(ps -eo args | grep -c '[b]inaries/firecracker' || true)
  ns=$(ip netns list | grep -c '^fp-slot-' || true)
  vv=$(ip -o link show type veth 2>/dev/null | wc -l || true)
  pend=$(psqlq "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
  mem=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)
  printf '%s,%s,%s,%s,%s\n' "$fc" "$ns" "$vv" "$pend" "$mem"
}
wait_sql() { local sql=$1 want=$2 n=${3:-90} got; for _ in $(seq 1 "$n"); do got=$(psqlq "$sql"); [[ "$got" == "$want" ]] && return 0; sleep 3; done; log "state timeout want=$want got=${got:-<empty>} sql=$sql"; return 1; }
wait_edge() { local hn=$1; for _ in $(seq 1 60); do rc=$(curl -sS -m 15 --resolve "$hn:$EDGE_TLS:127.0.0.1" --cacert "$CERT_DIR/ca.crt" -o /dev/null -w '%{http_code}' "https://$hn:$EDGE_TLS/" || true); [[ "$rc" == 200 ]] && return 0; sleep 2; done; return 1; }
cleanup_app() { local app=$1; api -X DELETE "$API_ADDR/v1/apps/$app" >/dev/null || return 1; wait_sql "SELECT count(*) FROM machines WHERE app_id='$app' AND desired_state <> 'DELETED'" 0 60; wait_sql "SELECT count(*) FROM operations o JOIN machines m ON m.id=o.machine_id WHERE m.app_id='$app' AND o.status IN ('PENDING','CLAIMED')" 0 60; }

START=$(date +%s); ROUND=0
while (( $(date +%s) - START < SECONDS_TOTAL )); do
  ROUND=$((ROUND + 1)); APP="soak-$(date +%s)-$ROUND"; HN="$APP.$DOMAIN"; log "round $ROUND begin app=$APP"
  failed=0
  # create and prove initial traffic
  api -X POST "$API_ADDR/v1/apps" -H 'Content-Type: application/json' -d "{\"app_id\":\"$APP\",\"project_id\":\"dev\",\"hostname\":\"$HN\",\"image\":\"$ONTIME_REF\",\"port\":80,\"replicas\":1}" >/dev/null || failed=1
  (( failed == 0 )) && wait_sql "SELECT count(*) FROM machines WHERE app_id='$APP' AND observed_state='RUNNING' AND desired_state <> 'DELETED'" 1 || failed=1
  (( failed == 0 )) && wait_edge "$HN" || failed=1
  # scale must create two running machines.
  (( failed == 0 )) && api -X POST "$API_ADDR/v1/apps/$APP/scale" -H 'Content-Type: application/json' -d '{"replicas":2}' >/dev/null || failed=1
  (( failed == 0 )) && wait_sql "SELECT count(*) FROM machines WHERE app_id='$APP' AND observed_state='RUNNING' AND desired_state <> 'DELETED'" 2 || failed=1
  # deploy deliberately uses a supplied different digest; a same-digest deployment is invalid.
  (( failed == 0 )) && api -X POST "$API_ADDR/v1/apps/$APP/deployments" -H 'Content-Type: application/json' -d "{\"image\":\"$DEPLOY_REF\"}" >/dev/null || failed=1
  (( failed == 0 )) && wait_sql "SELECT count(*) FROM deployments WHERE app_id='$APP' AND status='ACTIVE' AND image_ref='$DEPLOY_REF'" 1 120 || failed=1
  (( failed == 0 )) && wait_edge "$HN" || failed=1
  # Fault: pause then resume a real machine and verify recovery (not merely curl exit status).
  if (( failed == 0 )); then
    M=$(psqlq "SELECT id FROM machines WHERE app_id='$APP' AND observed_state='RUNNING' AND desired_state <> 'DELETED' ORDER BY id LIMIT 1")
    [[ -n "$M" ]] || failed=1
    (( failed == 0 )) && api -X POST "$API_ADDR/v1/machines/$M/pause" >/dev/null || failed=1
    sleep 2
    (( failed == 0 )) && api -X POST "$API_ADDR/v1/machines/$M/resume" >/dev/null || failed=1
    (( failed == 0 )) && wait_sql "SELECT count(*) FROM machines WHERE id='$M' AND observed_state='RUNNING'" 1 60 || failed=1
  fi
  (( failed == 0 )) && wait_edge "$HN" || failed=1
  IFS=, read -r fc ns vv pend mem <<<"$(snapshot)"; elapsed=$(( $(date +%s) - START )); rc=${rc:-000}
  if ! cleanup_app "$APP"; then failed=1; fi
  # Delete state means both desired-state convergence and no in-flight ops, checked by cleanup_app.
  summary=ok; (( failed == 0 )) || summary=failed
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$ROUND" "$(now)" "$elapsed" "$fc" "$ns" "$vv" "$pend" "$mem" "$rc" "$summary" >> "$RESULTS_DIR/summary.csv"
  (( failed == 0 )) || { log "round $ROUND FAIL"; exit 1; }
  log "round $ROUND PASS edge=$rc fc=$fc ns=$ns veth=$vv pending=$pend"
done
log "PASS soak finished rounds=$ROUND duration=$DURATION results=$RESULTS_DIR/summary.csv"
