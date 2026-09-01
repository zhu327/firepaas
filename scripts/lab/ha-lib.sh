#!/usr/bin/env bash
# Shared fail-closed helpers for provisioned multi-node HA validation.
# Source this file; it intentionally has no defaults for topology or credentials.
set -euo pipefail

ha_die() { printf '[ha] FAIL: %s\n' "$*" >&2; exit 1; }
ha_log() { printf '[ha] %s\n' "$*" >&2; }
ha_require() { local v; for v in "$@"; do [[ -n "${!v:-}" ]] || ha_die "required environment variable $v is unset"; done; }
ha_require_cmd() { command -v "$1" >/dev/null 2>&1 || ha_die "required command not found: $1"; }
ha_csv_count() { local value=$1; [[ -n "$value" ]] || { echo 0; return; }; awk -F, '{print NF}' <<<"$value"; }
ha_require_csv_count() { local name=$1 minimum=$2 count; ha_require "$name"; count=$(ha_csv_count "${!name}"); (( count >= minimum )) || ha_die "$name needs at least $minimum comma-separated entries (got $count)"; }
ha_csv_items() { tr ',' '\n' <<<"$1" | sed '/^[[:space:]]*$/d'; }
ha_require_unique_csv() { local name=$1 values duplicate; ha_require "$name"; values=$(ha_csv_items "${!name}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'); duplicate=$(printf '%s\n' "$values" | sort | uniq -d); [[ -z "$duplicate" ]] || ha_die "$name contains duplicate topology entries: $duplicate"; }
ha_require_disjoint_csv() { local left=$1 right=$2 overlap; overlap=$(comm -12 <(ha_csv_items "${!left}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sort -u) <(ha_csv_items "${!right}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sort -u)); [[ -z "$overlap" ]] || ha_die "$left and $right overlap: $overlap"; }
ha_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
ha_run_id() { date -u +%Y%m%dT%H%M%SZ; }
ha_remote() { # host command...; SSH options must be provided explicitly by operator.
  local host=$1; shift
  ha_require SSH_USER SSH_IDENTITY_FILE
  [[ -r "$SSH_IDENTITY_FILE" ]] || ha_die "SSH_IDENTITY_FILE is not readable"
  ssh -o BatchMode=yes -o ConnectTimeout="${SSH_CONNECT_TIMEOUT:-10}" -i "$SSH_IDENTITY_FILE" \
    "${SSH_USER}@${host}" "$@"
}
ha_http_code() { # URL [Host header]
  local url=$1 host=${2:-}
  ha_require_cmd curl
  if [[ -n "$host" ]]; then curl -sS --connect-timeout 5 --max-time 15 -o /dev/null -w '%{http_code}' -H "Host: $host" "$url"; else curl -sS --connect-timeout 5 --max-time 15 -o /dev/null -w '%{http_code}' "$url"; fi
}
ha_wait_http_200() { local url=$1 host=${2:-} timeout=${3:-120} start code; start=$(date +%s)
  while (( $(date +%s) - start < timeout )); do code=$(ha_http_code "$url" "$host" || true); [[ "$code" == 200 ]] && return 0; sleep 2; done
  ha_die "did not receive HTTP 200 within ${timeout}s: $url (last=$code)"
}
ha_event() { # event name detail; requires EVIDENCE_DIR
  ha_require EVIDENCE_DIR; mkdir -p "$EVIDENCE_DIR"
  python3 - "$EVIDENCE_DIR/events.jsonl" "$1" "$2" <<'PY'
import json, sys, datetime
with open(sys.argv[1], 'a', encoding='utf-8') as f:
    f.write(json.dumps({'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'event': sys.argv[2], 'detail': sys.argv[3]})+'\n')
PY
}
ha_check_topology() {
  ha_require_csv_count COMPUTE_HOSTS 2
  ha_require_csv_count EDGE_HOSTS 2
  ha_require_csv_count CONTROL_HOSTS 3
  ha_require_unique_csv COMPUTE_HOSTS
  ha_require_unique_csv EDGE_HOSTS
  ha_require_unique_csv CONTROL_HOSTS
  ha_require_disjoint_csv COMPUTE_HOSTS EDGE_HOSTS
  ha_require_disjoint_csv COMPUTE_HOSTS CONTROL_HOSTS
  ha_require_disjoint_csv EDGE_HOSTS CONTROL_HOSTS
  ha_require VIP_ADDRESS WORKLOAD_URL WORKLOAD_HOSTNAME SSH_USER SSH_IDENTITY_FILE
  [[ -r "$SSH_IDENTITY_FILE" ]] || ha_die "SSH_IDENTITY_FILE is not readable"
  local host
  while read -r host; do
    [[ -n "$host" ]] || continue
    ha_remote "$host" true >/dev/null || ha_die "topology host is unreachable: $host"
  done < <(printf '%s\n%s\n%s\n' "$COMPUTE_HOSTS" "$EDGE_HOSTS" "$CONTROL_HOSTS" | tr ',' '\n')
  [[ -n "${FIREPAAS_CONFIG_PATHS:-}" ]] || ha_die "FIREPAAS_CONFIG_PATHS must identify deployed configuration files"
}
