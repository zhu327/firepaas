#!/usr/bin/env bash
# Shared helpers for single-node runtime E2E scenarios. Callers provide
# LAB_BIN, API_PORT, API_TOKEN and authed_raw.

lab_restart_agentd() {
  local attempts="${1:-45}" interval="${2:-2}"
  nomad job restart -on-error fail firepaas-agentd >/dev/null 2>&1 || return 1
  for ((attempt = 0; attempt < attempts; attempt++)); do
    "$LAB_BIN/agentctl" info >/dev/null 2>&1 && return 0
    sleep "$interval"
  done
  return 1
}

lab_guest_exec() {
  local machine="$1"
  shift
  local operation_id="exec-$RANDOM-$RANDOM"
  local payload
  payload=$(python3 -c 'import json,sys; print(json.dumps({"command": list(sys.argv[2:]), "operation_id": sys.argv[1]}))' "$operation_id" "$@") || return
  authed_raw -X POST "http://127.0.0.1:$API_PORT/v1/machines/$machine/exec" \
    -H 'Content-Type: application/json' -d "$payload"
}

lab_exec_stdout() {
  python3 -c 'import base64,json,sys
chunks=[]; rc=None
for line in sys.stdin:
    if not line.strip(): continue
    obj=json.loads(line)
    if "stdout" in obj: chunks.append(base64.b64decode(obj["stdout"]).decode("utf-8","replace"))
    if "exit_code" in obj: rc=obj["exit_code"]
sys.stdout.write("".join(chunks))
raise SystemExit(0 if rc == 0 else (rc if isinstance(rc,int) else 1))'
}
