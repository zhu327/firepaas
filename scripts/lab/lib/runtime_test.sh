#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
LAB_BIN="$TMP"
API_PORT=8080
API_TOKEN=test-token
calls=0
nomad() { [[ "$*" == "job restart -on-error fail firepaas-agentd" ]]; }
cat >"$LAB_BIN/agentctl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$LAB_BIN/agentctl"
authed_raw() { printf '%s\0' "$@" >"$TMP/request"; }
# shellcheck source=runtime.sh
source "$HERE/runtime.sh"
lab_restart_agentd 1 0
lab_guest_exec machine-1 /bin/sh -c 'echo hello world'
python3 - "$TMP/request" <<'PY'
import json, pathlib, sys
args = pathlib.Path(sys.argv[1]).read_bytes().decode().rstrip('\0').split('\0')
assert args[:3] == ['-X', 'POST', 'http://127.0.0.1:8080/v1/machines/machine-1/exec']
payload = json.loads(args[args.index('-d') + 1])
assert payload['command'] == ['/bin/sh', '-c', 'echo hello world']
assert payload['operation_id']
PY
encoded=$(printf hello | base64 -w0)
printf '{"stdout":"%s"}\n{"exit_code":0}\n' "$encoded" | lab_exec_stdout | grep -qx hello
if printf '{"exit_code":7}\n' | lab_exec_stdout; then
  echo 'expected nonzero stream to fail' >&2
  exit 1
fi
