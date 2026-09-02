#!/usr/bin/env bash
# Validate all Nomad jobs and assert that lab path variables render literally.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NOMAD_BIN="${NOMAD_BIN:-nomad}"
# Keep validation independent from any developer/lab cluster. Nomad still performs
# local schema validation and only warns that driver validation was skipped.
export NOMAD_ADDR="${NOMAD_VALIDATE_ADDR:-http://127.0.0.1:9}"

cd "$ROOT_DIR"
"$NOMAD_BIN" fmt -check iac/nomad
"$NOMAD_BIN" job validate -var-file=iac/nomad/ci/agent.vars.hcl iac/nomad/agent.hcl
"$NOMAD_BIN" job validate iac/nomad/agentd-single.hcl
"$NOMAD_BIN" job validate \
  -var="repo_root=/tmp/firepaas" -var="lab_bin=/tmp/firepaas-lab-bin" \
  iac/nomad/hypeman-p0.hcl
"$NOMAD_BIN" job validate -var-file=iac/nomad/ci/control-plane.vars.hcl iac/nomad/control-plane.hcl
"$NOMAD_BIN" job validate -var-file=iac/nomad/ci/edge.vars.hcl iac/nomad/edge.hcl

repo_root=/tmp/firepaas-render-check
lab_bin=/tmp/firepaas-lab-bin
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT
"$NOMAD_BIN" job run -output \
  -var="repo_root=$repo_root" -var="lab_bin=$lab_bin" -var="agentd_binary_sha256=test-digest" \
  iac/nomad/agentd-single.hcl >"$rendered"
python3 - "$rendered" "$lab_bin/agentd" "$repo_root/scripts/lab/agentd.yaml" <<'PY'
import json
import sys

job = json.load(open(sys.argv[1], encoding="utf-8"))
job = job.get("Job", job)
task = job["TaskGroups"][0]["Tasks"][0]
actual_command = task["Config"]["command"]
actual_config = task["Env"]["CONFIG_PATH"]
actual_digest = task["Env"]["FIREPAAS_BUILD_SHA256"]
if actual_command != sys.argv[2] or actual_config != sys.argv[3] or actual_digest != "test-digest":
    raise SystemExit(
        f"agentd rendering mismatch: command={actual_command!r}, config={actual_config!r}, digest={actual_digest!r}"
    )
PY

"$NOMAD_BIN" job run -output \
  -var="repo_root=$repo_root" -var="lab_bin=$lab_bin" \
  iac/nomad/hypeman-p0.hcl >"$rendered"
python3 - "$rendered" "$lab_bin/hypeman" "$repo_root/scripts/lab/hypeman-p0.yaml" <<'PY'
import json
import sys

job = json.load(open(sys.argv[1], encoding="utf-8"))
job = job.get("Job", job)
task = job["TaskGroups"][0]["Tasks"][0]
actual_command = task["Config"]["command"]
actual_config = task["Env"]["CONFIG_PATH"]
if actual_command != sys.argv[2] or actual_config != sys.argv[3]:
    raise SystemExit(
        f"hypeman rendering mismatch: command={actual_command!r}, config={actual_config!r}"
    )
PY
