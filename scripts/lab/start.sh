#!/usr/bin/env bash
# 启动单机 Nomad 实验室（可选 Consul）。用法: bash scripts/lab/start.sh [--with-consul]
# 幂等：已运行时直接复用。Nomad 以普通用户运行；raw_exec job 的运行需要 root（见 README）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

WITH_CONSUL=0
[[ "${1:-}" == "--with-consul" ]] && WITH_CONSUL=1

if [[ $EUID -eq 0 ]]; then
  NOMAD_RUN="/var/lib/firepaas-p0/nomad"
else
  NOMAD_RUN="$LAB_ROOT/run/nomad"
fi
NOMAD_PIDFILE="$NOMAD_RUN/nomad.pid"
NOMAD_LOG="$NOMAD_RUN/agent.log"
CONSUL_RUN="$LAB_ROOT/run/consul"
CONSUL_PIDFILE="$CONSUL_RUN/consul.pid"
CONSUL_LOG="$CONSUL_RUN/agent.log"

mkdir -p "$NOMAD_RUN" "$CONSUL_RUN"

start_nomad() {
  if [[ -f "$NOMAD_PIDFILE" ]] && kill -0 "$(cat "$NOMAD_PIDFILE")" 2>/dev/null; then
    echo "==> Nomad already running (pid $(cat "$NOMAD_PIDFILE"))"
    return 0
  fi
  echo "==> Starting Nomad (single-node, node_pool=compute, euid=$EUID)"
  nohup nomad agent -config="$HERE/nomad-single.hcl" -data-dir="$NOMAD_RUN" >>"$NOMAD_LOG" 2>&1 &
  echo $! > "$NOMAD_PIDFILE"

  echo "==> Waiting for Nomad leader"
  for i in $(seq 1 60); do
    if curl -fsS "$NOMAD_ADDR/v1/status/leader" 2>/dev/null | grep -q '"'; then
      break
    fi
    if ! kill -0 "$(cat "$NOMAD_PIDFILE")" 2>/dev/null; then
      echo "ERROR: Nomad exited during startup. tail of $NOMAD_LOG:" >&2
      tail -40 "$NOMAD_LOG" >&2
      exit 1
    fi
    sleep 1
  done
  if ! curl -fsS "$NOMAD_ADDR/v1/status/leader" 2>/dev/null | grep -q '"'; then
    echo "ERROR: Nomad did not elect a leader within 60s. tail of $NOMAD_LOG:" >&2
    tail -40 "$NOMAD_LOG" >&2
    exit 1
  fi
  echo "==> Nomad ready: $NOMAD_ADDR"
}

ensure_compute_pool() {
  echo "==> Applying node pool 'compute'"
  nomad node pool apply "$(cd "$HERE/../.." && pwd)/iac/nomad/pools/compute.hcl"
}

start_consul() {
  if [[ -f "$CONSUL_PIDFILE" ]] && kill -0 "$(cat "$CONSUL_PIDFILE")" 2>/dev/null; then
    echo "==> Consul already running (pid $(cat "$CONSUL_PIDFILE"))"
    return 0
  fi
  echo "==> Starting Consul (single-node)"
  nohup consul agent -config-file="$HERE/consul-single.hcl" >>"$CONSUL_LOG" 2>&1 &
  echo $! > "$CONSUL_PIDFILE"
  echo "==> Consul starting (HTTP $CONSUL_HTTP_ADDR)"
}

start_nomad
ensure_compute_pool
[[ "$WITH_CONSUL" == 1 ]] && start_consul

echo "==> Done. Next:"
echo "    nomad job validate iac/nomad/hypeman-p0.hcl"
echo "    nomad job plan iac/nomad/hypeman-p0.hcl"
echo "    sudo env PATH=\"$PATH\" NOMAD_ADDR=$NOMAD_ADDR nomad job run iac/nomad/hypeman-p0.hcl"
