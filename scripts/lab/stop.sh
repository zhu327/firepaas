#!/usr/bin/env bash
# 停止单机实验室进程。用法: bash scripts/lab/stop.sh
# 普通用户只能停用户态 Nomad/Consul；root 运行的 Nomad 需要 sudo bash scripts/lab/stop.sh。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

stop_one() {
  local name="$1" pidfile="$2"
  if [[ ! -f "$pidfile" ]]; then
    echo "==> $name not running (no pidfile)"
    return 0
  fi
  local pid
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "==> Stopping $name (pid $pid)"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 15); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
  else
    echo "==> $name already stopped"
  fi
  rm -f "$pidfile"
}

if [[ $EUID -eq 0 ]]; then
  stop_one "Nomad(root)" "/var/lib/firepaas-p0/nomad/nomad.pid"
  stop_one "Nomad(user)" "$LAB_ROOT/run/nomad/nomad.pid"
else
  stop_one "Nomad(user)" "$LAB_ROOT/run/nomad/nomad.pid"
  if [[ -f "/var/lib/firepaas-p0/nomad/nomad.pid" ]]; then
    echo "==> 检测到 root Nomad pidfile，请用: sudo bash scripts/lab/stop.sh"
  fi
fi
stop_one "Consul" "$LAB_ROOT/run/consul/consul.pid"
echo "==> Done"
