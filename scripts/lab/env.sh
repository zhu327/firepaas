# firepaas 单机实验室环境变量。用法: source scripts/lab/env.sh
LAB_ROOT="${LAB_ROOT:-${FIREPAAS_LAB_ROOT:-$HOME/.local/firepaas-lab}}"
export PATH="$LAB_ROOT/bin:$LAB_ROOT/go/bin:$PATH"
export NOMAD_ADDR="${NOMAD_ADDR:-http://127.0.0.1:4646}"
export CONSUL_HTTP_ADDR="${CONSUL_HTTP_ADDR:-http://127.0.0.1:8500}"
export FIREPAAS_LAB_CONFIG="${FIREPAAS_LAB_CONFIG:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
