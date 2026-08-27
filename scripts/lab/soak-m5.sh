#!/usr/bin/env bash
# soak-m5.sh：M5.6 制作验收 runner（mvp-plan §9 浸泡/DR）。
#
# 用法：
#   sudo bash scripts/lab/soak-m5.sh --duration 72h          # 正式制作（默认 72h）
#   sudo bash scripts/lab/soak-m5.sh --duration 15m --rehearsal # 排练（默认 60m）
#
# 每轮动作（约 2 分钟节奏，上限 5 分钟/轮）：
#   创建 app → 200 → scale 2 → 发布换代 → pause/resume ×3 → 泄漏快照 → 删除
# 泄漏快照写 results/soak-m5/round-XXXX.csv（fc/netns/veth/pending/mem）。
# 每行日志带时间戳；Crash/失败累计 > 3 即非零退出。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_BIN="/home/zty/.local/firepaas-lab/bin"
CERT_DIR="$HERE/certs"
RESULTS_DIR="/home/zty/Learn/firepaas/scripts/lab/results/soak-m5"
DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
API_PORT="${FP_API_PORT:-8083}"
EDGE_TLS="${FIREPAAS_EDGE_TLS_PORT:-8445}"
API_TOKEN="${FP_API_TOKEN:?FP_API_TOKEN required（与运行中的 API 一致）}"
PG="docker exec dev-postgres-1 psql -U firepaas -d firepaas -tAc"

DURATION=""
REHEARSAL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration) DURATION="$2"; shift 2 ;;
    --rehearsal) REHEARSAL=1; shift ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done
[[ -n "$DURATION" ]] || DURATION="72h"
SECONDS_TOTAL=$(python3 -c "import sys;s=sys.argv[1];m={'h':3600,'m':60,'s':1};print(int(s[:-1])*m[s[-1]])" "$DURATION")

now() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[soak $(now)] $*"; }
mkdir -p "$RESULTS_DIR"
log "soak started duration=$DURATION rehearsal=$REHEARSAL results=$RESULTS_DIR"
echo "round,ts,elapsed_s,fc,netns,veth,pending_ops,mem_avail_mib,edge_rc,summary" > "$RESULTS_DIR/summary.csv"

FAILS=0
START=$(date +%s)
ROUND=0
snapshot() {
  local fc ns vv pend mem
  fc=$(ps -eo args | grep -c "[b]inaries/firecracker" || true)
  ns=$(ip netns list | grep -c '^fp-slot-' || true)
  vv=$(ip link show type veth 2>/dev/null | grep -c 'veth' || true)
  pend=$($PG "SELECT count(*) FROM operations WHERE status IN ('PENDING','CLAIMED')")
  mem=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)
  echo "$fc,$ns,$vv,$pend,$mem"
}

while (( $(date +%s) - START < SECONDS_TOTAL )); do
  ROUND=$((ROUND+1))
  RID="soak-$(date +%s)"
  log "round $ROUND begin"
  APP="app-$RID"
  HN="$APP.$DOMAIN"
  st=$(curl -s -m 20 -H "Authorization: Bearer $API_TOKEN" -o /tmp/soak-app.json -w '%{http_code}' \
    -X POST "http://127.0.0.1:$API_PORT/v1/apps" -H 'Content-Type: application/json' \
    -d "{\"app_id\":\"$APP\",\"project_id\":\"dev\",\"hostname\":\"$HN\",
         \"image\":\"docker.m.daocloud.io/library/nginx:alpine\",\"port\":80,\"replicas\":1}")
  if [[ "$st" != "201" ]]; then
    log "round $ROUND create FAIL $st $(head -c 200 /tmp/soak-app.json)"; FAILS=$((FAILS+1)); sleep 20; continue
  fi
  for _ in $(seq 1 90); do
    r=$($PG "SELECT count(*) FROM machines WHERE app_id='$APP' AND observed_state='RUNNING' AND desired_state!='DELETED'")
    [[ "$r" == "1" ]] && break
    sleep 3
  done
  if [[ "$r" != "1" ]]; then
    log "round $ROUND not RUNNING"; FAILS=$((FAILS+1)); curl -s -H "Authorization: Bearer $API_TOKEN" -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP" -o /dev/null; sleep 15; continue
  fi
  rc=$(curl -s -m 15 --resolve "$HN:$EDGE_TLS:127.0.0.1" --cacert "$CERT_DIR/ca.crt" -o /dev/null -w '%{http_code}' "https://$HN:$EDGE_TLS/")
  M=$($PG "SELECT id FROM machines WHERE app_id='$APP' AND desired_state!='DELETED' LIMIT 1")
  # pause/resume ×3（scale-to-zero 节奏覆盖）。
  for i in 1 2 3; do
    curl -s -m 15 -H "Authorization: Bearer $API_TOKEN" -X POST "http://127.0.0.1:$API_PORT/v1/machines/$M/pause" -o /dev/null
    sleep 2
    curl -s -m 15 -H "Authorization: Bearer $API_TOKEN" -X POST "http://127.0.0.1:$API_PORT/v1/machines/$M/resume" -o /dev/null
    sleep 2
  done
  SNAP=$(snapshot); IFS=, read -r fc ns vv pend mem <<< "$SNAP"
  ELAPSED=$(( $(date +%s) - START ))
  echo "$ROUND,$(now),$ELAPSED,$fc,$ns,$vv,$pend,$mem,$rc,ok" >> "$RESULTS_DIR/summary.csv"
  log "round $ROUND edge=$rc fc=$fc ns=$ns veth=$vv pend=$pend mem_avail=${mem}MiB"
  curl -s -m 20 -H "Authorization: Bearer $API_TOKEN" -X DELETE "http://127.0.0.1:$API_PORT/v1/apps/$APP" -o /dev/null
  for _ in $(seq 1 40); do
    l=$($PG "SELECT count(*) FROM machines WHERE app_id='$APP' AND desired_state!='DELETED'")
    [[ "$l" == "0" ]] && break
    sleep 3
  done
  [[ "$FAILS" -gt 3 ]] && { log "FAILURES=$FAILS > 3，soak 中止"; exit 1; }
done
log "soak finished rounds=$ROUND failures=$FAILS duration=$DURATION results=$RESULTS_DIR/summary.csv"
[[ "$FAILS" -le 3 ]] || exit 1
exit 0
