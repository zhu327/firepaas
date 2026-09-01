#!/usr/bin/env bash
# pg-restore-rehearsal.sh：M5.4（mvp-plan §9.4）恢复演练。
#
# 用法：sudo bash scripts/lab/pg-restore-rehearsal.sh [dump.sql.gz]
#   - 最近备份（或指定文件）恢复到同容器内的 scratch 库 firepaas_rehearsal
#   - 跑一遍 db.Migrate（幂等）→ 断言关键表行数一致（apps/machines/operations/
#     secrets/api_keys）→ 删 scratch 库
set -euo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[pg-rehearsal $(TS)] $*"; }

CONTAINER="${PG_CONTAINER:-dev-postgres-1}"
DUMP="${1:-$(ls -1t /var/lib/firepaas-p0/backups/postgres/pg-*.sql.gz 2>/dev/null | head -1)}"
[[ -n "$DUMP" && -f "$DUMP" ]] || { say "no backup found; run pg-backup.sh first"; exit 1; }
SCRATCH="firepaas_rehearsal"
say "rehearsing restore of $DUMP → $SCRATCH"

docker exec "$CONTAINER" psql -U firepaas -d postgres -c \
  "DROP DATABASE IF EXISTS $SCRATCH" >/dev/null
docker exec "$CONTAINER" psql -U firepaas -d postgres -q -c "CREATE DATABASE $SCRATCH"
gunzip -c "$DUMP" | docker exec -i "$CONTAINER" psql -U firepaas -d "$SCRATCH" -q -v ON_ERROR_STOP=1
say "restored; asserting row counts"

ROWS=$(docker exec "$CONTAINER" psql -U firepaas -d "$SCRATCH" -tAc \
  "SELECT 'apps='||(SELECT count(*) FROM apps)||' machines='||(SELECT count(*) FROM machines)
        ||' operations='||(SELECT count(*) FROM operations)
        ||' secrets='||(SELECT count(*) FROM secrets)
        ||' api_keys='||(SELECT count(*) FROM api_keys)")
say "scratch $ROWS"
# P3-17：基线用备份时刻的 sidecar 行数（无 sidecar 的旧备份兑底为活库快照，
# 兼容过渡）；恢复库与活库持续写入无关联，不再比较两者。
RCFILE="${DUMP%.sql.gz}.rowcounts"
if [[ -f "$RCFILE" ]]; then
  EXPECTED=$(cat "$RCFILE")
else
  say "WARN 备份无 .rowcounts sidecar（旧备份）；退化为活库对比（soak 期间可能漂移）"
  EXPECTED=$(docker exec "$CONTAINER" psql -U firepaas -d firepaas -tAc \
    "SELECT 'apps='||(SELECT count(*) FROM apps)||' machines='||(SELECT count(*) FROM machines)
          ||' operations='||(SELECT count(*) FROM operations)
          ||' secrets='||(SELECT count(*) FROM secrets)
          ||' api_keys='||(SELECT count(*) FROM api_keys)")
fi
say "expect  $EXPECTED"
[[ "$ROWS" == "$EXPECTED" ]] || { say "FAIL row count mismatch"; docker exec "$CONTAINER" psql -U firepaas -d postgres -c "DROP DATABASE $SCRATCH" >/dev/null; exit 1; }
say "PASS 备份可恢复且行数与备份时刻一致"

docker exec "$CONTAINER" psql -U firepaas -d postgres -c "DROP DATABASE $SCRATCH" >/dev/null
say "scratch db dropped"
