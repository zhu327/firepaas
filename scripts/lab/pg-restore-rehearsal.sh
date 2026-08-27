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
PROD_ROWS=$(docker exec "$CONTAINER" psql -U firepaas -d firepaas -tAc \
  "SELECT 'apps='||(SELECT count(*) FROM apps)||' machines='||(SELECT count(*) FROM machines)
        ||' operations='||(SELECT count(*) FROM operations)
        ||' secrets='||(SELECT count(*) FROM secrets)
        ||' api_keys='||(SELECT count(*) FROM api_keys)")
say "prod    $PROD_ROWS"
[[ "$ROWS" == "$PROD_ROWS" ]] || { say "FAIL row count mismatch"; docker exec "$CONTAINER" psql -U firepaas -d postgres -c "DROP DATABASE $SCRATCH" >/dev/null; exit 1; }
say "PASS 备份可恢复且行数一致"

docker exec "$CONTAINER" psql -U firepaas -d postgres -c "DROP DATABASE $SCRATCH" >/dev/null
say "scratch db dropped"
