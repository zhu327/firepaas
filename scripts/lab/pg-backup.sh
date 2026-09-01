#!/usr/bin/env bash
# pg-backup.sh：M5.4（mvp-plan §9.4）PostgreSQL 全库备份。
#
# 用法：sudo bash scripts/lab/pg-backup.sh [备份目录]
#   - pg_dump（容器 dev-postgres-1）→ gzip → $BACKUP_DIR/pg-YYYYmmdd-HHMMSS.sql.gz
#   - 保留最近 7 份，其余删除
#   - 不打断在途连接（pg_dump 普通事务快照）
set -euo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[pg-backup $(TS)] $*"; }

CONTAINER="${PG_CONTAINER:-dev-postgres-1}"
BACKUP_DIR="${1:-/var/lib/firepaas-p0/backups/postgres}"
STAMP=$(date '+%Y%m%d-%H%M%S')
OUT="$BACKUP_DIR/pg-$STAMP.sql.gz"

mkdir -p "$BACKUP_DIR"
say "dumping $CONTAINER → $OUT"
docker exec "$CONTAINER" pg_dump -U firepaas -d firepaas --no-owner --no-privileges |
  gzip > "$OUT"
SIZE=$(du -h "$OUT" | cut -f1)
say "done $OUT ($SIZE)"

# P3-17（M5 评审）：记录备份时刻的关键表行数 sidecar（.rowcounts）。恢复演练
# 对比它，而不是活库——活库在 soak/chaos 持续写入下必然漂移，原断言必误报。
ROWCOUNTS=$(docker exec "$CONTAINER" psql -U firepaas -d firepaas -tAc \
  "SELECT 'apps='||(SELECT count(*) FROM apps)||' machines='||(SELECT count(*) FROM machines)
        ||' operations='||(SELECT count(*) FROM operations)
        ||' secrets='||(SELECT count(*) FROM secrets)
        ||' api_keys='||(SELECT count(*) FROM api_keys)")
printf '%s\n' "$ROWCOUNTS" > "${OUT%.sql.gz}.rowcounts"
say "rowcounts at backup time: $ROWCOUNTS"

# 保留最近 7 份（含 .rowcounts sidecar）。
ls -1t "$BACKUP_DIR"/pg-*.sql.gz 2>/dev/null | tail -n +8 | while read -r old; do
  rm -f "$old" "${old%.sql.gz}.rowcounts"
  say "pruned $old"
done
say "keep latest 7 backups; current list:"
ls -1t "$BACKUP_DIR"/pg-*.sql.gz
