#!/usr/bin/env bash
# pg-backup.sh：M5.4（mvp-plan §9.4）PostgreSQL 全库备份。
#
# 用法：sudo bash scripts/lab/pg-backup.sh [备份目录]
#   - pg_dump（容器 dev-postgres-1）→ gzip → $BACKUP_DIR/pg-YYYYmmdd-HHMMSS.sql.gz
#   - 保留最近 7 份，其余删除
#   - 不打断在途连接（pg_dump 普通事务快照）
#
# 可选加固（生产就绪 P2#22；全部默认关闭，不设 env 行为与原版一致）：
#   - FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE：非空时对 .sql.gz 做 gpg 对称加密
#     （AES256），产物改为 .sql.gz.gpg 并删除未加密中间文件
#   - FIREPAAS_BACKUP_UPLOAD_CMD：非空时作为外送 hook 调用，同 dr-rehearsal.sh
#     的命令注入风格（eval "$FIREPAAS_BACKUP_UPLOAD_CMD" <最终产物路径>）；
#     上传失败即非零退出，不静默退回仅本地留存
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

# 可选 gpg 对称加密：产物改名 .sql.gz.gpg，未加密中间文件立即删除。
if [[ -n "${FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE:-}" ]]; then
  [[ -f "$FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE" ]] || { say "FAIL passphrase file missing: $FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE"; exit 1; }
  gpg --batch --yes --pinentry-mode loopback \
    --passphrase-file "$FIREPAAS_BACKUP_GPG_PASSPHRASE_FILE" \
    --symmetric --cipher-algo AES256 -o "$OUT.gpg" "$OUT"
  rm -f "$OUT"
  OUT="$OUT.gpg"
  say "encrypted → $OUT ($(du -h "$OUT" | cut -f1))"
fi

# P3-17（M5 评审）：记录备份时刻的关键表行数 sidecar（.rowcounts）。恢复演练
# 对比它，而不是活库——活库在 soak/chaos 持续写入下必然漂移，原断言必误报。
ROWCOUNTS=$(docker exec "$CONTAINER" psql -U firepaas -d firepaas -tAc \
  "SELECT 'apps='||(SELECT count(*) FROM apps)||' machines='||(SELECT count(*) FROM machines)
        ||' operations='||(SELECT count(*) FROM operations)
        ||' secrets='||(SELECT count(*) FROM secrets)
        ||' api_keys='||(SELECT count(*) FROM api_keys)")
printf '%s\n' "$ROWCOUNTS" > "$BACKUP_DIR/pg-$STAMP.rowcounts"
say "rowcounts at backup time: $ROWCOUNTS"

# 可选外送 hook：上传的是最终产物（加密开启时为 .sql.gz.gpg）。
# 与外送相关的凭证只出现在 FIREPAAS_BACKUP_UPLOAD_CMD 展开后命令自身，不进本脚本日志。
if [[ -n "${FIREPAAS_BACKUP_UPLOAD_CMD:-}" ]]; then
  if eval "$FIREPAAS_BACKUP_UPLOAD_CMD" "\"$OUT\""; then
    say "uploaded via FIREPAAS_BACKUP_UPLOAD_CMD"
  else
    say "FAIL upload hook: FIREPAAS_BACKUP_UPLOAD_CMD"
    exit 1
  fi
fi

# 保留最近 7 份（含 .rowcounts sidecar 与可选 .sql.gz.gpg 加密产物）。
# 文件名内嵌 pg-YYYYmmdd-HHMMSS 时间戳，按名字倒序即按时间倒序。
{ ls -1 "$BACKUP_DIR"/pg-*.sql.gz 2>/dev/null || true; ls -1 "$BACKUP_DIR"/pg-*.sql.gz.gpg 2>/dev/null || true; } \
  | sort -r | tail -n +8 | while read -r old; do
  rm -f "$old" "${old%%.sql.gz*}.rowcounts"
  say "pruned $old"
done
say "keep latest 7 backups; current list:"
{ ls -1 "$BACKUP_DIR"/pg-*.sql.gz 2>/dev/null || true; ls -1 "$BACKUP_DIR"/pg-*.sql.gz.gpg 2>/dev/null || true; } | sort -r
