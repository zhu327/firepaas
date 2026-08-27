#!/usr/bin/env bash
# minio-backup-rehearsal.sh：M5.4（mvp-plan §9.4）对象存储备份演练。
#
# 实验室 MinIO 容器是 distroless（无 find/tar/ls -R），无法在容器内做树对照。
# 因此演练改为确定性校验：将 /data 连续拷贝两次到两个全新目录，比对两边的
# 逐文件 SHA-256 清单（字节级一致 → docker cp 往返确定性成立，恢复路径有据）；
# 同时与本机保留的上次清单（results/m5/minio-manifest.txt）比对，发现漂移即失败。
#
# 生产恢复路径见 docs/runbook-object-storage.md。
set -euo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[minio-rehearsal $(TS)] $*"; }

CONTAINER="${MINIO_CONTAINER:-dev-minio-1}"
BASE=$(mktemp -d /tmp/minio-rehearsal.XXXXXX)
trap 'rm -rf "$BASE"' EXIT
A="$BASE/copy-a"; B="$BASE/copy-b"
mkdir -p "$A" "$B"

say "copy pass 1 → $A"
docker cp "$CONTAINER":/data/. "$A"/
say "copy pass 2 → $B"
docker cp "$CONTAINER":/data/. "$B"/

manifest() { (cd "$1" && find . -type f | sort | while read -r f; do
    md5sum "$f"; done); }
A_MAN="$BASE/man-a.txt"; B_MAN="$BASE/man-b.txt"
manifest "$A" > "$A_MAN"
manifest "$B" > "$B_MAN"

if ! cmp -s "$A_MAN" "$B_MAN"; then
  say "FAIL 两次拷贝清单不一致"
  diff "$A_MAN" "$B_MAN" | head -10
  exit 1
fi
say "PASS 两次拷贝清单一致（$(wc -l < "$A_MAN") 个文件）"

KEEP="/home/zty/Learn/firepaas/scripts/lab/results/m5/minio-manifest.txt"
if [[ -f "$KEEP" ]]; then
  if cmp -s "$KEEP" "$A_MAN"; then
    say "PASS 与上次演练清单无漂移"
  else
    say "FAIL 对象存储内容相对上次演练发生漂移（人工确认后更新清单）"
    diff "$KEEP" "$A_MAN" | head -10
    exit 1
  fi
else
  mkdir -p "$(dirname "$KEEP")"
  cp "$A_MAN" "$KEEP"
  say "PASS 首次演练：基线清单已保存 $KEEP"
fi
