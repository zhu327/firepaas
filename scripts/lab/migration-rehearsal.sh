#!/usr/bin/env bash
# migration-rehearsal.sh：迁移重演（生产就绪 P2#22）。
#
# 对独立 scratch 库按文件名顺序重演全部迁移，验证：
#   1) 一遍跑全：schema_migrations ledger 与 migrations 目录逐一对应（无缺失）
#   2) 幂等：第二次启动同一二进制时迁移器跳过全部版本（ledger 零变化）
#   3) 打印回滚纪律说明（迁移只新增不改写，无 down migration；见末尾输出）
#
# 迁移器即生产路径：临时构建 firepaas-api 并让其在启动时执行 db.Migrate
# （internal/controlplane/db/migrate.go，advisory lock 串行、每迁移一事务）。
#
# 用法：bash scripts/lab/migration-rehearsal.sh
# 环境（默认值对齐 make dev-up 的 compose 依赖，与 FIREPAAS_TEST_* 约定一致）：
#   PG_CONTAINER=dev-postgres-1                 compose PG 容器名
#   FIREPAAS_REHEARSAL_PG_URL_BASE=postgres://firepaas:firepaas@127.0.0.1:5432
#   FIREPAAS_REHEARSAL_DB=firepaas_migration_rehearsal   scratch 库名（会被 DROP/CREATE！）
#   FIREPAAS_TEST_REDIS=127.0.0.1:6379          api 启动需要的 Redis（惰性连接）
#   FIREPAAS_REHEARSAL_API_PORT=<随机空闲端口>    临时 API 监听端口（可覆盖）
set -euo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[migration-rehearsal $(TS)] $*"; }

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$HERE/../.." && pwd)"
MIGRATIONS_DIR="$ROOT_DIR/internal/controlplane/db/migrations"

PG_CONTAINER="${PG_CONTAINER:-dev-postgres-1}"
PG_URL_BASE="${FIREPAAS_REHEARSAL_PG_URL_BASE:-postgres://firepaas:firepaas@127.0.0.1:5432}"
SCRATCH="${FIREPAAS_REHEARSAL_DB:-firepaas_migration_rehearsal}"
REDIS_ADDR="${FIREPAAS_TEST_REDIS:-127.0.0.1:6379}"
# 默认取系统分配的临时空闲端口，避免与常驻实验进程撞车。
API_PORT="${FIREPAAS_REHEARSAL_API_PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')}"

WORK=$(mktemp -d)
API_PID=""
cleanup() {
  [[ -n "$API_PID" ]] && kill "$API_PID" 2>/dev/null || true
  docker exec "$PG_CONTAINER" psql -U firepaas -d postgres -c \
    "DROP DATABASE IF EXISTS $SCRATCH" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

psql() { docker exec "$PG_CONTAINER" psql -U firepaas -d "$1" -tAc "$2"; }

# 1) 构建当前工作树的 api 二进制——演练的必须是本仓库内的迁移集，而非
# $LAB_BIN 里的旧构建。
say "building firepaas-api from worktree"
(cd "$ROOT_DIR" && GOWORK=off go build -trimpath -o "$WORK/firepaas-api" ./cmd/api)

# 2) scratch 库重建。
say "recreating scratch db $SCRATCH in $PG_CONTAINER"
docker exec "$PG_CONTAINER" psql -U firepaas -d postgres -c \
  "DROP DATABASE IF EXISTS $SCRATCH" >/dev/null
docker exec "$PG_CONTAINER" psql -U firepaas -d postgres -q -c "CREATE DATABASE $SCRATCH"

start_api() {
  # 最小装配：认证关闭（本地演练）、Nomad 指向不可达地址（发现报错不影响
  # 迁移与 HTTP 服务）、Redis 指向 compose（惰性连接）。
  FIREPAAS_POSTGRES_URL="$PG_URL_BASE/$SCRATCH?sslmode=disable" \
    FIREPAAS_AUTH_DISABLED=true \
    FIREPAAS_HTTP_PORT="$API_PORT" \
    FIREPAAS_REDIS_ADDR="$REDIS_ADDR" \
    FIREPAAS_NOMAD_ADDR="http://127.0.0.1:9" \
    "$WORK/firepaas-api" >"$WORK/api.log" 2>&1 &
  API_PID=$!
  for _ in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$API_PORT/v1/health" >/dev/null 2>&1 && return 0
    kill -0 "$API_PID" 2>/dev/null || break
    sleep 1
  done
  say "FAIL api did not become healthy; last log lines:"
  tail -20 "$WORK/api.log" >&2 || true
  return 1
}

stop_api() {
  [[ -z "$API_PID" ]] && return 0
  kill "$API_PID" 2>/dev/null || true
  wait "$API_PID" 2>/dev/null || true
  API_PID=""
}

ledger_md5() {
  psql "$SCRATCH" "SELECT version || '@' || applied_at FROM schema_migrations ORDER BY version" | md5sum
}

# 3) 第一遍：全部迁移按序落账。
say "run 1: applying all migrations"
start_api || exit 1
stop_api

WANT=$(cd "$MIGRATIONS_DIR" && ls -1 ./*.sql | xargs -n1 basename | sort)
GOT=$(psql "$SCRATCH" "SELECT version FROM schema_migrations ORDER BY version")
MISSING=$(comm -23 <(printf '%s\n' "$WANT") <(printf '%s\n' "$GOT"))
EXTRA=$(comm -13 <(printf '%s\n' "$WANT") <(printf '%s\n' "$GOT"))
COUNT_W=$(printf '%s\n' "$WANT" | wc -l)
COUNT_G=$(printf '%s\n' "$GOT" | wc -l)
say "ledger: $COUNT_G applied / $COUNT_W files"
[[ -z "$MISSING" && -z "$EXTRA" && "$COUNT_G" -eq "$COUNT_W" ]] || {
  say "FAIL schema_migrations ledger incomplete/mismatched"
  [[ -n "$MISSING" ]] && say "missing: $MISSING"
  [[ -n "$EXTRA" ]] && say "unexpected: $EXTRA"
  exit 1
}
say "PASS ledger completeness（$COUNT_W 个迁移全部落账）"
BEFORE=$(ledger_md5)

# 4) 第二遍：迁移器必须整体跳过（幂等）。ledger 内容（含 applied_at）不变
# 证明没有版本被重放。
say "run 2: idempotent re-run (migrator must skip all)"
start_api || exit 1
stop_api
AFTER=$(ledger_md5)
[[ "$BEFORE" == "$AFTER" ]] || { say "FAIL second run mutated schema_migrations"; exit 1; }
say "PASS idempotency（第二遍 ledger 零变化，迁移器全部跳过）"

cat <<'NOTE'
[migration-rehearsal] 回滚纪律：
  - migration 只新增顺序文件，不改写既有文件（AGENTS.md）；
    db.Migrate 没有 down migration。
  - 需要回退 schema 时走"新的纠正性前向迁移"，不回收旧版本号；
    数据级回退 = pg 备份恢复（scripts/lab/pg-restore-rehearsal.sh 演练同一链路）。
  - 新迁移必须在同一 release 内保持 rollback-compatible：旧二进制仍可
    正常读写新 schema（见 docs/runbook-upgrade-control-plane.md）。
NOTE
say "PASS 迁移重演完成（scratch 库 $SCRATCH 已清理）"
