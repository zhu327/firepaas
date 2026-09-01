#!/usr/bin/env bash
# push-ontime.sh：构建并推送 ontime guest-clock 探针镜像到本地 registry
# （P1-6 修复：e2e-m5 依赖此镜像；此前硬编码 digest 且无构建脚本，registry
# 数据一旦损坏即无法复现——2026-08-27 实测踩中：tag 指向的 manifest blob 404）。
#
# 用法：bash scripts/lab/push-ontime.sh
# 输出：REF=<digest-pinned 引用> 与 DIGEST=<manifest sha256>（e2e/soak source 用）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REG="${FIREPAAS_REGISTRY:-127.0.0.1:5000}"
API="$REG/v2/firepaas/ontime"   # registry HTTP API 路径
NAME="$REG/firepaas/ontime"      # 镜像引用名（无 /v2/）
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

export PATH="$HOME/.local/firepaas-lab/go/bin:$HOME/.local/firepaas-lab/bin:$PATH"

# 0) 静态 busybox：hypeman-init 经 /bin/sh -c 启动 entrypoint（mode_exec.go），
#    镜像内必须有 sh（2026-08-27 实测：scratch 无 sh → init panic → 卡 INITIALIZING）。
#    从镜像源拉一次并缓存到 scripts/lab/.cache/。
CACHE="$HERE/.cache"
mkdir -p "$CACHE"
if [[ ! -x "$CACHE/busybox" ]]; then
  docker pull docker.m.daocloud.io/library/busybox:1.36-musl >/dev/null 2>&1 || docker pull busybox:1.36-musl >/dev/null
  bid=$(docker create docker.m.daocloud.io/library/busybox:1.36-musl 2>/dev/null || docker create busybox:1.36-musl)
  docker cp "$bid":/bin/busybox "$CACHE/busybox"
  docker rm "$bid" >/dev/null
fi
[[ -x "$CACHE/busybox" ]] || { echo "busybox 获取失败（需 docker 与镜像源）" >&2; exit 1; }

# 1) 静态编译（scratch 镜像，无 libc 依赖）。
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-s -w' \
  -o "$WORK/ontime" ./tools/ontime

# 2) 最小 OCI 镜像（config + 单 tar layer），直接走 registry v2 API，
#    无需本机 docker daemon。
(cd "$WORK" && mkdir -p bin && cp "$CACHE/busybox" bin/busybox && \
  chmod 755 bin/busybox && ln -sf busybox bin/sh && tar cf layer.tar ontime bin && gzip -n layer.tar)

python3 - "$WORK" <<'PY'
import hashlib, json, sys, os
w = sys.argv[1]
compressed = open(os.path.join(w, "layer.tar.gz"), "rb").read()
import gzip as _gz
with _gz.open(os.path.join(w, "layer.tar.gz"), "rb") as f:
    layer = f.read()
diff_id = hashlib.sha256(layer).hexdigest()
cfg = {"architecture": "amd64", "os": "linux",
       "config": {"Env": ["PORT=80"], "Entrypoint": ["/ontime"]},
       "rootfs": {"type": "layers", "diff_ids": ["sha256:" + diff_id]}}
open(os.path.join(w, "config.json"), "w").write(json.dumps(cfg))
manifest = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
    "config": {"mediaType": "application/vnd.docker.container.image.v1+json",
               "size": len(json.dumps(cfg).encode()),
               "digest": "sha256:" + hashlib.sha256(json.dumps(cfg).encode()).hexdigest()},
    "layers": [{"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "size": len(compressed),
                "digest": "sha256:" + hashlib.sha256(compressed).hexdigest()}]}
open(os.path.join(w, "manifest.json"), "w").write(json.dumps(manifest))
PY

push_blob() { # push_blob <file> <digest>：两段式上传（POST 拿 session → PUT 带 digest）。
  # 注：本实验室 registry:2 对单 POST+digest 返回 202 且忽略 body，
  # 不能把 202 当成功。
  local loc sep code
  loc=$(curl -s -D - -o /dev/null -X POST "$API/blobs/uploads/" |
    awk 'tolower($1)=="location:"{print $2}' | tr -d '\r')
  [[ -n "$loc" ]] || { echo "blob session create failed" >&2; exit 1; }
  [[ "$loc" == /* ]] && loc="$REG$loc"
  sep='?'; [[ "$loc" == *\?* ]] && sep='&'
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/octet-stream' --data-binary "@$1" \
    "${loc}${sep}digest=$2")
  [[ "$code" == "201" || "$code" == "204" ]] ||
    { echo "blob push failed ($code): $2" >&2; exit 1; }
}

CFG_DIGEST="sha256:$(python3 -c "import hashlib;print(hashlib.sha256(open('$WORK/config.json','rb').read()).hexdigest())")"
LAYER_DIGEST="sha256:$(python3 -c "import hashlib;print(hashlib.sha256(open('$WORK/layer.tar.gz','rb').read()).hexdigest())")"
push_blob "$WORK/config.json" "$CFG_DIGEST"
push_blob "$WORK/layer.tar.gz" "$LAYER_DIGEST"

MANIFEST_DIGEST="sha256:$(sha256sum "$WORK/manifest.json" | cut -d' ' -f1)"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/vnd.docker.distribution.manifest.v2+json' \
  --data-binary "@$WORK/manifest.json" \
  "$API/manifests/$MANIFEST_DIGEST")
[[ "$code" == "201" || "$code" == "202" ]] || { echo "manifest push failed: $code" >&2; exit 1; }
# 同时打个可读 tag（tag 只是别名；部署引用一律 digest-pinned）。
curl -s -o /dev/null -X PUT \
  -H 'Content-Type: application/vnd.docker.distribution.manifest.v2+json' \
  --data-binary "@$WORK/manifest.json" \
  "$API/manifests/1"

# 自检：digest 可解析（防止“tag 在、blob 丢”的损坏态再次静默溜进 e2e）。
code=$(curl -s -o /dev/null -w '%{http_code}' \
  -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
  "$API/manifests/$MANIFEST_DIGEST")
[[ "$code" == "200" ]] || { echo "digest self-check failed: $code" >&2; exit 1; }

echo "REF=$NAME@$MANIFEST_DIGEST"
echo "DIGEST=$MANIFEST_DIGEST"
