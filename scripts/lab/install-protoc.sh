#!/usr/bin/env bash
# 安装用户态 protoc 与 Go 生成插件到 $LAB_ROOT（M1.1 proto 生成工具链）。
# 用法: bash scripts/lab/install-protoc.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

PROTOC_VERSION="${PROTOC_VERSION:-29.3}"
PROTOC_ROOT="$LAB_ROOT/protoc"
export GOTOOLCHAIN=local
export GOPROXY="${GOPROXY:-direct}"
export GOSUMDB=off

echo "==> protoc $PROTOC_VERSION"
if [[ ! -x "$PROTOC_ROOT/bin/protoc" ]]; then
  mkdir -p "$PROTOC_ROOT"
  curl -fsSL -o /tmp/protoc.zip \
    "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip"
  rm -rf /tmp/protoc-extract && mkdir -p /tmp/protoc-extract
  unzip -oq /tmp/protoc.zip -d /tmp/protoc-extract
  cp -r /tmp/protoc-extract/bin /tmp/protoc-extract/include "$PROTOC_ROOT/"
  rm -rf /tmp/protoc.zip /tmp/protoc-extract
fi
ln -sf "$PROTOC_ROOT/bin/protoc" "$LAB_ROOT/bin/protoc"

echo "==> protoc-gen-go / protoc-gen-go-grpc"
GOBIN="$LAB_ROOT/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
GOBIN="$LAB_ROOT/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

"$LAB_ROOT/bin/protoc" --version
"$LAB_ROOT/bin/protoc-gen-go" --version 2>/dev/null || true
"$LAB_ROOT/bin/protoc-gen-go-grpc" --version 2>/dev/null || true
echo "==> done"
