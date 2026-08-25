#!/usr/bin/env bash
# 构建 hypeman 二进制（嵌入 Cloud Hypervisor / Firecracker v1.14.2 / Caddy v2.10.2）。
# 本机无 make/gcc，本脚本等价执行 hypeman Makefile 的 build-linux（当前架构），
# 全程 CGO_ENABLED=0。产物复制到 $LAB_ROOT/bin/hypeman。
# 用法: bash scripts/lab/build-hypeman.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

SRC="${HYPEMAN_SRC:-$HOME/Learn/hypeman}"
BIN="$SRC/bin"
CH_VERSION_OLD="v49.0"
CH_VERSION_NEW="v51.1"
FC_VERSION="v1.14.2"
CADDY_VERSION="v2.10.2"

export CGO_ENABLED=0
export GOTOOLCHAIN=local

mkdir -p "$BIN"

echo "==> Cloud Hypervisor embedded binaries"
for ver in "$CH_VERSION_OLD" "$CH_VERSION_NEW"; do
  mkdir -p "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/x86_64" \
           "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/aarch64"
  if [[ ! -s "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/x86_64/cloud-hypervisor" ]]; then
    curl -fsSL -o "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/x86_64/cloud-hypervisor" \
      "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/$ver/cloud-hypervisor-static"
    chmod +x "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/x86_64/cloud-hypervisor"
  fi
  if [[ ! -s "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/aarch64/cloud-hypervisor" ]]; then
    curl -fsSL -o "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/aarch64/cloud-hypervisor" \
      "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/$ver/cloud-hypervisor-static-aarch64"
    chmod +x "$SRC/lib/vmm/binaries/cloud-hypervisor/$ver/aarch64/cloud-hypervisor"
  fi
done

echo "==> Firecracker $FC_VERSION embedded binaries"
FC_DIR="$SRC/lib/hypervisor/firecracker/binaries/firecracker/$FC_VERSION"
mkdir -p "$FC_DIR/x86_64" "$FC_DIR/aarch64"
if [[ ! -s "$FC_DIR/x86_64/firecracker" ]]; then
  curl -fsSL "https://github.com/firecracker-microvm/firecracker/releases/download/$FC_VERSION/firecracker-$FC_VERSION-x86_64.tgz" \
    | tar -xzO "release-$FC_VERSION-x86_64/firecracker-$FC_VERSION-x86_64" > "$FC_DIR/x86_64/firecracker"
  chmod +x "$FC_DIR/x86_64/firecracker"
fi
if [[ ! -s "$FC_DIR/aarch64/firecracker" ]]; then
  curl -fsSL "https://github.com/firecracker-microvm/firecracker/releases/download/$FC_VERSION/firecracker-$FC_VERSION-aarch64.tgz" \
    | tar -xzO "release-$FC_VERSION-aarch64/firecracker-$FC_VERSION-aarch64" > "$FC_DIR/aarch64/firecracker"
  chmod +x "$FC_DIR/aarch64/firecracker"
fi

echo "==> Caddy $CADDY_VERSION（当前架构，cloudflare DNS 插件）"
CADDY_DIR="$SRC/lib/ingress/binaries/caddy/$CADDY_VERSION/x86_64"
mkdir -p "$CADDY_DIR"
if [[ ! -s "$CADDY_DIR/caddy" ]]; then
  XCADDY="$BIN/xcaddy"
  if [[ ! -x "$XCADDY" ]]; then
    GOBIN="$BIN" go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest
  fi
  GOOS=linux GOARCH=amd64 "$XCADDY" build "$CADDY_VERSION" \
    --with github.com/caddy-dns/cloudflare \
    --output "$CADDY_DIR/caddy"
  chmod +x "$CADDY_DIR/caddy"
fi

echo "==> guest init / guest-agent"
( cd "$SRC/lib/system/guest_agent" && GOOS=linux go build -ldflags="-s -w" -o guest-agent . )
( cd "$SRC/lib/system/init" && GOOS=linux go build -ldflags="-s -w" -o init . )

echo "==> go build hypeman + uffd-pager + token tool"
( cd "$SRC" && go build -tags containers_image_openpgp -o "$BIN/hypeman" ./cmd/api )
( cd "$SRC" && go build -o "$BIN/hypeman-uffd-pager" ./cmd/uffd-pager )
( cd "$SRC" && go build -o "$BIN/hypeman-token" ./cmd/gen-jwt )

cp -f "$BIN/hypeman" "$LAB_ROOT/bin/hypeman"
cp -f "$BIN/hypeman-token" "$LAB_ROOT/bin/hypeman-token"

echo "==> hypeman-cli v0.18.0（REST/exec 客户端）"
if [[ ! -x "$LAB_ROOT/bin/hypeman-cli" ]]; then
  CLI_SRC="${HYPEMAN_CLI_SRC:-$HOME/.local/firepaas-lab/var/tmp/hypeman-cli}"
  if [[ ! -d "$CLI_SRC/.git" ]]; then
    rm -rf "$CLI_SRC"
    git clone --depth 1 --branch v0.18.0 https://github.com/kernel/hypeman-cli.git "$CLI_SRC"
  fi
  ( cd "$CLI_SRC" && GOPROXY=direct GOSUMDB=off \
      go build -o "$LAB_ROOT/bin/hypeman-cli" ./cmd/hypeman )
fi

echo "==> provenance"
{
  echo "built_at: $(date -Is)"
  echo "hypeman_git_sha: $(git -C "$SRC" rev-parse HEAD)"
  echo "go_version: $(go version)"
  echo "firecracker: $FC_VERSION"
  echo "caddy: $CADDY_VERSION"
  echo "binary: $LAB_ROOT/bin/hypeman"
} | tee "$LAB_ROOT/bin/hypeman.provenance" >/dev/null

echo "==> done: $LAB_ROOT/bin/hypeman"
