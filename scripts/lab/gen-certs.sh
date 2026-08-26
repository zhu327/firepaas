#!/usr/bin/env bash
# 生成 M1.3 静态 mTLS 证书（ADR-0006 降级路径）。用法: bash scripts/lab/gen-certs.sh
# 输出 scripts/lab/certs/（gitignore）：ca / agentd server / control-plane client / edge client。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERT_DIR="${CERT_DIR:-$HERE/certs}"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

CN_CA="${CN_CA:-firepaas-lab-ca}"
CN_AGENT="${CN_AGENT:-agentd}"
CN_CP="${CN_CP:-control-plane}"
CN_EDGE="${CN_EDGE:-edge-proxy}"

echo "==> CA"
if [[ ! -f ca.crt ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout ca.key -out ca.crt \
    -subj "/CN=$CN_CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"
fi

gen() {
  local name="$1" cn="$2" usage="$3" san="$4"
  if [[ -f "$name.crt" ]]; then
    echo "    $name 已存在，跳过"
    return 0
  fi
  openssl req -newkey rsa:2048 -nodes -keyout "$name.key" -out "$name.csr" -subj "/CN=$cn"
  cat > "$name.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=$usage
subjectAltName=$san
EOF
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 365 -out "$name.crt" -extfile "$name.ext"
  rm -f "$name.csr" "$name.ext"
}

echo "==> agentd server（SAN: IP 127.0.0.1 + DNS localhost/agentd）"
gen agentd "$CN_AGENT" "serverAuth" "IP:127.0.0.1,DNS:localhost,DNS:agentd"

echo "==> control-plane client"
gen control-plane "$CN_CP" "clientAuth" "DNS:$CN_CP"

echo "==> edge client"
gen edge "$CN_EDGE" "clientAuth" "DNS:$CN_EDGE"

chmod 600 *.key
echo "==> done: $CERT_DIR"
