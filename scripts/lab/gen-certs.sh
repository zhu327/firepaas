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

# M4（ADR-0011 实验室形态）：客户端入口泛域名证书。
# 生产用 step-ca ACME 按需签发；实验室由内部 CA 直接签 10 年 leaf，
# 客户端信任 = 预置 ca.crt（runbook：curl --cacert ca.crt https://...）。
WILDCARD_DOMAIN="${FIREPAAS_INGRESS_DOMAIN:-firepaas.local}"
echo "==> ingress wildcard (SAN: *.$WILDCARD_DOMAIN + $WILDCARD_DOMAIN + localhost)"
if [[ ! -f "wildcard-$WILDCARD_DOMAIN.crt" ]]; then
  openssl req -newkey rsa:2048 -nodes     -keyout "wildcard-$WILDCARD_DOMAIN.key"     -out "wildcard-$WILDCARD_DOMAIN.csr"     -subj "/CN=*.$WILDCARD_DOMAIN"
  cat > "wildcard-$WILDCARD_DOMAIN.ext" <<XEOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:*.$WILDCARD_DOMAIN,DNS:$WILDCARD_DOMAIN,IP:127.0.0.1
XEOF
  openssl x509 -req -in "wildcard-$WILDCARD_DOMAIN.csr" -CA ca.crt -CAkey ca.key     -CAcreateserial -days 3650 -out "wildcard-$WILDCARD_DOMAIN.crt"     -extfile "wildcard-$WILDCARD_DOMAIN.ext"
  rm -f "wildcard-$WILDCARD_DOMAIN.csr" "wildcard-$WILDCARD_DOMAIN.ext"
else
  echo "    wildcard-$WILDCARD_DOMAIN already exists, skip"
fi

chmod 600 *.key
echo "==> done: $CERT_DIR"
