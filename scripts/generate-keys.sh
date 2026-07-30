#!/usr/bin/env bash

set -euo pipefail

OUTDIR="${1:-.}"
PRIVATE_KEY="${OUTDIR}/private_key.pem"
PUBLIC_KEY="${OUTDIR}/public_key.pem"

echo "==> Generating RSA-2048 key pair..."

openssl genpkey -algorithm RSA -out "${PRIVATE_KEY}" -pkeyopt rsa_keygen_bits:2048
openssl rsa -pubout -in "${PRIVATE_KEY}" -out "${PUBLIC_KEY}"

echo ""
echo "==> Keys generated:"
echo "    Private: ${PRIVATE_KEY}"
echo "    Public:  ${PUBLIC_KEY}"
echo ""
echo "==> Add the following to your .env file:"
echo ""
echo "PRIVATE_KEY_PEM='$(cat "${PRIVATE_KEY}")'"
echo ""
echo "PUBLIC_KEY_PEM='$(cat "${PUBLIC_KEY}")'"
echo ""
echo "==> Or export them directly:"
echo ""
echo "    export PRIVATE_KEY_PEM=\"\$(cat ${PRIVATE_KEY})\""
echo "    export PUBLIC_KEY_PEM=\"\$(cat ${PUBLIC_KEY})\""
echo ""
echo "==> Then start the system:"
echo "    docker compose up -d"
