#!/usr/bin/env bash
# Register the Debezium Postgres connector; succeed if it already exists (HTTP 409).
set -euo pipefail
URL="${CONNECT_URL:-http://connect:8083}"
for attempt in $(seq 1 30); do
  code=$(curl -s -o /tmp/register.out -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    --data @/infra/connect/postgres-cdc.json "${URL}/connectors" || echo "000")
  if [ "${code}" = "201" ] || [ "${code}" = "409" ]; then
    echo "connector paylater-cdc registered (HTTP ${code})"
    exit 0
  fi
  echo "attempt ${attempt}: HTTP ${code}: $(cat /tmp/register.out 2>/dev/null || true)"
  sleep 5
done
echo "could not register the connector" >&2
exit 1
