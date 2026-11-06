#!/usr/bin/env bash
# Register the Debezium Postgres connector (HTTP 409 = already registered), mark success for the
# healthcheck, then stay up so `docker compose up --wait` treats this service as healthy.
set -euo pipefail
URL="${CONNECT_URL:-http://connect:8083}"
for attempt in $(seq 1 30); do
  code=$(curl -s -o /tmp/register.out -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    --data @/infra/connect/postgres-cdc.json "${URL}/connectors" || echo "000")
  if [ "${code}" = "201" ] || [ "${code}" = "409" ]; then
    echo "connector paylater-cdc registered (HTTP ${code})"
    touch /tmp/registered
    exec sleep infinity
  fi
  echo "attempt ${attempt}: HTTP ${code}: $(cat /tmp/register.out 2>/dev/null || true)"
  sleep 5
done
echo "could not register the connector" >&2
exit 1
