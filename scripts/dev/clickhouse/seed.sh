#!/usr/bin/env bash
#
# Create sample log data in a local ClickHouse for the ES|QL ClickHouse data source.
#
# Starts a ClickHouse server in Docker (docker-compose.yml) seeded with a logs.app_logs table
# of sample application logs (init-logs.sql), waits until the table is queryable, and prints the
# row count plus the ClickHouse URL to use as the dataset resource.
#
# This is the data-creation half of the demo. To wire an Elasticsearch cluster (built from this
# branch) to the ClickHouse server and validate that the data is queryable through ES|QL, run:
#
#   CLICKHOUSE_URL='clickhouse://localhost:8123/logs/app_logs' \
#     ./gradlew :x-pack:plugin:esql-datasource-clickhouse:qa:javaRestTest
#
# Stop and remove the ClickHouse server with:
#
#   docker compose -f scripts/dev/clickhouse/docker-compose.yml down -v

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${SCRIPT_DIR}/docker-compose.yml}"

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASS="${CLICKHOUSE_PASS:-}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-logs}"
CLICKHOUSE_TABLE="${CLICKHOUSE_TABLE:-app_logs}"
CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-esql-ds-clickhouse}"
CLICKHOUSE_TIMEOUT="${CLICKHOUSE_TIMEOUT:-180}"
BASE_URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"

log()  { printf '\033[1;34m[seed]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[seed]\033[0m %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

clickhouse_query() {
  curl -sf \
    -H "X-ClickHouse-User: ${CLICKHOUSE_USER}" \
    -H "X-ClickHouse-Key: ${CLICKHOUSE_PASS}" \
    --data-binary "$1" \
    "${BASE_URL}/"
}

command -v docker >/dev/null 2>&1 || die "docker is required but not found on PATH."
docker info >/dev/null 2>&1 || die "Docker daemon is not running. Start Docker and retry."
docker compose version >/dev/null 2>&1 || die "docker compose (v2) is required but not available."
[[ -f "$COMPOSE_FILE" ]] || die "docker-compose file not found: $COMPOSE_FILE"

log "Starting ClickHouse in Docker on :${CLICKHOUSE_PORT} (docker compose)..."
docker compose -f "$COMPOSE_FILE" up -d >/dev/null

log "Waiting for ClickHouse to respond (timeout ${CLICKHOUSE_TIMEOUT}s)..."
deadline=$(( $(date +%s) + CLICKHOUSE_TIMEOUT ))
until curl -sf "${BASE_URL}/ping" >/dev/null 2>&1; do
  if [[ $(date +%s) -ge $deadline ]]; then
    err "ClickHouse did not become healthy in time. Recent container logs:"
    docker logs --tail 40 "$CLICKHOUSE_CONTAINER" >&2 || true
    die "Aborting."
  fi
  sleep 3
done

log "Waiting for the seeded table ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE} to be ready..."
deadline=$(( $(date +%s) + 60 ))
until clickhouse_query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}" >/dev/null 2>&1; do
  if [[ $(date +%s) -ge $deadline ]]; then
    err "Seeded table did not appear in time. Recent container logs:"
    docker logs --tail 40 "$CLICKHOUSE_CONTAINER" >&2 || true
    die "Aborting."
  fi
  sleep 2
done

rows="$(clickhouse_query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}" | tr -d '[:space:]')"
log "ClickHouse is up at ${BASE_URL} with ${rows} row(s) in ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_TABLE}."
log "Use this dataset resource:  clickhouse://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/${CLICKHOUSE_DATABASE}/${CLICKHOUSE_TABLE}"
