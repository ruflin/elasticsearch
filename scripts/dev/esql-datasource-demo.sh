#!/usr/bin/env bash
#
# ES|QL Elasticsearch data source end-to-end demo.
#
# Brings up the full topology and wires it together:
#
#   Kibana (yarn start :5601)
#        |
#        v
#   Primary ES from source :9200  (this branch: esql-elasticsearch-datasource)
#        |  ES|QL `elasticsearch` connector (HTTP + api_key)
#        v
#   Second single-node ES (Docker) :9201
#        ^
#        |  destination (logs-synth-default data stream)
#   node scripts/synthtrace.js distributed_unstructured_logs  <--- synthetic ECS logs, no credentials
#
# What it does:
#   1. Starts a second single-node Elasticsearch cluster in Docker on :9201 (HTTP, security on).
#   2. Starts the primary Elasticsearch from source (this branch) on :9200 via Kibana's `yarn es source`,
#      with the external-datasources feature flag enabled.
#   3. Starts Kibana from source (yarn start).
#   4. Mints an API key on the second cluster and registers an ES|QL `elasticsearch` data source +
#      dataset on the primary, pointing at the second cluster over plain HTTP (es:// scheme).
#   5. Feeds synthetic ECS logs into the second cluster with Kibana's synthtrace (no upstream cluster or
#      credentials): the `distributed_unstructured_logs` scenario writes a real `logs-synth-default`
#      data stream with ECS fields (log.level, host.*, service.*, message, data_stream.*).
#   6. Verifies the data is queryable through the data source with `FROM remote_logs | STATS COUNT(*)`.
#
# Data feed:
#   Kibana's synthtrace generates the data. synthtrace's CLI always resolves a Kibana/Fleet endpoint
#   (to look up the APM package version) and reuses the ES target's credentials for it, so the second
#   cluster's `elastic` password must match the primary's and we point synthtrace's --kibana at the
#   primary Kibana. The `distributed_unstructured_logs` scenario itself only uses the logs client, so
#   the (harmless) APM Fleet lookup is the only reason Kibana is involved.
#
# Stop everything with Ctrl-C; the Docker container and all background processes are cleaned up.

set -euo pipefail

# --------------------------------------------------------------------------------------------------
# Configuration (override via environment variables)
# --------------------------------------------------------------------------------------------------
ES_SOURCE_PATH="${ES_SOURCE_PATH:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
KIBANA_PATH="${KIBANA_PATH:-$(cd "${ES_SOURCE_PATH}/../kibana" 2>/dev/null && pwd || echo "${ES_SOURCE_PATH}/../kibana")}"
KIBANA_CONFIG="${KIBANA_CONFIG:-${KIBANA_PATH}/config/kibana.dev.yml}"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-${ES_SOURCE_PATH}/../gcs-credentials.json}"

# Primary ES (from source) — reached by Kibana and where the data source/dataset live.
PRIMARY_HOST="${PRIMARY_HOST:-http://localhost:9200}"
PRIMARY_USER="${PRIMARY_USER:-elastic}"
PRIMARY_PASS="${PRIMARY_PASS:-changeme}"

# Second ES (Docker) — the data-source target. Reached over plain HTTP (connector requires it).
# Its `elastic` password MUST match the primary's: synthtrace's CLI reuses the ES target credentials
# for the Kibana/Fleet endpoint (see header), and we point that endpoint at the primary Kibana.
SECOND_ES_IMAGE="${SECOND_ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:9.4.0}"
SECOND_CONTAINER="${SECOND_CONTAINER:-esql-ds-second}"
SECOND_PORT="${SECOND_PORT:-9201}"
SECOND_HOST="http://localhost:${SECOND_PORT}"
SECOND_USER="elastic"
SECOND_PASS="${SECOND_PASS:-${PRIMARY_PASS}}"

# Names of the registered data source / dataset, and the remote data stream synthtrace writes into.
DATA_SOURCE_NAME="${DATA_SOURCE_NAME:-second_cluster}"
DATASET_NAME="${DATASET_NAME:-remote_logs}"
# synthtrace's distributed_unstructured_logs scenario writes the logs-synth-default data stream.
TARGET_INDEX="${TARGET_INDEX:-logs-synth-default}"

# synthtrace data generation. The distributed_unstructured_logs scenario produces ECS-shaped logs into
# the logs-synth-default data stream with no upstream cluster or credentials. SYNTHTRACE_RATE controls
# logs/minute and SYNTHTRACE_FROM/_TO the historical window backfilled on startup.
SYNTHTRACE_SCENARIO="${SYNTHTRACE_SCENARIO:-distributed_unstructured_logs}"
SYNTHTRACE_RATE="${SYNTHTRACE_RATE:-10}"
SYNTHTRACE_MESSAGE_GROUP="${SYNTHTRACE_MESSAGE_GROUP:-httpAccess}"
SYNTHTRACE_FROM="${SYNTHTRACE_FROM:-now-6h}"
SYNTHTRACE_TO="${SYNTHTRACE_TO:-now}"

# Elastic Inference Service. `yarn start --eis` (and `yarn es source --eis`) require reachable EIS
# inference endpoints and abort Kibana with "No EIS inference endpoints found" when they are absent —
# which they are in a credentials-free local demo. EIS is unrelated to the ES|QL data source, so it is
# OFF by default; opt in with --eis (or USE_EIS=1) when you have EIS wired up.
USE_EIS="${USE_EIS:-0}"

# The `elasticsearch` data source stores its API key encrypted into cluster state. That requires the
# project encryption key (PEK), which the master only installs once a wrapping password is present in
# the keystore. `yarn es source` cannot inject keystore string values, so we add them to the running
# install's keystore and reload secure settings. ES_INSTALL_PATH is where `yarn es source` installs.
ES_INSTALL_PATH="${ES_INSTALL_PATH:-${KIBANA_PATH}/.es/source}"
ENCRYPTION_PASSWORD_ID="${ENCRYPTION_PASSWORD_ID:-demo}"
ENCRYPTION_PASSWORD="${ENCRYPTION_PASSWORD:-esql-datasource-demo-password}"

# How long (seconds) to wait for each service to become ready. The primary builds the full
# Elasticsearch distribution from source on first run, which can take well over 15 minutes on
# a cold Gradle cache, so the default is generous.
PRIMARY_TIMEOUT="${PRIMARY_TIMEOUT:-2400}"
SECOND_TIMEOUT="${SECOND_TIMEOUT:-180}"

# --------------------------------------------------------------------------------------------------
# Logging helpers
# --------------------------------------------------------------------------------------------------
log()  { printf '\033[1;34m[demo]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[demo]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[demo]\033[0m %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

# --------------------------------------------------------------------------------------------------
# Teardown: stop background processes and the Docker container on exit / Ctrl-C
# --------------------------------------------------------------------------------------------------
PRIMARY_PID=""
KIBANA_PID=""

# Recursively terminate a process and all of its descendants (gradle/node/yarn fork children
# that would otherwise be orphaned and keep ports bound).
kill_tree() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  local child
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    kill_tree "$child"
  done
  kill "$pid" 2>/dev/null || true
}

CLEANUP_DONE=0
cleanup() {
  [[ "$CLEANUP_DONE" == "1" ]] && return
  CLEANUP_DONE=1
  # Ignore further INT/TERM while we tear down so a second Ctrl-C cannot abort cleanup midway;
  # only the EXIT trap remains, which is harmless once CLEANUP_DONE is set.
  trap '' INT TERM
  log "Shutting down..."
  # Remove the Docker container first: a later signal to our own process group must not
  # abort this function before the container is cleaned up.
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$SECOND_CONTAINER"; then
    log "Removing Docker container ${SECOND_CONTAINER}..."
    docker rm -f "$SECOND_CONTAINER" >/dev/null 2>&1 || true
  fi
  # Terminate the background services and their descendants (gradle/node spawn child processes).
  for pid in "$KIBANA_PID" "$PRIMARY_PID"; do
    [[ -n "$pid" ]] || continue
    kill_tree "$pid"
  done
  log "Done."
}
trap cleanup INT TERM EXIT

# --------------------------------------------------------------------------------------------------
# Node version: Kibana's yarn/node tooling requires the exact version in kibana/.nvmrc.
# A non-interactive shell does not load the nvm shell functions, so we resolve the matching
# nvm-installed Node and prepend it to PATH ourselves (mirrors what `nvm use` would do).
# --------------------------------------------------------------------------------------------------
setup_node() {
  local nvmrc="$KIBANA_PATH/.nvmrc"
  [[ -f "$nvmrc" ]] || { warn "No .nvmrc in $KIBANA_PATH; using node on PATH."; return; }
  local required
  required="$(tr -d ' \t\r\n' < "$nvmrc")"
  required="${required#v}"

  # Already on the right version?
  if command -v node >/dev/null 2>&1 && [[ "$(node -v 2>/dev/null)" == "v${required}" ]]; then
    log "Using Node v${required} (already on PATH)."
    return
  fi

  local nvm_root="${NVM_DIR:-$HOME/.nvm}"
  local node_bin="${nvm_root}/versions/node/v${required}/bin"
  if [[ -x "${node_bin}/node" ]]; then
    PATH="${node_bin}:${PATH}"
    export PATH
    log "Using Node v${required} from ${node_bin}."
    return
  fi

  warn "Node v${required} (required by ${nvmrc}) not found under ${nvm_root}/versions/node."
  warn "Install it with: nvm install ${required}    (continuing with $(node -v 2>/dev/null || echo 'no node') — yarn may refuse to run)."
}

# --------------------------------------------------------------------------------------------------
# Prerequisite checks
# --------------------------------------------------------------------------------------------------
check_prereqs() {
  log "Checking prerequisites..."
  command -v docker >/dev/null 2>&1 || die "docker is required but not found on PATH."
  docker info >/dev/null 2>&1 || die "Docker daemon is not running. Start Docker and retry."
  command -v curl >/dev/null 2>&1 || die "curl is required but not found on PATH."

  [[ -d "$ES_SOURCE_PATH" ]]   || die "Elasticsearch source path not found: $ES_SOURCE_PATH"
  [[ -d "$KIBANA_PATH" ]]      || die "Kibana path not found: $KIBANA_PATH"
  [[ -f "$KIBANA_CONFIG" ]]    || die "Kibana config not found: $KIBANA_CONFIG"
  [[ -f "$KIBANA_PATH/scripts/synthtrace.js" ]] || die "synthtrace.js not found under $KIBANA_PATH/scripts"

  setup_node
  command -v node >/dev/null 2>&1 || die "node is required but not found on PATH."
  command -v yarn >/dev/null 2>&1 || die "yarn is required but not found on PATH."

  if [[ -z "${JAVA_HOME:-}" ]]; then
    warn "JAVA_HOME is not set; 'yarn es source' needs a JDK (JDK 25 for this branch)."
  fi
  if [[ ! -f "$GCS_CREDENTIALS_FILE" ]]; then
    warn "GCS credentials file not found ($GCS_CREDENTIALS_FILE); continuing without --secure-files."
    GCS_CREDENTIALS_FILE=""
  fi
}

# --------------------------------------------------------------------------------------------------
# Second cluster (Docker)
# --------------------------------------------------------------------------------------------------
start_second_cluster() {
  log "Starting second Elasticsearch cluster in Docker (${SECOND_ES_IMAGE}) on :${SECOND_PORT}..."
  if docker ps -a --format '{{.Names}}' | grep -qx "$SECOND_CONTAINER"; then
    log "Removing pre-existing container ${SECOND_CONTAINER}..."
    docker rm -f "$SECOND_CONTAINER" >/dev/null 2>&1 || true
  fi

  docker run -d --name "$SECOND_CONTAINER" \
    -p "${SECOND_PORT}:9200" \
    -e "discovery.type=single-node" \
    -e "ELASTIC_PASSWORD=${SECOND_PASS}" \
    -e "xpack.security.enabled=true" \
    -e "xpack.security.http.ssl.enabled=false" \
    -e "xpack.license.self_generated.type=trial" \
    -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
    "$SECOND_ES_IMAGE" >/dev/null

  log "Waiting for second cluster to become healthy (timeout ${SECOND_TIMEOUT}s)..."
  local deadline=$(( $(date +%s) + SECOND_TIMEOUT ))
  until curl -sf -u "${SECOND_USER}:${SECOND_PASS}" "${SECOND_HOST}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; do
    if [[ $(date +%s) -ge $deadline ]]; then
      err "Second cluster did not become healthy in time. Recent container logs:"
      docker logs --tail 40 "$SECOND_CONTAINER" >&2 || true
      die "Aborting."
    fi
    sleep 3
  done
  log "Second cluster is up at ${SECOND_HOST}."
  # synthtrace installs its own composable data-stream templates for logs-synth-* on the target, so no
  # template needs to be created here.
}

# --------------------------------------------------------------------------------------------------
# Primary ES from source + Kibana
# --------------------------------------------------------------------------------------------------
start_primary_es() {
  log "Starting primary Elasticsearch from source (${ES_SOURCE_PATH}) on ${PRIMARY_HOST}..."
  local -a es_args=(
    es source
    "--source-path=${ES_SOURCE_PATH}"
    --license trial
    -E network.host=0.0.0.0
    -Des.esql_external_datasources_feature_flag_enabled=true
  )
  [[ "$USE_EIS" == "1" ]] && es_args+=(--eis)
  if [[ -n "$GCS_CREDENTIALS_FILE" ]]; then
    es_args+=(--secure-files "gcs.client.default.credentials_file=${GCS_CREDENTIALS_FILE}")
  fi

  ( cd "$KIBANA_PATH" && exec yarn "${es_args[@]}" ) &
  PRIMARY_PID=$!

  log "Waiting for primary cluster to become healthy (timeout ${PRIMARY_TIMEOUT}s; first build can be slow)..."
  local deadline=$(( $(date +%s) + PRIMARY_TIMEOUT ))
  until curl -sf -u "${PRIMARY_USER}:${PRIMARY_PASS}" "${PRIMARY_HOST}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; do
    if ! kill -0 "$PRIMARY_PID" 2>/dev/null; then
      die "Primary Elasticsearch process exited before becoming healthy."
    fi
    if [[ $(date +%s) -ge $deadline ]]; then
      die "Primary cluster did not become healthy in time."
    fi
    sleep 5
  done
  log "Primary cluster is up at ${PRIMARY_HOST}."
}

start_kibana() {
  log "Starting Kibana from source..."
  local eis_flag=""
  [[ "$USE_EIS" == "1" ]] && eis_flag="--eis"
  ( cd "$KIBANA_PATH" \
      && KBN_USE_RSPACK=true yarn kbn bootstrap \
      && exec yarn start ${eis_flag:+$eis_flag} --config "$KIBANA_CONFIG" --no-base-path ) &
  KIBANA_PID=$!
  log "Kibana is starting (this takes a while); it will be available at http://localhost:5601."
}

# --------------------------------------------------------------------------------------------------
# Bootstrap the project encryption key on the primary.
# The master installs the PEK only when a wrapping password is configured in the keystore, so we add
# the two required secure settings to the running install and reload secure settings. Without this the
# data source PUT fails with 503 EncryptionKeyNotYetAvailable ("project encryption key is not yet available").
# --------------------------------------------------------------------------------------------------
bootstrap_encryption_key() {
  local keystore="${ES_INSTALL_PATH}/bin/elasticsearch-keystore"
  if [[ ! -x "$keystore" ]]; then
    warn "elasticsearch-keystore not found at ${keystore}; skipping PEK bootstrap (data source PUT may 503)."
    return
  fi
  log "Configuring project encryption key password in the primary keystore..."
  # -f overwrites without prompting; --stdin reads the value so it never appears in the process list.
  printf '%s' "$ENCRYPTION_PASSWORD" \
    | "$keystore" add -f --stdin "cluster.state.encryption.password.${ENCRYPTION_PASSWORD_ID}" \
    || die "Failed to add encryption password to the keystore."
  printf '%s' "$ENCRYPTION_PASSWORD_ID" \
    | "$keystore" add -f --stdin "cluster.state.encryption.active_password_id" \
    || die "Failed to add active_password_id to the keystore."

  log "Reloading secure settings so the master installs the project encryption key..."
  curl -sf -u "${PRIMARY_USER}:${PRIMARY_PASS}" \
    -H 'Content-Type: application/json' \
    -X POST "${PRIMARY_HOST}/_nodes/reload_secure_settings" \
    -d '{}' >/dev/null || die "Failed to reload secure settings on the primary."
  log "Project encryption key bootstrap requested."
}

# --------------------------------------------------------------------------------------------------
# Wire the data source on the primary, pointing at the second cluster
# --------------------------------------------------------------------------------------------------
mint_second_cluster_api_key() {
  log "Minting an API key on the second cluster..."
  local response
  response=$(curl -sf -u "${SECOND_USER}:${SECOND_PASS}" \
    -H 'Content-Type: application/json' \
    -X POST "${SECOND_HOST}/_security/api_key" \
    -d '{"name":"esql-datasource-demo","role_descriptors":{}}') \
    || die "Failed to create API key on the second cluster."

  # Extract the base64 "encoded" field without requiring jq.
  SECOND_API_KEY=$(printf '%s' "$response" | node -e \
    'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const o=JSON.parse(s);if(!o.encoded){console.error(s);process.exit(1);}process.stdout.write(o.encoded);})') \
    || die "Could not parse API key response: $response"
  [[ -n "$SECOND_API_KEY" ]] || die "Empty API key returned from second cluster."
  log "API key created."
}

# PUT JSON to the primary, retrying on transient startup conditions (the data source stores its
# API key encrypted into cluster state, and the project encryption key is not available for the
# first few seconds after the cluster reports healthy -> a 503 EncryptionKeyNotYetAvailable).
# Args: <description> <url> <json-body>. Dies after the retry budget is exhausted.
primary_put_with_retry() {
  local what="$1" url="$2" body="$3"
  local attempts=60 i=1 http_code response_body tmp
  tmp="$(mktemp)"
  while (( i <= attempts )); do
    http_code="$(curl -s -o "$tmp" -w '%{http_code}' \
      -u "${PRIMARY_USER}:${PRIMARY_PASS}" \
      -H 'Content-Type: application/json' \
      -X PUT "$url" -d "$body" 2>/dev/null || echo 000)"
    response_body="$(cat "$tmp")"
    if [[ "$http_code" =~ ^2 ]]; then
      rm -f "$tmp"
      return 0
    fi
    if [[ "$http_code" == "503" ]] || printf '%s' "$response_body" | grep -q "EncryptionKeyNotYetAvailable"; then
      (( i % 5 == 1 )) && log "  ${what}: waiting for encryption key to become available (attempt ${i}/${attempts})..."
      sleep 3
      (( i++ ))
      continue
    fi
    rm -f "$tmp"
    die "Failed to ${what} (HTTP ${http_code}): ${response_body}"
  done
  rm -f "$tmp"
  die "Failed to ${what}: still HTTP ${http_code} after ${attempts} attempts: ${response_body}"
}

register_data_source() {
  log "Registering ES|QL data source '${DATA_SOURCE_NAME}' on the primary..."
  primary_put_with_retry \
    "register data source" \
    "${PRIMARY_HOST}/_query/data_source/${DATA_SOURCE_NAME}" \
    "{\"type\":\"elasticsearch\",\"description\":\"Second cluster (demo)\",\"settings\":{\"api_key\":\"${SECOND_API_KEY}\"}}"

  # The dataset resource is the remote data stream synthtrace writes into (logs-synth-default).
  log "Registering ES|QL dataset '${DATASET_NAME}' -> es://localhost:${SECOND_PORT}/${TARGET_INDEX}..."
  primary_put_with_retry \
    "register dataset" \
    "${PRIMARY_HOST}/_query/dataset/${DATASET_NAME}" \
    "{\"data_source\":\"${DATA_SOURCE_NAME}\",\"resource\":\"es://localhost:${SECOND_PORT}/${TARGET_INDEX}\",\"settings\":{}}"
  log "Data source and dataset registered."
}

# --------------------------------------------------------------------------------------------------
# Generate synthetic logs into the second cluster with Kibana's synthtrace
# --------------------------------------------------------------------------------------------------
# synthtrace backfills a fixed historical window and exits (it is not a tail), so we run it
# synchronously and let verify_via_data_source confirm the docs landed in logs-synth-default.
#
# synthtrace's CLI always resolves a Kibana/Fleet endpoint to look up the APM package version, and it
# reuses the ES target's credentials for that endpoint. The second cluster has no Kibana, so we point
# --kibana at the PRIMARY Kibana; this is why SECOND_PASS must equal PRIMARY_PASS. The
# distributed_unstructured_logs scenario uses only the logs client, so nothing APM is generated — the
# Fleet lookup is the sole reason Kibana is involved and is otherwise a no-op for our data.
start_feed() {
  log "Generating synthetic logs into the second cluster via synthtrace ('${SYNTHTRACE_SCENARIO}', ${SYNTHTRACE_FROM}..${SYNTHTRACE_TO})..."
  (
    cd "$KIBANA_PATH"
    exec node scripts/synthtrace.js "$SYNTHTRACE_SCENARIO" \
      --target="http://${SECOND_USER}:${SECOND_PASS}@localhost:${SECOND_PORT}" \
      --kibana="http://${PRIMARY_USER}:${PRIMARY_PASS}@localhost:5601" \
      --scenarioOpts.distribution=uniform \
      --scenarioOpts.rate="$SYNTHTRACE_RATE" \
      --scenarioOpts.messageGroup="$SYNTHTRACE_MESSAGE_GROUP" \
      --from="$SYNTHTRACE_FROM" \
      --to="$SYNTHTRACE_TO"
  ) || die "synthtrace failed to generate synthetic logs into the second cluster."
  log "Synthetic log generation complete."
}

# --------------------------------------------------------------------------------------------------
# Verify the data is queryable through the data source
# --------------------------------------------------------------------------------------------------
verify_via_data_source() {
  # The remote data stream synthtrace wrote into.
  local count_index="${TARGET_INDEX}"
  log "Waiting for documents to land in the second cluster (${count_index})..."
  local deadline=$(( $(date +%s) + 180 ))
  local count=0
  while [[ $(date +%s) -lt $deadline ]]; do
    count=$(curl -sf -u "${SECOND_USER}:${SECOND_PASS}" \
      "${SECOND_HOST}/${count_index}/_count" 2>/dev/null \
      | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{process.stdout.write(String((JSON.parse(s).count)||0))}catch(e){process.stdout.write("0")}})' 2>/dev/null || echo 0)
    if [[ "${count:-0}" -gt 0 ]]; then
      break
    fi
    sleep 5
  done

  if [[ "${count:-0}" -le 0 ]]; then
    warn "No documents in ${count_index} on the second cluster yet."
    warn "The data source is still registered; once logs arrive they will be queryable."
    return
  fi

  log "Second cluster has ${count} document(s) in ${count_index}. Querying through the data source on the primary..."
  log "Query: FROM ${DATASET_NAME} | STATS c = COUNT(*)"
  curl -sf -u "${PRIMARY_USER}:${PRIMARY_PASS}" \
    -H 'Content-Type: application/json' \
    -X POST "${PRIMARY_HOST}/_query?format=txt" \
    -d "{\"query\":\"FROM ${DATASET_NAME} | STATS c = COUNT(*)\"}" \
    || warn "Query through the data source failed (see output above)."
  echo
}

# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --eis)        USE_EIS="1" ;;
      --no-eis)     USE_EIS="0" ;;
      -h|--help)
        grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
        exit 0
        ;;
      *) die "Unknown argument: $1 (supported: --eis, --no-eis, --help)" ;;
    esac
    shift
  done
}

main() {
  parse_args "$@"
  check_prereqs
  start_second_cluster
  start_primary_es
  start_kibana
  bootstrap_encryption_key
  mint_second_cluster_api_key
  register_data_source
  start_feed
  verify_via_data_source

  log "All set. Topology is running:"
  log "  - Kibana:          http://localhost:5601"
  log "  - Primary ES:      ${PRIMARY_HOST}  (data source '${DATA_SOURCE_NAME}', dataset '${DATASET_NAME}')"
  log "  - Second ES:       ${SECOND_HOST}   (data stream '${TARGET_INDEX}', synthtrace)"
  log "Try in Kibana / Console:  FROM ${DATASET_NAME} | LIMIT 10"
  log "Press Ctrl-C to stop everything and clean up."
  wait
}

main "$@"
