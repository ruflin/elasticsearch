#!/usr/bin/env bash
#
# ES|QL elasticsearch data source — local two-cluster test harness.
#
# Spins up two Elasticsearch clusters locally and wires one as an external data source of the other,
# then ingests 20,000 synthetic log documents and runs a suite of ES|QL queries through the connector.
# Self-contained: it does NOT depend on Kibana (unlike scripts/dev/esql-datasource-demo.sh) — logs are
# generated and bulk-indexed directly by this script.
#
#   Primary ES from source :9200   (this branch: the `elasticsearch` connector + data-source APIs)
#        |  ES|QL `elasticsearch` connector (HTTP + api_key)
#        v
#   Remote single-node ES (Docker) :9201   <--- 20,000 synthetic logs bulk-indexed here
#
# What it does:
#   1. Starts the remote single-node Elasticsearch in Docker on :9201 (HTTP, security on).
#   2. Bulk-indexes 20,000 synthetic log docs into the remote (fields mirror logs-*: @timestamp,
#      message, data_stream.dataset, host.name, service.name, log.level, http.response.status_code,
#      event.duration). Distinct values per field so grouped aggregations have multiple groups.
#   3. Starts the primary Elasticsearch from source on :9200 via `./gradlew :run`, with the
#      external-datasources feature flag enabled and a project-encryption-key password in its keystore
#      (the data source stores its API key encrypted into cluster state, which needs the PEK).
#   4. Mints an API key on the remote cluster and registers an ES|QL `elasticsearch` data source +
#      dataset on the primary, pointing at the remote over plain HTTP (es:// scheme).
#   5. Runs a verification suite of ES|QL queries through the data source, including the currently
#      known pushdown gaps (BUCKET / DATE_TRUNC / computed EVAL key / multi-field grouping) so this
#      script doubles as a manual playground and an automated smoke test for the pushdown work.
#
# Modes:
#   (default)   Full setup + run the verification suite, then keep both clusters up for manual testing.
#   --ci        Full setup + verification suite, then tear everything down and exit non-zero on failure.
#   --no-run    Skip starting the primary from source; only start + populate the remote (:9201). Useful
#               when you already have a primary running (e.g. from your IDE) and just want remote data.
#
# Stop everything with Ctrl-C; the Docker container and the gradle :run process are cleaned up.
#
# Examples:
#   scripts/dev/esql-two-cluster-test.sh                # interactive playground
#   scripts/dev/esql-two-cluster-test.sh --ci           # one-shot automated smoke test
#   DOC_COUNT=50000 scripts/dev/esql-two-cluster-test.sh

set -euo pipefail

# --------------------------------------------------------------------------------------------------
# Configuration (override via environment variables)
# --------------------------------------------------------------------------------------------------
ES_SOURCE_PATH="${ES_SOURCE_PATH:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

# Primary ES (from source) — runs this branch and hosts the data source/dataset.
PRIMARY_HOST="${PRIMARY_HOST:-http://localhost:9200}"
PRIMARY_USER="${PRIMARY_USER:-elastic-admin}"
PRIMARY_PASS="${PRIMARY_PASS:-elastic-password}"

# Remote ES (Docker) — the data-source target. Reached over plain HTTP (connector defaults to http).
REMOTE_ES_IMAGE="${REMOTE_ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:9.4.0}"
REMOTE_CONTAINER="${REMOTE_CONTAINER:-esql-2c-remote}"
REMOTE_PORT="${REMOTE_PORT:-9201}"
REMOTE_HOST="http://localhost:${REMOTE_PORT}"
REMOTE_USER="elastic"
REMOTE_PASS="${REMOTE_PASS:-changeme-remote}"

# Names of the registered data source / dataset, and the index we populate on the remote cluster.
DATA_SOURCE_NAME="${DATA_SOURCE_NAME:-remote_cluster}"
DATASET_NAME="${DATASET_NAME:-remote_logs}"
# logs-foo-bar matches the built-in logs-*-* naming and the index is created in LogsDB mode
# (index.mode: logsdb) so the remote stores the synthetic logs the same way a real logs data stream does.
TARGET_INDEX="${TARGET_INDEX:-logs-foo-bar}"

# Number of synthetic documents to bulk-index into the remote cluster, and the bulk batch size.
DOC_COUNT="${DOC_COUNT:-20000}"
BULK_BATCH="${BULK_BATCH:-2000}"

# The `elasticsearch` data source stores its API key encrypted into cluster state, which requires the
# project encryption key (PEK). The master installs the PEK only when a wrapping password is present in
# the keystore, so we add it to the running :run install's keystore and reload secure settings.
ENCRYPTION_PASSWORD_ID="${ENCRYPTION_PASSWORD_ID:-twoclustertest}"
ENCRYPTION_PASSWORD="${ENCRYPTION_PASSWORD:-esql-two-cluster-test-password}"

# How long (seconds) to wait for each service to become ready. The primary builds from source on the
# first run, which can take a long time on a cold Gradle cache.
PRIMARY_TIMEOUT="${PRIMARY_TIMEOUT:-2400}"
REMOTE_TIMEOUT="${REMOTE_TIMEOUT:-180}"

# Modes
MODE_CI=0
START_PRIMARY=1
for arg in "$@"; do
  case "$arg" in
    --ci) MODE_CI=1 ;;
    --no-run) START_PRIMARY=0 ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

# --------------------------------------------------------------------------------------------------
# Logging helpers
# --------------------------------------------------------------------------------------------------
log()  { printf '\033[1;34m[2c]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[2c]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[2c]\033[0m %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

PASS_COUNT=0
FAIL_COUNT=0
pass() { printf '\033[1;32m[2c] PASS\033[0m %s\n' "$*"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { printf '\033[1;31m[2c] FAIL\033[0m %s\n' "$*" >&2; FAIL_COUNT=$((FAIL_COUNT + 1)); }

# --------------------------------------------------------------------------------------------------
# Teardown
# --------------------------------------------------------------------------------------------------
PRIMARY_PID=""

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
  trap '' INT TERM
  log "Shutting down..."
  if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$REMOTE_CONTAINER"; then
    log "Removing Docker container ${REMOTE_CONTAINER}..."
    docker rm -f "$REMOTE_CONTAINER" >/dev/null 2>&1 || true
  fi
  [[ -n "$PRIMARY_PID" ]] && kill_tree "$PRIMARY_PID"
  log "Done."
}

# In CI mode we always tear down; in interactive mode we keep the clusters up for manual testing and
# only clean up on Ctrl-C.
if [[ "$MODE_CI" == "1" ]]; then
  trap cleanup INT TERM EXIT
else
  trap cleanup INT TERM
fi

# --------------------------------------------------------------------------------------------------
# Prerequisite checks
# --------------------------------------------------------------------------------------------------
check_prereqs() {
  log "Checking prerequisites..."
  command -v docker >/dev/null 2>&1 || die "docker is required but not found on PATH."
  docker info >/dev/null 2>&1 || die "Docker daemon is not running. Start Docker and retry."
  command -v curl >/dev/null 2>&1 || die "curl is required but not found on PATH."
  [[ -d "$ES_SOURCE_PATH" ]] || die "Elasticsearch source path not found: $ES_SOURCE_PATH"
  if [[ "$START_PRIMARY" == "1" && -z "${JAVA_HOME:-}" ]]; then
    warn "JAVA_HOME is not set; './gradlew :run' needs a JDK (JDK 25 for this branch)."
  fi
}

# --------------------------------------------------------------------------------------------------
# Remote cluster (Docker) + synthetic data
# --------------------------------------------------------------------------------------------------
start_remote_cluster() {
  log "Starting remote Elasticsearch in Docker (${REMOTE_ES_IMAGE}) on :${REMOTE_PORT}..."
  if docker ps -a --format '{{.Names}}' | grep -qx "$REMOTE_CONTAINER"; then
    log "Removing pre-existing container ${REMOTE_CONTAINER}..."
    docker rm -f "$REMOTE_CONTAINER" >/dev/null 2>&1 || true
  fi

  docker run -d --name "$REMOTE_CONTAINER" \
    -p "${REMOTE_PORT}:9200" \
    -e "discovery.type=single-node" \
    -e "ELASTIC_PASSWORD=${REMOTE_PASS}" \
    -e "xpack.security.enabled=true" \
    -e "xpack.security.http.ssl.enabled=false" \
    -e "xpack.license.self_generated.type=trial" \
    -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
    "$REMOTE_ES_IMAGE" >/dev/null

  log "Waiting for remote cluster to become healthy (timeout ${REMOTE_TIMEOUT}s)..."
  local deadline=$(( $(date +%s) + REMOTE_TIMEOUT ))
  until curl -sf -u "${REMOTE_USER}:${REMOTE_PASS}" "${REMOTE_HOST}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; do
    if [[ $(date +%s) -ge $deadline ]]; then
      err "Remote cluster did not become healthy in time. Recent container logs:"
      docker logs --tail 40 "$REMOTE_CONTAINER" >&2 || true
      die "Aborting."
    fi
    sleep 3
  done
  log "Remote cluster is up at ${REMOTE_HOST}."
}

# Registers an index template that backs the destination as a LogsDB data stream (index.mode: logsdb)
# with an explicit mapping so grouped aggregations and date functions behave deterministically (keyword
# group keys, a date @timestamp, numeric metrics). The logs-foo-bar name matches the built-in logs-*-*
# template, which creates data streams only, so we layer a higher-priority template and let the first
# bulk `create` auto-create the data stream. LogsDB is the storage mode a real logs data stream uses, so
# this exercises the connector against a realistic remote index layout rather than a plain index.
create_remote_index() {
  log "Registering LogsDB data-stream template for '${TARGET_INDEX}*' (index.mode: logsdb) on the remote cluster..."
  curl -sf -u "${REMOTE_USER}:${REMOTE_PASS}" \
    -H 'Content-Type: application/json' \
    -X PUT "${REMOTE_HOST}/_index_template/${TARGET_INDEX}-template" \
    -d "{
      \"index_patterns\": [\"${TARGET_INDEX}*\"],
      \"data_stream\": {},
      \"priority\": 500,
      \"template\": {
        \"settings\": { \"index.mode\": \"logsdb\" },
        \"mappings\": {
          \"properties\": {
            \"@timestamp\":                 { \"type\": \"date\" },
            \"message\":                    { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\" } } },
            \"data_stream.dataset\":        { \"type\": \"keyword\" },
            \"host.name\":                  { \"type\": \"keyword\" },
            \"service.name\":               { \"type\": \"keyword\" },
            \"log.level\":                  { \"type\": \"keyword\" },
            \"http.response.status_code\":  { \"type\": \"long\" },
            \"event.duration\":             { \"type\": \"long\" }
          }
        }
      }
    }" >/dev/null || die "Failed to register LogsDB data-stream template on the remote cluster."
}

# Bulk-indexes DOC_COUNT synthetic log docs into the LogsDB data stream. Generates NDJSON in batches
# with awk (no node/jq needed) and posts each batch to the remote _bulk API using `create` actions
# (a data stream rejects `index` actions). Field cardinalities are small and fixed so grouped results
# have a predictable, multi-group shape.
ingest_synthetic_logs() {
  log "Bulk-indexing ${DOC_COUNT} synthetic log docs into '${TARGET_INDEX}' (batch ${BULK_BATCH})..."
  local now_ms
  now_ms="$(( $(date +%s) * 1000 ))"
  local indexed=0
  while (( indexed < DOC_COUNT )); do
    local batch=$(( DOC_COUNT - indexed ))
    (( batch > BULK_BATCH )) && batch=$BULK_BATCH
    awk -v start="$indexed" -v n="$batch" -v idx="$TARGET_INDEX" -v now="$now_ms" '
      BEGIN {
        split("nginx,apache,kafka,postgres,redis", datasets, ",");
        split("host-a,host-b,host-c,host-d", hosts, ",");
        split("checkout,search,cart,payments,catalog", services, ",");
        split("INFO,WARN,ERROR,DEBUG", levels, ",");
        split("200,201,400,404,500,503", codes, ",");
        for (i = 0; i < n; i++) {
          g = start + i;
          ds = datasets[(g % 5) + 1];
          host = hosts[(g % 4) + 1];
          svc = services[(g % 5) + 1];
          lvl = levels[(g % 4) + 1];
          code = codes[(g % 6) + 1];
          # Spread timestamps over the last ~12h so BUCKET / DATE_TRUNC produce several buckets.
          ts = now - ((g % 720) * 60000);
          dur = ((g * 37) % 5000) + 1;
          printf "{\"create\":{}}\n";
          printf "{\"@timestamp\":%d,\"message\":\"request %d on %s\",\"data_stream.dataset\":\"%s\",\"host.name\":\"%s\",\"service.name\":\"%s\",\"log.level\":\"%s\",\"http.response.status_code\":%s,\"event.duration\":%d}\n", ts, g, svc, ds, host, svc, lvl, code, dur;
        }
      }
    ' > /tmp/esql-2c-bulk.ndjson
    curl -sf -u "${REMOTE_USER}:${REMOTE_PASS}" \
      -H 'Content-Type: application/x-ndjson' \
      -X POST "${REMOTE_HOST}/${TARGET_INDEX}/_bulk" \
      --data-binary @/tmp/esql-2c-bulk.ndjson >/dev/null || die "Bulk index failed at offset ${indexed}."
    indexed=$(( indexed + batch ))
    printf '\r\033[1;34m[2c]\033[0m   indexed %d/%d' "$indexed" "$DOC_COUNT"
  done
  printf '\n'
  rm -f /tmp/esql-2c-bulk.ndjson
  curl -sf -u "${REMOTE_USER}:${REMOTE_PASS}" -X POST "${REMOTE_HOST}/${TARGET_INDEX}/_refresh" >/dev/null || true
  log "Bulk indexing complete."
}

# --------------------------------------------------------------------------------------------------
# Primary ES from source (./gradlew :run)
# --------------------------------------------------------------------------------------------------
# The PEK bootstrap needs the `elasticsearch-keystore` CLI, which `./gradlew :run` does NOT leave on disk
# (its testcluster work dir under build/testclusters/runTask-0 holds only config/, data/, logs/ — no bin/).
# The `localDistro` task installs an extracted distribution to build/distribution/local/elasticsearch-<ver>/
# which DOES contain bin/elasticsearch-keystore. We build it up front so bootstrap_encryption_key can find
# the CLI; most of the work is shared with the :run build and comes from the Gradle cache.
build_local_distro() {
  [[ "$START_PRIMARY" == "1" ]] || return 0
  if [[ -n "$(find_run_keystore)" ]]; then
    log "Local distribution already present; skipping localDistro build."
    return 0
  fi
  log "Building a local distribution (./gradlew localDistro) so the elasticsearch-keystore CLI is available..."
  ( cd "$ES_SOURCE_PATH" && ./gradlew localDistro --console=plain ) \
    || warn "localDistro build failed; PEK bootstrap may be skipped and the data source PUT may 503."
}

start_primary_es() {
  [[ "$START_PRIMARY" == "1" ]] || { log "--no-run: skipping primary from source."; return; }
  log "Starting primary Elasticsearch from source (${ES_SOURCE_PATH}) on ${PRIMARY_HOST} via ./gradlew :run..."
  log "First build can take a long time; watch this terminal for the embedded gradle output."
  # ./gradlew :run builds a SNAPSHOT distribution, in which feature flags are enabled by default (see
  # server FeatureFlag: snapshot builds default to enabled). So the external-datasources and
  # project-encryption-key feature flags the connector needs are already on — no -D flag required.
  #
  # The project encryption key (PEK) is installed by the master's KeyRotationCoordinator only when it
  # observes a configured active password — and that check runs on a cluster-state change OR on the PEK
  # "check_interval" tick (default 1 HOUR). Because we add the password to the keystore + reload AFTER the
  # node is up, there is no further cluster-state change to trigger the install, so without help the PEK
  # would not appear for an hour and the data-source PUT would 503 the whole time. We shrink the tick to 1s
  # via the RunTask custom-setting passthrough (-Dtests.es.<setting> -> node setting) so the install fires
  # within ~1s of the reload picking up the password.
  (
    cd "$ES_SOURCE_PATH"
    exec ./gradlew :run \
      --console=plain \
      -Drun.license_type=trial \
      -Dtests.es.xpack.encryption.key_rotation.check_interval=1s
  ) &
  PRIMARY_PID=$!

  log "Waiting for primary cluster to become healthy (timeout ${PRIMARY_TIMEOUT}s; first build can be slow)..."
  local deadline=$(( $(date +%s) + PRIMARY_TIMEOUT ))
  until curl -sf -u "${PRIMARY_USER}:${PRIMARY_PASS}" "${PRIMARY_HOST}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null 2>&1; do
    if ! kill -0 "$PRIMARY_PID" 2>/dev/null; then
      die "Primary Elasticsearch (./gradlew :run) exited before becoming healthy."
    fi
    if [[ $(date +%s) -ge $deadline ]]; then
      die "Primary cluster did not become healthy in time."
    fi
    sleep 5
  done
  log "Primary cluster is up at ${PRIMARY_HOST}."
}

# Locate the `elasticsearch-keystore` CLI binary. The `:run` testcluster does NOT keep the bin/ scripts
# in its work dir (build/testclusters/runTask-0 holds only config/, data/, logs/); the CLI lives in the
# extracted local distribution. Prefer the distro we build via `localDistro`, but fall back to scanning
# the testclusters tree in case a future layout keeps it there.
find_run_keystore() {
  local ks
  ks="$(/usr/bin/find "${ES_SOURCE_PATH}/build/distribution/local" -path '*/bin/elasticsearch-keystore' -type f 2>/dev/null | head -1)"
  [[ -z "$ks" ]] && ks="$(/usr/bin/find "${ES_SOURCE_PATH}/build/testclusters" -path '*/bin/elasticsearch-keystore' -type f 2>/dev/null | head -1)"
  printf '%s' "$ks"
}

# Locate the config dir of the running `:run` node — this is where its live elasticsearch.keystore lives.
# elasticsearch-keystore edits the keystore under ES_PATH_CONF, so we must point it at this dir (not the
# distro's own config/) for the edits to reach the running node.
find_run_config_dir() {
  local cfg
  cfg="$(/usr/bin/find "${ES_SOURCE_PATH}/build/testclusters" -path '*/config/elasticsearch.keystore' -type f 2>/dev/null | head -1)"
  [[ -n "$cfg" ]] && printf '%s' "$(dirname "$cfg")"
}

# Bootstrap the project encryption key on the primary so the data source PUT does not 503.
bootstrap_encryption_key() {
  [[ "$START_PRIMARY" == "1" ]] || return 0
  local keystore config_dir
  keystore="$(find_run_keystore)"
  config_dir="$(find_run_config_dir)"
  if [[ -z "$keystore" || ! -x "$keystore" ]]; then
    local msg="Could not locate the elasticsearch-keystore CLI (expected under build/distribution/local; run localDistro). Without it the PEK is never installed and the data source PUT 503s forever."
    [[ "$MODE_CI" == "1" ]] && die "$msg"
    warn "$msg Skipping PEK bootstrap."
    return
  fi
  if [[ -z "$config_dir" || ! -d "$config_dir" ]]; then
    local msg="Could not locate the :run node config dir (build/testclusters/*/config). Without it the PEK is never installed and the data source PUT 503s forever."
    [[ "$MODE_CI" == "1" ]] && die "$msg"
    warn "$msg Skipping PEK bootstrap."
    return
  fi
  log "Configuring project encryption key password in the primary keystore (config: ${config_dir})..."
  # ES_PATH_CONF makes the CLI edit the running node's keystore rather than the distro's own config/.
  printf '%s' "$ENCRYPTION_PASSWORD" \
    | ES_PATH_CONF="$config_dir" "$keystore" add -f --stdin "cluster.state.encryption.password.${ENCRYPTION_PASSWORD_ID}" \
    || { warn "Failed to add encryption password to the keystore."; return; }
  printf '%s' "$ENCRYPTION_PASSWORD_ID" \
    | ES_PATH_CONF="$config_dir" "$keystore" add -f --stdin "cluster.state.encryption.active_password_id" \
    || { warn "Failed to add active_password_id to the keystore."; return; }

  log "Reloading secure settings so the master installs the project encryption key..."
  curl -sf -u "${PRIMARY_USER}:${PRIMARY_PASS}" \
    -H 'Content-Type: application/json' \
    -X POST "${PRIMARY_HOST}/_nodes/reload_secure_settings" \
    -d '{}' >/dev/null || warn "Failed to reload secure settings on the primary."
}

# --------------------------------------------------------------------------------------------------
# Wire the data source on the primary, pointing at the remote cluster
# --------------------------------------------------------------------------------------------------
mint_remote_api_key() {
  log "Minting an API key on the remote cluster..."
  local response
  response=$(curl -sf -u "${REMOTE_USER}:${REMOTE_PASS}" \
    -H 'Content-Type: application/json' \
    -X POST "${REMOTE_HOST}/_security/api_key" \
    -d '{"name":"esql-two-cluster-test","role_descriptors":{}}') \
    || die "Failed to create API key on the remote cluster."
  # Extract the base64 "encoded" field without requiring jq/node.
  REMOTE_API_KEY="$(printf '%s' "$response" | sed -n 's/.*"encoded":"\([^"]*\)".*/\1/p')"
  [[ -n "$REMOTE_API_KEY" ]] || die "Could not parse API key from response: $response"
  log "API key created."
}

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
  [[ "$START_PRIMARY" == "1" ]] || { log "--no-run: skipping data source registration."; return; }
  log "Registering ES|QL data source '${DATA_SOURCE_NAME}' on the primary..."
  primary_put_with_retry \
    "register data source" \
    "${PRIMARY_HOST}/_query/data_source/${DATA_SOURCE_NAME}" \
    "{\"type\":\"elasticsearch\",\"description\":\"Remote cluster (two-cluster test)\",\"settings\":{\"api_key\":\"${REMOTE_API_KEY}\"}}"

  log "Registering ES|QL dataset '${DATASET_NAME}' -> es://localhost:${REMOTE_PORT}/${TARGET_INDEX}..."
  primary_put_with_retry \
    "register dataset" \
    "${PRIMARY_HOST}/_query/dataset/${DATASET_NAME}" \
    "{\"data_source\":\"${DATA_SOURCE_NAME}\",\"resource\":\"es://localhost:${REMOTE_PORT}/${TARGET_INDEX}\",\"settings\":{}}"
  log "Data source and dataset registered."
}

# --------------------------------------------------------------------------------------------------
# Verification suite
# --------------------------------------------------------------------------------------------------
# The suite is built around one idea: run a query through the connector (primary) and the SAME query
# directly against the remote, then compare the normalized JSON `values`. If they match, the connector
# returned the full, correct result; if they differ, the connector silently degraded (e.g. computed a
# STATS locally over its implicitly-capped first page). This is the gold-standard check the live ITs use
# and it catches the whole "capped page -> wrong total" class of bugs.
#
# Two assertion flavors:
#   assert_match        — a correctness invariant that MUST hold today. A mismatch is a real failure.
#   probe_gap           — a KNOWN pushdown gap. We still compare, but a mismatch is reported as
#                         "GAP (still open)" without failing CI, and an unexpected match is reported as
#                         "GAP CLOSED" (a signal to flip it into assert_match). This makes the script a
#                         living regression signal for the pushdown work rather than printing raw rows.

# Runs an ES|QL query through the primary connector and prints the txt result. Echoes the query first.
run_query() {
  local q="$1"
  echo
  log "Query: ${q}"
  curl -s -u "${PRIMARY_USER}:${PRIMARY_PASS}" \
    -H 'Content-Type: application/json' \
    -X POST "${PRIMARY_HOST}/_query?format=txt" \
    -d "{\"query\":\"${q}\"}"
}

# Runs an ES|QL query through the connector on the primary and prints the raw JSON response on stdout.
connector_json() {
  local q="$1"
  curl -s -u "${PRIMARY_USER}:${PRIMARY_PASS}" -H 'Content-Type: application/json' \
    -X POST "${PRIMARY_HOST}/_query?format=json" -d "{\"query\":\"${q}\"}"
}

# Runs an ES|QL query directly against the remote cluster (the data-source target) and prints the raw
# JSON response on stdout. The remote query targets TARGET_INDEX directly instead of the dataset name.
remote_json() {
  local q="$1"
  curl -s -u "${REMOTE_USER}:${REMOTE_PASS}" -H 'Content-Type: application/json' \
    -X POST "${REMOTE_HOST}/_query?format=json" -d "{\"query\":\"${q}\"}"
}

# Extracts and normalizes the `values` 2-D array from an ES|QL JSON response so two responses can be
# compared regardless of column-order/whitespace noise: emit one canonical line per row, rows sorted.
# Uses awk only (no jq/node dependency, matching the rest of the script). It strips everything up to
# "values":, then splits the array into rows on "],[" and prints each row trimmed, finally `sort`s them.
normalize_values() {
  awk '
    {
      v = $0
      i = index(v, "\"values\":")
      if (i == 0) { next }
      v = substr(v, i + 9)            # drop up to and including "values":
      # trim the trailing "}" / "]" wrapper noise; keep the outer [...] of values
      sub(/\][^]]*$/, "]", v)
      gsub(/^\[/, "", v); sub(/\]$/, "", v)   # remove the outermost [ ]
      gsub(/\],\[/, "]\n[", v)        # one row per line
      print v
    }
  ' | sed -E 's/^\[//; s/\]$//' | sort
}

# Returns 0 if the connector and direct-remote results for the given query are identical (normalized).
# Args: <connector-query> <remote-query>
results_match() {
  local cq="$1" rq="$2" c r
  c="$(connector_json "$cq" | normalize_values)"
  r="$(remote_json "$rq" | normalize_values)"
  [[ "$c" == "$r" ]]
}

# Correctness invariant: connector result MUST equal the direct-remote result.
# Args: <label> <connector-query> <remote-query>
assert_match() {
  local label="$1" cq="$2" rq="$3"
  if results_match "$cq" "$rq"; then
    pass "${label}"
  else
    fail "${label} — connector result differs from direct-remote result"
    warn "  connector: $(connector_json "$cq")"
    warn "  remote:    $(remote_json "$rq")"
  fi
}

# Known-gap probe: compare but do not fail CI. Reports whether the gap is still open or has been closed.
# Args: <label> <connector-query> <remote-query>
probe_gap() {
  local label="$1" cq="$2" rq="$3"
  if results_match "$cq" "$rq"; then
    printf '\033[1;32m[2c] GAP CLOSED\033[0m %s — connector now matches direct-remote; promote to assert_match.\n' "$label"
  else
    printf '\033[1;33m[2c] GAP (open)\033[0m %s — connector degrades vs direct-remote (expected until pushdown lands).\n' "$label"
  fi
}

# Asserts a scalar (single-cell) connector result equals an expected value.
# Args: <label> <connector-query> <expected>
assert_scalar() {
  local label="$1" cq="$2" expected="$3" out got
  out="$(connector_json "$cq")"
  got="$(printf '%s' "$out" | sed -n 's/.*"values":\[\[\([^]]*\)\]\].*/\1/p')"
  if [[ "$got" == "$expected" ]]; then
    pass "${label} (= ${expected})"
  else
    fail "${label} expected [${expected}], got [${got}] (raw: ${out})"
  fi
}

# Asserts a `... | LIMIT n` connector query returns exactly n rows. Args: <label> <connector-query> <n>
assert_row_count() {
  local label="$1" cq="$2" n="$3" out rows
  out="$(connector_json "$cq")"
  # Count rows by counting the row separators "],[" in values plus one (when values is non-empty).
  rows="$(printf '%s' "$out" | normalize_values | grep -c . || true)"
  if [[ "$rows" == "$n" ]]; then
    pass "${label} (= ${n} rows)"
  else
    fail "${label} expected ${n} rows, got ${rows} (raw: ${out})"
  fi
}

run_verification_suite() {
  [[ "$START_PRIMARY" == "1" ]] || { log "--no-run: skipping verification suite."; return; }
  log "=============================================================="
  log "Verification suite — connector result vs. direct-remote result"
  log "=============================================================="

  local D="${DATASET_NAME}" T="${TARGET_INDEX}"

  log "-- Correctness invariants (must pass today) ------------------"

  # 1. Ungrouped COUNT(*) is pushed down and computed over the full remote data set.
  assert_scalar "ungrouped COUNT(*) pushed down" "FROM ${D} | STATS c = COUNT(*)" "${DOC_COUNT}"

  # 2. Grouped COUNT by a single keyword field matches the direct-remote histogram exactly.
  assert_match "grouped COUNT BY keyword (data_stream.dataset)" \
    "FROM ${D} | STATS c = COUNT(*) BY \`data_stream.dataset\` | SORT \`data_stream.dataset\`" \
    "FROM ${T} | STATS c = COUNT(*) BY \`data_stream.dataset\` | SORT \`data_stream.dataset\`"

  # 3. The single-keyword grouped counts sum back to the full document count (no capped page).
  assert_scalar "grouped COUNT BY keyword sums to total" \
    "FROM ${D} | STATS c = COUNT(*) BY \`service.name\` | STATS t = SUM(c)" "${DOC_COUNT}"

  # 4. Pushed-down filter (== on a keyword) matches the direct-remote filtered count.
  assert_match "filtered COUNT (WHERE log.level == ERROR)" \
    "FROM ${D} | WHERE \`log.level\` == \\\"ERROR\\\" | STATS c = COUNT(*)" \
    "FROM ${T} | WHERE \`log.level\` == \\\"ERROR\\\" | STATS c = COUNT(*)"

  # 5. Numeric range filter (>=) matches direct-remote.
  assert_match "filtered COUNT (WHERE status_code >= 500)" \
    "FROM ${D} | WHERE \`http.response.status_code\` >= 500 | STATS c = COUNT(*)" \
    "FROM ${T} | WHERE \`http.response.status_code\` >= 500 | STATS c = COUNT(*)"

  # 6. Compound AND/OR filter matches direct-remote.
  assert_match "filtered COUNT (WHERE level==WARN OR code==404)" \
    "FROM ${D} | WHERE \`log.level\` == \\\"WARN\\\" OR \`http.response.status_code\` == 404 | STATS c = COUNT(*)" \
    "FROM ${T} | WHERE \`log.level\` == \\\"WARN\\\" OR \`http.response.status_code\` == 404 | STATS c = COUNT(*)"

  # 7. SORT + LIMIT (top-N projection) returns the same rows as the direct-remote query.
  assert_match "SORT + KEEP + LIMIT top-N" \
    "FROM ${D} | SORT \`event.duration\` DESC, \`message.keyword\` ASC | KEEP \`event.duration\`, \`host.name\` | LIMIT 25" \
    "FROM ${T} | SORT \`event.duration\` DESC, \`message.keyword\` ASC | KEEP \`event.duration\`, \`host.name\` | LIMIT 25"

  # 8. Plain LIMIT returns exactly the requested number of rows.
  assert_row_count "plain LIMIT returns n rows" "FROM ${D} | LIMIT 37" 37

  # 9. Grouped COUNT by TWO keyword fields matches the direct-remote result. Multi-field grouping is
  # pushed down on this branch (verified by the two-cluster harness), so this is a correctness invariant.
  assert_match "grouped COUNT BY two keyword fields" \
    "FROM ${D} | STATS c = COUNT(*) BY \`host.name\`, \`service.name\` | SORT \`host.name\`, \`service.name\`" \
    "FROM ${T} | STATS c = COUNT(*) BY \`host.name\`, \`service.name\` | SORT \`host.name\`, \`service.name\`"

  # 10. (match path: METADATA _id) The KI `match` rule reads back _id via FROM <dataset> METADATA _id. The
  # rewriter now forwards _id/_source to the elasticsearch connector, the connector renders FROM ... METADATA
  # on the remote leg, and _id (a keyword) decodes as before, so a deterministic _id slice must match direct.
  assert_match "METADATA _id read matches direct-remote" \
    "FROM ${D} METADATA _id | KEEP _id | SORT _id | LIMIT 50" \
    "FROM ${T} METADATA _id | KEEP _id | SORT _id | LIMIT 50"

  # 11. (match path: METADATA _source decode) Reading _source back through the connector must not throw and
  # must return the requested rows. The connector decodes the structured _source value into a JSON BytesRef;
  # asserting the exact row count proves decode succeeded (a decode failure would error or drop the column).
  assert_row_count "METADATA _id, _source read returns n rows" \
    "FROM ${D} METADATA _id, _source | KEEP _id, _source | LIMIT 13" 13

  log "-- Known pushdown gaps (probes; non-fatal) -------------------"
  # These compare connector vs direct-remote too, but a mismatch is expected until the pushdown work
  # lands. When one starts matching, the probe prints "GAP CLOSED" so it can be promoted to assert_match.

  # Grouped COUNT BY a time BUCKET — the SigEvents histogram core (function grouping not pushed).
  probe_gap "grouped COUNT BY BUCKET(@timestamp, 1 hour)" \
    "FROM ${D} | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour) | SORT b" \
    "FROM ${T} | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour) | SORT b"

  # Grouped COUNT BY DATE_TRUNC — same family, different function.
  probe_gap "grouped COUNT BY DATE_TRUNC(1 hour, @timestamp)" \
    "FROM ${D} | STATS c = COUNT(*) BY b = DATE_TRUNC(1 hour, @timestamp) | SORT b" \
    "FROM ${T} | STATS c = COUNT(*) BY b = DATE_TRUNC(1 hour, @timestamp) | SORT b"

  # Grouped COUNT BY a computed EVAL key (CONCAT) — computed grouping key not pushed.
  probe_gap "grouped COUNT BY computed EVAL key (CONCAT)" \
    "FROM ${D} | EVAL k = CONCAT(\`service.name\`, \\\"-\\\", \`log.level\`) | STATS c = COUNT(*) BY k | SORT k" \
    "FROM ${T} | EVAL k = CONCAT(\`service.name\`, \\\"-\\\", \`log.level\`) | STATS c = COUNT(*) BY k | SORT k"

  # Grouped metric aggregates (MIN/MAX/SUM/AVG) by keyword — only COUNT is pushed for grouped STATS today.
  probe_gap "grouped MAX/MIN metric BY keyword" \
    "FROM ${D} | STATS mx = MAX(\`event.duration\`), mn = MIN(\`event.duration\`) BY \`service.name\` | SORT \`service.name\`" \
    "FROM ${T} | STATS mx = MAX(\`event.duration\`), mn = MIN(\`event.duration\`) BY \`service.name\` | SORT \`service.name\`"

  echo
  log "Verification suite finished: ${PASS_COUNT} passed, ${FAIL_COUNT} failed (correctness assertions)."
  log "GAP lines above are informational: 'GAP (open)' = pushdown not yet implemented (expected),"
  log "'GAP CLOSED' = the connector now matches direct-remote and the probe should become an assertion."
}

# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------
main() {
  check_prereqs
  start_remote_cluster
  create_remote_index
  ingest_synthetic_logs
  build_local_distro
  start_primary_es
  bootstrap_encryption_key
  mint_remote_api_key
  register_data_source
  run_verification_suite

  if [[ "$MODE_CI" == "1" ]]; then
    if (( FAIL_COUNT > 0 )); then
      die "CI mode: ${FAIL_COUNT} assertion(s) failed."
    fi
    log "CI mode: all assertions passed. Tearing down."
    exit 0
  fi

  log "All set. Topology is running:"
  log "  - Primary ES (source):  ${PRIMARY_HOST}  (data source '${DATA_SOURCE_NAME}', dataset '${DATASET_NAME}')"
  log "  - Remote ES (Docker):   ${REMOTE_HOST}   (index '${TARGET_INDEX}', ${DOC_COUNT} docs)"
  log "Try:  curl -u ${PRIMARY_USER}:${PRIMARY_PASS} ${PRIMARY_HOST}/_query?format=txt -H 'Content-Type: application/json' -d '{\"query\":\"FROM ${DATASET_NAME} | LIMIT 10\"}'"
  log "Press Ctrl-C to stop everything and clean up."
  [[ "$START_PRIMARY" == "1" ]] && wait "$PRIMARY_PID" || wait
}

main
