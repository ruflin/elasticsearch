# ES|QL Elasticsearch External Data Source — Implementation Report

## 1. Overview

This work makes it possible for an ES|QL query running on one Elasticsearch cluster to read data
from **another, separate Elasticsearch cluster over HTTP** — without cross-cluster search (CCS) and
without a transport-level connection. The remote cluster is treated as an ES|QL *external data
source* of type `elasticsearch`, addressed by `es://host:port/index` URIs.

The motivation: connect a Serverless cluster to an ECH (Elastic Cloud Hosted) cluster (which CCS
cannot do today), and achieve looser version compatibility by operating purely over the HTTP
`_query` API rather than the binary transport protocol.

Delivered in two phases on branch `esql-elasticsearch-datasource`:

- **Phase 1** — the connector itself: schema resolution, query execution against the remote
  `_query` API, columnar-JSON decoding into ES|QL `Page`s, projection (`KEEP`) pushdown, and a
  two-cluster end-to-end test.
- **Phase 2** — best-effort **filter (`WHERE`)** and **limit (`LIMIT`)** pushdown so the remote
  cluster filters and truncates before sending data back over the wire.

## 2. Architecture

### Request flow

```
Local cluster (ES|QL coordinator)
  └─ EXTERNAL "es://remote:9200/index" | WHERE age > 30 | KEEP name, age | LIMIT 10
       │
       ▼ (local physical optimization on the coordinator)
  ExternalSourceResolver → resolves schema via remote `_query ... | LIMIT 0`
  PushLimitToExternalSource / PushFiltersToSource → attach pushedLimit + pushedExpressions
       │
       ▼
  OperatorFactoryRegistry → builds QueryRequest(target, projected, rowLimit, pushedFilters)
  AsyncConnectorSourceOperatorFactory → drives the connector
       │
       ▼ HTTP POST /_query  {"query":"FROM index | WHERE `age` > 30 | KEEP name, age | LIMIT 10","columnar":true}
Remote cluster
       │
       ▼ columnar JSON  ({columns:[...], values:[[...],[...]]})
  EsqlTypeMapping → decode columns/values into ES|QL Blocks → Page → ResultCursor
```

External (connector) sources execute **on the coordinator only**, which is why the pushed filter
expressions are produced and consumed in the same JVM and never serialized across the wire.

### Why a connector (pull), not push

The `elasticsearch` source is modeled as a **`ConnectorFactory`/`Connector`** (API-based pull), not
a byte-level `StorageProvider`. Schema and data are both fetched by issuing ES|QL queries to the
remote `_query` endpoint. Because the remote *is* an Elasticsearch cluster that already speaks
ES|QL, pushdown is a direct re-render of ES|QL fragments rather than a translation into a foreign
query DSL.

## 3. What was built

### New module: `x-pack/plugin/esql-datasource-elasticsearch`

| File | Responsibility |
|---|---|
| `ElasticsearchDataSourcePlugin` | Registers `es`/`elasticsearch` schemes; provides the `ConnectorFactory` and a placeholder `StorageProvider`. SPI-registered via `META-INF/services`. |
| `ElasticsearchConnectorFactory` | Parses `es://host:port/index`; resolves schema via remote `FROM <index> \| LIMIT 0`; validates config (`api_key`); builds the `RestClient`; exposes `filterPushdownSupport()`. |
| `ElasticsearchConnector` | Runs the remote `_query` over HTTP and decodes the columnar response into `Page`s. Owns `buildRemoteQuery()` which assembles `FROM … \| WHERE … \| KEEP … \| LIMIT …`. |
| `EsqlTypeMapping` | Maps remote ES|QL column types to local `DataType` and converts columnar JSON values into `Block`s. |
| `EsqlFilterTranslator` (Phase 2) | Decides pushability of filter expressions and re-renders the supported subset into a remote `WHERE` clause. |
| `ElasticsearchStorageProvider` | Minimal placeholder so the resolver's initial scheme dispatch succeeds; not byte-addressable. |

### Core ES|QL changes (Phase 2 plumbing)

Minimal shared-core edits needed to route limit and filters to connector sources:

- `QueryRequest` gained `rowLimit` and `pushedFilters` (the original ES|QL `Expression`s).
- `OperatorFactoryRegistry` forwards `context.rowLimit()` and `context.pushedExpressions()` into the
  `QueryRequest`.
- `ExternalOptimizerContext` now also carries the registered `sourceFactories`, threaded from
  `ComputeService`/`DataNodeComputeHandler` via `PlannerUtils.localPlan(...)`.
- `PushFiltersToSource` consults `ExternalSourceFactory.filterPushdownSupport()` for connector
  sources (file sources keep using `FormatReader.filterPushdownSupport()` unchanged).
- `DataSourceModule.LazyConnectorFactory` delegates `filterPushdownSupport()` to the wrapped factory.

### Pushdown safety model

The local plan keeps the `FilterExec`/`LimitExec` above the source as **safety nets**. Whatever the
connector pushes (or fails to push) can only change *how much data is transferred* — never the
correctness of results.

## 4. How to use it

### Enable the feature flag

The whole feature is gated behind the `esql_external_datasources` feature flag (system property
`es.esql_external_datasources_feature_flag_enabled=true`). It is a non-production/dev flag today.

### Install the plugin

The `esql-datasource-elasticsearch` plugin must be installed on the **local** (coordinating) cluster.

### Query syntax

Inline `EXTERNAL` command (currently dev-only), pointing at a remote cluster's HTTP endpoint and
index:

```esql
EXTERNAL "es://remote-host:9200/my-index"
| KEEP name, age
| SORT age
```

With filter and limit pushdown (Phase 2):

```esql
EXTERNAL "es://remote-host:9200/logs-*"
| WHERE age > 30 AND status == "active"
| KEEP name, age, status
| LIMIT 100
```

The connector turns that into the following remote query (sent to `remote-host:9200/_query`):

```esql
FROM logs-* | WHERE (`age` > 30 AND `status` == "active") | KEEP name, age, status | LIMIT 100
```

### Authentication

An API key can be supplied and is stored as an encrypted data-source secret. In the resolved
connector config it is the `api_key` key; the connector sends it as `Authorization: ApiKey <key>`.

### Supported column types (decoding)

`keyword`, `text`, `ip`, `version` → BytesRef; `long`, `datetime` → long; `integer`; `double`;
`boolean`. Unsupported types resolve to `UNSUPPORTED` — referencing such a column fails exactly as it
would for a local index, while other columns remain usable.

### Pushable predicates (Phase 2)

| Pushed to remote | Not pushed (stays local) |
|---|---|
| `==`, `!=`, `<`, `<=`, `>`, `>=` between a field and a **foldable literal** (either operand order) | comparisons between two fields |
| literal types: keyword/text, integer, long, double, boolean | dates, IPs, versions, multi-valued literals |
| `AND`, `OR`, `NOT` of pushable sub-expressions | functions, `IN`, `LIKE`, `IS NULL`, anything else |
| `LIMIT n`; projection via `KEEP` | — |

Anything not pushed simply stays in the local `FilterExec` and is applied on the coordinator.

## 5. Key decisions made along the way

1. **Source name = `elasticsearch`** (schemes `es://`, `elasticsearch://`). Scoped specifically to
   *pointing at another Elasticsearch cluster*.
2. **Pull via the remote `_query` API over HTTP**, not transport/CCS — enables Serverless↔ECH
   connectivity and looser version coupling.
3. **Connector model over storage model.** Registered as a `ConnectorFactory`; a minimal
   `StorageProvider` exists only to satisfy the resolver's initial scheme dispatch.
4. **Schema resolution via `FROM <index> | LIMIT 0`** rather than `_field_caps`, so the resolved
   schema matches exactly what a real query returns.
5. **Columnar JSON for v1**, decoded with `XContentParser.map()` — Arrow output was considered and
   deferred.
6. **Single cursor per query (no parallel splits) for v1.**
7. **Best-effort partial pushdown** with the local `FilterExec`/`LimitExec` kept as a correctness
   safety net; conservative predicate subset only.
8. **Filter pushdown wired through the existing optimizer rule** (`PushFiltersToSource`) by adding a
   connector fallback, rather than inventing a parallel path — file-source behavior is untouched.
9. **`QueryRequest` carries the original ES|QL `Expression`s** (not a pre-rendered string), so the
   connector — which natively speaks ES|QL — re-renders them itself.
10. **Minimal core blast radius**: backward-compatible `QueryRequest`/`ExternalOptimizerContext`
    constructors were added so existing call sites and tests compiled unchanged.

## 6. Testing

- **Unit:** `EsqlTypeMappingTests`, `ElasticsearchConnectorFactoryTests` (incl. `buildRemoteQuery`
  for `KEEP`/`WHERE`/`LIMIT`), `EsqlFilterTranslatorTests` (translation, operator flipping, string
  escaping, field-name quoting, AND/OR/NOT, pushability classification).
- **End-to-end** (`ElasticsearchExternalSourceIT`, two real `InternalTestCluster`s over real HTTP):
  basic pull, `testFilterPushdownReturnsSubset`, `testFilterPushdownOnKeyword`,
  `testLimitPushdownCapsRows`, `testFilterAndLimitPushdownTogether`.
- **Regression:** `PhysicalPlanOptimizerTests`, `PushFiltersToSourceTests`,
  `PushLimitToExternalSourceTests`, `DataSourceModuleTests`, `DataSourceModuleLazyLoadingTests`,
  `AsyncConnectorSourceOperatorFactorySplitTests` — all pass.

**Testing caveat:** the e2e tests assert correct *result subsets*. Because the safety-net
`FilterExec`/`LimitExec` would also produce correct results, those tests prove the feature works but
do **not** independently prove the predicate reached the remote. That proof currently comes from the
`buildRemoteQuery` unit tests (which assert the exact remote query string) plus the existing
optimizer-rule tests.

## 7. What is missing / not yet done

**Pushdown gaps**
- No **aggregation/STATS** pushdown to the remote.
- No **TopN/SORT** pushdown.
- Filter pushdown subset is deliberately narrow: no dates, IPs, versions, `IN`, `LIKE`/`RLIKE`,
  `IS NULL`, functions, or multi-valued literals.
- No explicit IT assertion that captures the **remote query string** to prove pushdown reached the
  remote (only inferred from unit tests).

**Execution / scale**
- **Single round-trip, single page** — no pagination/streaming of large result sets.
- **No parallelism** (single cursor per query; no split strategy).
- No retry/backoff, circuit-breaker accounting for the HTTP fetch, or timeout/cancellation
  propagation to the remote `_query`.

**Type coverage**
- `EsqlTypeMapping` covers common scalars only; `date_nanos`, geo types, `unsigned_long`,
  aggregate-metric, and multi-valued fields are unsupported (resolve to `UNSUPPORTED`).

**Security / transport**
- HTTP only — the scheme is normalized to `http` and **TLS/`https` is out of scope for v1**.
- API-key auth only (no basic auth / token / cloud-id); no per-request header customization.

**API surface & productization**
- The `EXTERNAL "…"` inline command is **dev-only**; the intended GA surface is **named
  Datasets/DataSources** (cluster-state objects), which still need to be wired for this connector.
- Entire feature is behind the `esql_external_datasources` flag — not production-ready.
- No user-facing docs, no REST/YAML spec tests, no benchmarks.

## 8. Commits on `esql-elasticsearch-datasource`

1. `[ESQL] Add elasticsearch external data source connector module` (Phase 1 scaffold + connector +
   type mapping)
2. `[ESQL] Wire elasticsearch connector end-to-end with two-cluster IT` (Phase 1 e2e)
3. `[ESQL] Push WHERE and LIMIT into elasticsearch connector queries` (Phase 2 pushdown)

## 9. Follow-ups / next steps

- [ ] Add an IT assertion that captures the remote `_query` body (e.g. via a request-recording proxy
      or remote slow-log) to *prove* pushdown reached the remote.
- [ ] Wire the connector to the **named Dataset/DataSource** API surface for GA.
- [ ] Add **STATS/aggregation** pushdown for connectors.
- [ ] Add **TopN/SORT** pushdown for connectors.
- [ ] Add **pagination/streaming** for large result sets.
- [ ] Add a **split/parallelism** strategy for large results.
- [ ] Add **TLS/`https`** support.
- [ ] Broaden **authentication** options (basic auth / token / cloud-id; per-request headers).
- [ ] Expand `EsqlTypeMapping` type coverage (`date_nanos`, geo, `unsigned_long`, aggregate-metric,
      multi-valued fields).
- [ ] Expand the filter pushdown subset (dates, IPs, versions, `IN`, `LIKE`/`RLIKE`, `IS NULL`).
- [ ] Add retry/backoff, circuit-breaker accounting, and timeout/cancellation propagation to the
      remote `_query`.
- [ ] Add user-facing docs, REST/YAML spec tests, and benchmarks.
