/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Live end-to-end test that points the {@code elasticsearch} external data source at a <em>real</em>
 * remote cluster (e.g. Elastic Cloud / serverless) over HTTPS and compares results against the same
 * queries run directly through the cluster's {@code _query} API.
 * <p>
 * The remote endpoint and API key are supplied via system properties so no secret is committed:
 * <pre>
 *   -Dtests.esql.remote.url=https://&lt;host&gt;
 *   -Dtests.esql.remote.apikey=&lt;base64 api key&gt;
 *   [-Dtests.esql.remote.target=logs*]
 * </pre>
 * The test self-skips when {@code tests.esql.remote.url} is not set, so it never runs (or fails) in CI.
 */
public class ElasticsearchExternalSourceLiveIT extends AbstractElasticsearchLiveIT {

    /**
     * {@code EXTERNAL "es+https://host/target" [WITH {"api_key": "..."}]} prefix — derives the +https
     * scheme and host from the configured URL and passes the API key inline via the WITH options map.
     */
    private String externalSource() {
        String hostPort = URL.replaceFirst("^https?://", "");
        String location = "es+https://" + hostPort + "/" + TARGET;
        String prefix = "EXTERNAL \"" + location + "\"";
        if (API_KEY != null) {
            prefix += " WITH {\"api_key\": \"" + API_KEY + "\"}";
        }
        return prefix;
    }

    public void testSchemaResolvesViaConnector() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // Read a single row through the connector across the full (wide, mixed-type) schema. This exercises
        // schema resolution, the +https transport, auth, the index pattern, and decoding of every column
        // type the remote returns (including unsupported/date_nanos columns, which must not fail the query).
        List<List<Object>> rows = runExternal(externalSource() + " | LIMIT 1");
        assertThat(rows.size(), equalTo(1));
        assertThat("each row spans the full resolved schema", rows.get(0).size(), greaterThan(50));
    }

    public void testBoundedProjectedReadIsWellFormed() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // A bounded, projected read returns exactly the requested number of rows with the requested columns,
        // and every value decodes. This is the no-SORT case (an arbitrary page of `limit` matching rows); the
        // globally-sorted top-N is covered separately by testSortPushdownMatchesDirectTopN.
        String tail = " | WHERE message IS NOT NULL | KEEP @timestamp, message | LIMIT 50";
        List<List<Object>> rows = runExternal(externalSource() + tail);
        assertThat(rows.size(), equalTo(50));
        for (List<Object> row : rows) {
            assertThat(row.size(), equalTo(2));
            assertThat("message projected and non-null", row.get(1), notNullValue());
        }
    }

    public void testFilterPushdownOnlyReturnsMatchingRows() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // Pick a dataset value that exists, push an equality filter on it, and verify every returned row
        // matches. This validates filter pushdown correctness without depending on exact remote totals
        // (which the v1 implicit row cap would otherwise distort for an aggregate).
        List<List<Object>> top = directValues(
            "FROM "
                + TARGET
                + " | WHERE data_stream.dataset IS NOT NULL | STATS c = COUNT(*) BY data_stream.dataset"
                + " | SORT c DESC | LIMIT 1"
        );
        assumeTrue("remote has a non-null dataset to filter on", top.isEmpty() == false);
        String dataset = String.valueOf(top.get(0).get(1));

        String tail = " | WHERE data_stream.dataset == \"" + dataset + "\" | KEEP data_stream.dataset | LIMIT 100";
        List<List<Object>> rows = runExternal(externalSource() + tail);
        assertThat("filter matched at least one row", rows.isEmpty(), equalTo(false));
        for (List<Object> row : rows) {
            assertThat(String.valueOf(row.get(0)), equalTo(dataset));
        }
    }

    public void testSortPushdownMatchesDirectTopN() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // With SORT pushdown, a SORT ... | LIMIT n over the connector renders into the remote _query, so the
        // remote returns the correct global top-N rather than an arbitrary capped page. The connector result
        // must therefore match the same top-N run directly against the cluster, in the same order.
        int limit = 25;
        String topN = " | SORT @timestamp DESC NULLS LAST | KEEP @timestamp | LIMIT " + limit;

        List<List<Object>> viaConnector = runExternal(externalSource() + topN);
        List<List<Object>> direct = directValues("FROM " + TARGET + topN);

        assertThat(viaConnector.size(), equalTo(direct.size()));
        // Both paths render the datetime @timestamp as an ISO-8601 string in the response; parse to an instant
        // (epoch millis) so the comparison is independent of the exact string form.
        for (int i = 0; i < viaConnector.size(); i++) {
            long connectorMillis = epochMillis(viaConnector.get(i).get(0));
            long directMillis = epochMillis(direct.get(i).get(0));
            assertThat("row " + i + " timestamp matches direct top-N order", connectorMillis, equalTo(directMillis));
        }
        // Sanity: the sequence is actually descending (the remote SORT was applied, not arbitrary order).
        for (int i = 1; i < viaConnector.size(); i++) {
            assertThat(
                "descending sort order",
                epochMillis(viaConnector.get(i - 1).get(0)),
                greaterThanOrEqualTo(epochMillis(viaConnector.get(i).get(0)))
            );
        }
    }

    /**
     * STATS COUNT(*) is pushed to the remote, so the connector computes the count server-side over the full
     * data set and matches a direct aggregate exactly — no longer capped by the implicit FROM page size.
     */
    public void testUngroupedCountPushdownMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        long direct = ((Number) directValues("FROM " + TARGET + " | STATS c = COUNT(*)").get(0).get(0)).longValue();
        long viaConnector = ((Number) runExternal(externalSource() + " | STATS c = COUNT(*)").get(0).get(0)).longValue();
        assertThat("remote has a substantial number of rows", direct, greaterThan(10_000L));
        assertThat("connector count matches the direct full-dataset count (STATS pushed down)", viaConnector, equalTo(direct));
    }

    /**
     * Ungrouped {@code COUNT(field)}, {@code MIN(field)} and {@code MAX(field)} are pushed to the remote and
     * computed server-side over the full data set, so they match a direct aggregate exactly instead of being
     * capped by the implicit FROM page size. Field values are compared as-is (the field is a remote {@code long}).
     */
    public void testUngroupedCountMinMaxPushdownMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // A numeric field present in logs* with a large number of non-null values, so COUNT(field) would be
        // capped (and MIN/MAX computed over a truncated set) without STATS pushdown.
        String field = "`resource.attributes.host.cpu.cache.l2.size`";
        String stats = " | STATS c = COUNT(" + field + "), mn = MIN(" + field + "), mx = MAX(" + field + ")";

        List<Object> direct = directValues("FROM " + TARGET + stats).get(0);
        List<Object> viaConnector = runExternal(externalSource() + stats).get(0);

        long directCount = ((Number) direct.get(0)).longValue();
        assertThat("the field has many non-null values, so the count would be capped without pushdown", directCount, greaterThan(10_000L));
        assertThat(
            "connector COUNT(field) matches the direct full-dataset count",
            ((Number) viaConnector.get(0)).longValue(),
            equalTo(directCount)
        );
        assertThat("connector MIN(field) matches direct", asLongOrNull(viaConnector.get(1)), equalTo(asLongOrNull(direct.get(1))));
        assertThat("connector MAX(field) matches direct", asLongOrNull(viaConnector.get(2)), equalTo(asLongOrNull(direct.get(2))));
    }

    private static Long asLongOrNull(Object value) {
        return value == null ? null : ((Number) value).longValue();
    }

    /**
     * Grouped {@code STATS c = COUNT(*) BY <key>} is pushed to the remote and computed server-side, so the
     * connector returns the same per-group counts as a direct aggregate over the full data set. The remote
     * {@code STATS ... BY} response lists the aggregate before the key; the connector reorders to the
     * key-first intermediate layout the local FINAL aggregate consumes, so this also exercises that reordering.
     */
    public void testGroupedCountByKeyPushdownMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // A keyword field with several distinct values, so the grouped result has multiple rows.
        String stats = " | STATS c = COUNT(*) BY `resource.attributes.host.name`";

        Map<String, Long> direct = countsByKey(directValues("FROM " + TARGET + stats));
        Map<String, Long> viaConnector = countsByKey(runExternal(externalSource() + stats));

        assertThat("the grouping key has several distinct values", direct.size(), greaterThan(1));
        assertThat("connector grouped counts match the direct full-dataset grouped counts", viaConnector, equalTo(direct));
    }

    /**
     * Grouped {@code STATS c = COUNT(*) BY <key1>, <key2>} with multiple keys is pushed to the remote, so the
     * connector returns the same per-group counts as a direct aggregate. Keys are compared by composite value so
     * the test is independent of remote column ordering.
     */
    public void testGroupedCountByMultipleKeysPushdownMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String stats = " | STATS c = COUNT(*) BY `resource.attributes.host.name`, `service.name`";

        Map<String, Long> direct = countsByCompositeKey(directValues("FROM " + TARGET + stats));
        Map<String, Long> viaConnector = countsByCompositeKey(runExternal(externalSource() + stats));

        assertThat("the composite key has several distinct values", direct.size(), greaterThan(1));
        assertThat("connector multi-key grouped counts match the direct full-dataset grouped counts", viaConnector, equalTo(direct));
    }

    /**
     * Folds rows of a single-count, two-key grouped result into a composite-key -> count map. The numeric count is
     * identified by type, the remaining cells (in their column order) form the key, so the mapping is independent
     * of whether the count comes before or after the keys.
     */
    private static Map<String, Long> countsByCompositeKey(List<List<Object>> rows) {
        Map<String, Long> counts = new HashMap<>();
        for (List<Object> row : rows) {
            long count = 0;
            List<String> keyParts = new ArrayList<>();
            for (Object cell : row) {
                if (cell instanceof Number n) {
                    count = n.longValue();
                } else {
                    keyParts.add(String.valueOf(cell));
                }
            }
            counts.put(String.join("\u0000", keyParts), count);
        }
        return counts;
    }

    /**
     * Grouped {@code STATS c = COUNT(*), n = COUNT(field) BY <key>} with multiple COUNT aggregates is pushed to the
     * remote. This exercises the multi-aggregate intermediate layout
     * {@code [key, c_value, c_seen, n_value, n_seen]} the connector builds for the local FINAL aggregate. Grouped
     * MIN/MAX/SUM are intentionally out of scope (their per-group value channel may be null, which the connector's
     * uniform {@code seen=true} expansion cannot represent), so only COUNT aggregates are validated here. The
     * connector result must equal a direct aggregate over the full data set.
     */
    public void testGroupedMultiAggregatePushdownMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String field = "`resource.attributes.host.cpu.cache.l2.size`";
        String stats = " | STATS c = COUNT(*), n = COUNT(" + field + ") BY `resource.attributes.host.name`";

        Map<String, List<Long>> direct = aggsByKey(directValues("FROM " + TARGET + stats));
        Map<String, List<Long>> viaConnector = aggsByKey(runExternal(externalSource() + stats));

        assertThat("the grouping key has several distinct values", direct.size(), greaterThan(1));
        assertThat("connector grouped multi-aggregate results match the direct full-dataset results", viaConnector, equalTo(direct));
    }

    /**
     * Folds rows of a {@code COUNT, COUNT BY key} grouped result into a key -> [count1, count2] map. The single
     * string cell is the key; the numeric cells are the two aggregate values, kept in their column order so
     * connector and direct results are comparable as long as each renders the aggregates in the same relative order
     * (both follow the query's STATS order).
     */
    private static Map<String, List<Long>> aggsByKey(List<List<Object>> rows) {
        Map<String, List<Long>> result = new HashMap<>();
        for (List<Object> row : rows) {
            String key = null;
            List<Long> aggs = new ArrayList<>();
            for (Object cell : row) {
                if (cell instanceof Number n) {
                    aggs.add(n.longValue());
                } else if (cell != null) {
                    key = String.valueOf(cell);
                }
            }
            result.put(key, aggs);
        }
        return result;
    }

    /** Folds {@code [count, key]} (direct) or {@code [key, count]} (connector) rows into a key -> count map. */
    private static Map<String, Long> countsByKey(List<List<Object>> rows) {
        Map<String, Long> counts = new HashMap<>();
        for (List<Object> row : rows) {
            // Identify the numeric count and the (possibly null) string key regardless of column order.
            Object a = row.get(0);
            Object b = row.get(1);
            Object countObj = a instanceof Number ? a : b;
            Object keyObj = a instanceof Number ? b : a;
            counts.put(String.valueOf(keyObj), ((Number) countObj).longValue());
        }
        return counts;
    }

    private List<List<Object>> runExternal(String query) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            return collectRows(response);
        }
    }

    /** Normalizes a datetime cell (ISO-8601 string or numeric epoch millis) to epoch millis for comparison. */
    private static long epochMillis(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return java.time.Instant.parse(String.valueOf(value)).toEpochMilli();
    }
}
