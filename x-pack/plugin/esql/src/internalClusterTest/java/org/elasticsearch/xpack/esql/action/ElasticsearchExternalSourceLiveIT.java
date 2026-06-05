/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.elasticsearch.ElasticsearchDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
public class ElasticsearchExternalSourceLiveIT extends AbstractEsqlIntegTestCase {

    private static final String URL = System.getProperty("tests.esql.remote.url");
    private static final String API_KEY = System.getProperty("tests.esql.remote.apikey");
    private static final String TARGET = System.getProperty("tests.esql.remote.target", "logs*");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ElasticsearchDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

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
            "FROM " + TARGET + " | WHERE data_stream.dataset IS NOT NULL | STATS c = COUNT(*) BY data_stream.dataset"
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
        assertThat("connector COUNT(field) matches the direct full-dataset count", ((Number) viaConnector.get(0)).longValue(), equalTo(directCount));
        assertThat("connector MIN(field) matches direct", asLongOrNull(viaConnector.get(1)), equalTo(asLongOrNull(direct.get(1))));
        assertThat("connector MAX(field) matches direct", asLongOrNull(viaConnector.get(2)), equalTo(asLongOrNull(direct.get(2))));
    }

    private static Long asLongOrNull(Object value) {
        return value == null ? null : ((Number) value).longValue();
    }

    private List<List<Object>> runExternal(String query) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = new ArrayList<>();
            for (Iterator<Iterator<Object>> it = response.values(); it.hasNext();) {
                List<Object> row = new ArrayList<>();
                it.next().forEachRemaining(row::add);
                rows.add(row);
            }
            return rows;
        }
    }

    /** Runs the query straight against the remote {@code _query} API with the configured API key. */
    @SuppressWarnings("unchecked")
    private List<List<Object>> directValues(String esql) throws IOException {
        HttpHost host = HttpHost.create(URL);
        var builder = RestClient.builder(host);
        if (API_KEY != null) {
            builder.setDefaultHeaders(new org.apache.http.Header[] { new BasicHeader("Authorization", "ApiKey " + API_KEY) });
        }
        try (RestClient client = builder.build()) {
            Request request = new Request("POST", "/_query");
            request.addParameter("format", "json");
            request.setJsonEntity(Strings.format("{\"query\":%s}", quote(esql)));
            Response response = client.performRequest(request);
            try (
                InputStream content = response.getEntity().getContent();
                XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
            ) {
                Map<String, Object> body = parser.map();
                List<List<Object>> rows = new ArrayList<>();
                for (Object rowObj : (List<Object>) body.getOrDefault("values", List.of())) {
                    rows.add((List<Object>) rowObj);
                }
                return rows;
            }
        }
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /** Normalizes a datetime cell (ISO-8601 string or numeric epoch millis) to epoch millis for comparison. */
    private static long epochMillis(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return java.time.Instant.parse(String.valueOf(value)).toEpochMilli();
    }
}
