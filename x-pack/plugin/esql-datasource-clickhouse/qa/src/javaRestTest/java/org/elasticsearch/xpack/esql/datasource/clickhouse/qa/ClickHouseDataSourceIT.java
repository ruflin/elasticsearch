/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end integration test for the {@code clickhouse} ES|QL external data source.
 *
 * <p>Builds the same topology as the {@code elasticsearch}-connector demo, but the remote system is
 * ClickHouse: an Elasticsearch cluster (built from this branch, with the {@code esql-datasource-clickhouse}
 * plugin) connects to a ClickHouse server that holds sample log data, and the test validates that the
 * data is queryable through ES|QL via a registered data source + dataset:
 *
 * <ol>
 *   <li>seeds a {@code logs.app_logs} table of sample application logs in ClickHouse (over HTTP);</li>
 *   <li>{@code PUT _query/data_source} with {@code type: clickhouse} (storing the connection user +
 *       encrypted password);</li>
 *   <li>{@code PUT _query/dataset} binding that data source to {@code clickhouse://host/logs/app_logs};</li>
 *   <li>runs {@code FROM <dataset> | STATS ...} and asserts the rows that physically live in ClickHouse
 *       come back through Elasticsearch.</li>
 * </ol>
 *
 * <p>The test requires a reachable ClickHouse server, supplied via the {@code tests.clickhouse.url}
 * system property (set from the {@code CLICKHOUSE_URL} environment variable in {@code build.gradle}).
 * When unset it self-skips, so it is a no-op in CI. Start a local ClickHouse with:
 * <pre>
 *   scripts/dev/clickhouse/seed.sh
 *   CLICKHOUSE_URL='clickhouse://localhost:8123/logs/app_logs' \
 *     ./gradlew :x-pack:plugin:esql-datasource-clickhouse:qa:javaRestTest
 * </pre>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ClickHouseDataSourceIT extends ESRestTestCase {

    private static final String DATASOURCE_NAME = "clickhouse_demo";
    private static final String DATASET_NAME = "clickhouse_logs";

    // The test owns a dedicated database + table so it can seed and drop freely without disturbing any
    // sample data created by scripts/dev/clickhouse/seed.sh (which seeds logs.app_logs). CLICKHOUSE_URL
    // is used only for the connection (host/port/tls/credentials); its path is ignored in favour of these.
    private static final String IT_DATABASE = "esql_clickhouse_it";
    private static final String IT_TABLE = "app_logs";

    private static final String CLICKHOUSE_URL = System.getProperty("tests.clickhouse.url", "");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    private static ParsedUrl clickhouse;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void setUpClickHouse() throws Exception {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
        assumeTrue(
            "Set CLICKHOUSE_URL (e.g. clickhouse://localhost:8123/logs/app_logs) to run this test",
            CLICKHOUSE_URL.isBlank() == false
        );
        clickhouse = ParsedUrl.parse(CLICKHOUSE_URL);
        seedLogs(clickhouse);
    }

    @AfterClass
    public static void tearDownClickHouse() throws Exception {
        if (clickhouse != null) {
            execute(clickhouse, "DROP TABLE IF EXISTS " + IT_DATABASE + "." + IT_TABLE);
        }
    }

    @Before
    public void cleanupBefore() throws IOException {
        deleteDatasetIfExists(DATASET_NAME);
        deleteDataSourceIfExists(DATASOURCE_NAME);
    }

    /**
     * Core end-to-end guard: register a clickhouse data source + dataset, run a {@code STATS COUNT(*)}
     * through the dataset, and assert the count matches the seeded rows. A correct, non-empty result
     * proves the whole chain: validator accepted {@code type: clickhouse}, the connector resolved the
     * ClickHouse schema, built and ran the {@code SELECT}, and parsed the columnar response into ES|QL
     * blocks.
     */
    public void testCountThroughDataset() throws IOException {
        putDataSource(DATASOURCE_NAME, clickhouse);
        putDataset(DATASET_NAME, DATASOURCE_NAME, clickhouse.datasetResource());

        Map<String, Object> result = runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat("STATS COUNT(*) must return one row", values, hasSize(1));
        Number count = (Number) values.get(0).get(0);
        assertThat("count must match the seeded ClickHouse rows", count.intValue(), equalTo(SEEDED_ROWS));
    }

    /**
     * Validates a grouped aggregation and column projection over the ClickHouse-backed dataset, i.e.
     * a realistic "count logs by level" query plus a projected sample. Confirms typed columns
     * (keyword, long) round-trip correctly through the connector.
     */
    public void testGroupedStatsAndProjection() throws IOException {
        putDataSource(DATASOURCE_NAME, clickhouse);
        putDataset(DATASET_NAME, DATASOURCE_NAME, clickhouse.datasetResource());

        Map<String, Object> byLevel = runEsql(
            "FROM " + DATASET_NAME + " | STATS c = COUNT(*) BY log_level | SORT c DESC, log_level ASC | LIMIT 10"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> levelValues = (List<List<Object>>) byLevel.get("values");
        assertThat("expected at least the INFO/WARN/ERROR/DEBUG groups", levelValues, hasSize(greaterThanOrEqualTo(1)));
        int summed = levelValues.stream().mapToInt(row -> ((Number) row.get(0)).intValue()).sum();
        assertThat("group counts must sum to the seeded rows", summed, equalTo(SEEDED_ROWS));

        Map<String, Object> sample = runEsql("FROM " + DATASET_NAME + " | KEEP log_level, service, message | LIMIT 5");
        @SuppressWarnings("unchecked")
        List<List<Object>> sampleValues = (List<List<Object>>) sample.get("values");
        assertThat("projection must return rows", sampleValues, hasSize(greaterThanOrEqualTo(1)));
        assertThat("projection must keep exactly the three requested columns", sampleValues.get(0), hasSize(3));
    }

    // -----------------------------------------------------------------------------------------
    // ClickHouse seeding (over HTTP, independent of the docker init script)
    // -----------------------------------------------------------------------------------------

    private static final int SEEDED_ROWS = 8;

    private static void seedLogs(ParsedUrl ch) throws Exception {
        String db = IT_DATABASE;
        String table = IT_DATABASE + "." + IT_TABLE;
        execute(ch, "CREATE DATABASE IF NOT EXISTS " + db);
        execute(ch, "DROP TABLE IF EXISTS " + table);
        execute(
            ch,
            "CREATE TABLE "
                + table
                + " (timestamp DateTime, log_level String, service String, host String, message String, "
                + "status_code Int32, duration_ms Float64) ENGINE = MergeTree() ORDER BY (timestamp, service)"
        );
        execute(
            ch,
            "INSERT INTO "
                + table
                + " (timestamp, log_level, service, host, message, status_code, duration_ms) VALUES"
                + " ('2026-06-17 09:00:01','INFO','checkout','host-a','order placed',200,42.5),"
                + " ('2026-06-17 09:00:09','WARN','checkout','host-a','slow payment call',200,812.7),"
                + " ('2026-06-17 09:00:12','ERROR','checkout','host-c','payment gateway timeout',504,3001.2),"
                + " ('2026-06-17 09:00:15','INFO','catalog','host-a','product fetched',200,12.3),"
                + " ('2026-06-17 09:00:21','DEBUG','catalog','host-b','cache hit',200,1.1),"
                + " ('2026-06-17 09:00:24','ERROR','catalog','host-c','product not found',404,7.4),"
                + " ('2026-06-17 09:00:27','INFO','auth','host-a','user login',200,33.0),"
                + " ('2026-06-17 09:00:30','WARN','auth','host-a','repeated failed login',401,18.6)"
        );
    }

    /** Executes a write statement against ClickHouse over its HTTP API. */
    private static void execute(ParsedUrl ch, String sql) throws Exception {
        URI uri = URI.create((ch.tls ? "https" : "http") + "://" + ch.host + ":" + ch.port + "/");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(uri)
            .header("X-ClickHouse-User", ch.user)
            .header("X-ClickHouse-Key", ch.password)
            .header("Content-Type", "text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
            .timeout(Duration.ofSeconds(30))
            .build();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("ClickHouse HTTP " + response.statusCode() + ": " + response.body() + " for SQL: " + sql);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // ES|QL data source / dataset / query REST helpers
    // -----------------------------------------------------------------------------------------

    private static void putDataSource(String name, ParsedUrl ch) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "clickhouse")
                .field("description", "ClickHouse demo")
                .startObject("settings")
                .field("user", ch.user)
                .field("password", ch.password)
                .endObject()
                .endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataset(String name, String dataSource, String resource) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static Map<String, Object> runEsql(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", query).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return entityAsMap(r);
    }

    private static void deleteDataSourceIfExists(String name) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/_query/data_source/" + name));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    private static void deleteDatasetIfExists(String name) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/_query/dataset/" + name));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    /**
     * Connection details parsed from {@code clickhouse://host:port[/...]} (or {@code clickhouse+https://...}),
     * with optional {@code user}:{@code password} userinfo. Only host/port/tls/credentials are used; the
     * URL path (if any) is ignored because the test seeds and queries its own {@link #IT_DATABASE}.{@link #IT_TABLE}.
     * Defaults: user {@code default}, empty password, port 8123 (8443 for TLS).
     */
    private record ParsedUrl(String host, int port, boolean tls, String user, String password) {

        static ParsedUrl parse(String url) {
            URI uri = URI.create(url);
            boolean tls = "clickhouse+https".equals(uri.getScheme());
            String host = uri.getHost();
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("CLICKHOUSE_URL missing host: " + url);
            }
            int port = uri.getPort() > 0 ? uri.getPort() : (tls ? 8443 : 8123);
            String user = "default";
            String password = "";
            String userInfo = uri.getUserInfo();
            if (userInfo != null && userInfo.isBlank() == false) {
                int colon = userInfo.indexOf(':');
                if (colon >= 0) {
                    user = userInfo.substring(0, colon);
                    password = userInfo.substring(colon + 1);
                } else {
                    user = userInfo;
                }
            }
            return new ParsedUrl(host, port, tls, user, password);
        }

        /** The {@code clickhouse://host:port/database/table} resource string for the dataset. */
        String datasetResource() {
            return (tls ? "clickhouse+https" : "clickhouse") + "://" + host + ":" + port + "/" + IT_DATABASE + "/" + IT_TABLE;
        }
    }
}
