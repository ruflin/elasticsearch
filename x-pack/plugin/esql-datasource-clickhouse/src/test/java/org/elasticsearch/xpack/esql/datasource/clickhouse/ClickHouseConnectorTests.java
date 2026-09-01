/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for the ClickHouse connector. These exercise schema discovery and querying
 * against a live ClickHouse instance and are skipped (via {@code assumeTrue}) when the
 * {@code CLICKHOUSE_URL} environment variable is not set, so the suite is a no-op in normal CI.
 *
 * <p>Set the environment variable to run them:
 * <pre>
 *   CLICKHOUSE_URL=clickhouse://localhost:8123/test/employees \
 *     ./gradlew :x-pack:plugin:esql-datasource-clickhouse:test \
 *     --tests "*ClickHouseConnectorTests*"
 * </pre>
 *
 * <p>A Docker Compose file is provided in {@code src/test/resources/docker/} for convenience.
 * Start it with: {@code docker compose -f src/test/resources/docker/docker-compose.yml up -d}
 */
public class ClickHouseConnectorTests extends ESTestCase {

    private static final String ENV_CLICKHOUSE_URL = "CLICKHOUSE_URL";
    private static final String DEFAULT_USER = "default";
    private static final String DEFAULT_PASSWORD = "";

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * Tests end-to-end schema discovery and data query against a real ClickHouse instance.
     * Seeds a temporary table with employee data, queries it, and verifies results.
     */
    public void testEndToEndWithEmployeeData() throws Exception {
        String clickhouseUrl = System.getenv(ENV_CLICKHOUSE_URL);
        assumeTrue("Set " + ENV_CLICKHOUSE_URL + " to run integration tests", clickhouseUrl != null && clickhouseUrl.isBlank() == false);

        ClickHouseConnectorFactory.ParsedUri parsed = ClickHouseConnectorFactory.parseUri(clickhouseUrl);
        String database = parsed.database();
        String table = parsed.table();

        // Create test table and seed data
        seedTestData(parsed, database, table);
        try {
            // Verify schema discovery
            ClickHouseConnectorFactory factory = new ClickHouseConnectorFactory();
            Map<String, Object> userConfig = Map.of("user", DEFAULT_USER, "password", DEFAULT_PASSWORD);
            SourceMetadata metadata = factory.resolveMetadata(clickhouseUrl, userConfig);

            assertNotNull(metadata);
            assertEquals("clickhouse", metadata.sourceType());
            assertTrue("Expected at least 3 columns", metadata.schema().size() >= 3);

            List<Attribute> attrs = metadata.schema();
            assertNotNull(findAttr(attrs, "emp_no"));
            assertNotNull(findAttr(attrs, "first_name"));
            assertNotNull(findAttr(attrs, "salary"));

            // Verify data query
            Map<String, Object> resolvedConfig = metadata.config();
            try (Connector connector = factory.open(resolvedConfig)) {
                List<String> projected = List.of("emp_no", "first_name", "salary");
                List<Attribute> projectedAttrs = List.of(
                    findAttr(attrs, "emp_no"),
                    findAttr(attrs, "first_name"),
                    findAttr(attrs, "salary")
                );

                QueryRequest request = new QueryRequest(table, projected, projectedAttrs, resolvedConfig, 100, 10, blockFactory);
                try (ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
                    assertTrue("Expected at least one page of results", cursor.hasNext());
                    Page page = cursor.next();
                    assertTrue("Expected at least 1 row", page.getPositionCount() >= 1);
                    assertTrue("Expected at most 10 rows (LIMIT applied)", page.getPositionCount() <= 10);

                    try {
                        IntBlock empNos = (IntBlock) page.getBlock(0);
                        BytesRefBlock names = (BytesRefBlock) page.getBlock(1);
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            assertTrue("emp_no should be positive", empNos.getInt(i) > 0);
                            BytesRef name = names.getBytesRef(i, new BytesRef());
                            assertNotNull("first_name should not be null", name);
                        }
                    } finally {
                        page.releaseBlocks();
                    }
                }
            }
        } finally {
            dropTestData(parsed, database, table);
        }
    }

    private static void seedTestData(ClickHouseConnectorFactory.ParsedUri parsed, String database, String table) throws Exception {
        String createDb = "CREATE DATABASE IF NOT EXISTS " + ClickHouseConnector.quoteIdentifier(database);
        String createTable = "CREATE TABLE IF NOT EXISTS "
            + ClickHouseConnector.quoteIdentifier(database)
            + "."
            + ClickHouseConnector.quoteIdentifier(table)
            + " (emp_no Int32, first_name String, last_name String, salary Int32, still_hired Bool)"
            + " ENGINE = MergeTree() ORDER BY emp_no";
        String insert = "INSERT INTO "
            + ClickHouseConnector.quoteIdentifier(database)
            + "."
            + ClickHouseConnector.quoteIdentifier(table)
            + " (emp_no, first_name, last_name, salary, still_hired) VALUES"
            + " (1, 'Alice', 'Smith', 75000, true),"
            + " (2, 'Bob', 'Jones', 82000, true),"
            + " (3, 'Carol', 'Williams', 91000, false),"
            + " (4, 'Dave', 'Brown', 67000, true),"
            + " (5, 'Eve', 'Davis', 55000, false)";

        executeHttp(parsed, createDb);
        executeHttp(parsed, createTable);
        executeHttp(parsed, insert);
    }

    private static void dropTestData(ClickHouseConnectorFactory.ParsedUri parsed, String database, String table) throws Exception {
        String drop = "DROP TABLE IF EXISTS "
            + ClickHouseConnector.quoteIdentifier(database)
            + "."
            + ClickHouseConnector.quoteIdentifier(table);
        executeHttp(parsed, drop);
    }

    private static void executeHttp(ClickHouseConnectorFactory.ParsedUri parsed, String sql) throws Exception {
        String scheme = parsed.tls() ? "https" : "http";
        URI uri = URI.create(scheme + "://" + parsed.host() + ":" + parsed.port() + "/");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(uri)
            .header("X-ClickHouse-User", DEFAULT_USER)
            .header("X-ClickHouse-Key", DEFAULT_PASSWORD)
            .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
            .timeout(Duration.ofSeconds(30))
            .build();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IOException("ClickHouse returned HTTP " + response.statusCode() + ": " + response.body() + " for SQL: " + sql);
            }
        }
    }

    private static Attribute findAttr(List<Attribute> attrs, String name) {
        return attrs.stream().filter(a -> name.equals(a.name())).findFirst().orElse(null);
    }
}
