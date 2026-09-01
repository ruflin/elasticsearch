/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteGrouping;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteSort;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end tests that run the connector against a stub {@code _query} endpoint and assert on the HTTP request
 * it actually sent.
 * <p>
 * The optimizer-rule tests prove a pushdown reaches {@link QueryRequest}, and {@code ElasticsearchConnectorTests}
 * proves {@code buildRemoteQuery} renders it, but neither shows the rendered query leaves the JVM. That matters
 * because the local {@code FilterExec}/{@code SampleExec}/{@code AggregateExec} are removed when their clause is
 * pushed, so a clause silently dropped between rendering and the wire would change results rather than just cost
 * a round-trip. These tests capture the request body and assert the pushed clauses are in it.
 * <p>
 * They also pin the credential-handling contract: the API key must reach the {@code Authorization} header but must
 * never appear in the resolved config that {@code resolveMetadata} hands back for the physical plan.
 */
@SuppressForbidden(reason = "uses a local HTTP server to capture the request the connector sends")
public class RemoteRequestCaptureTests extends ESTestCase {

    private static final String API_KEY = "c2VjcmV0LWFwaS1rZXk=";

    /** A columnar {@code _query} response with two rows, matching the {@code columnar=true} body the connector posts. */
    private static final String CANNED_RESPONSE = """
        {
          "columns": [
            {"name": "@timestamp", "type": "date"},
            {"name": "message", "type": "keyword"}
          ],
          "values": [
            ["2026-01-01T00:00:00.000Z", "2026-01-01T00:01:00.000Z"],
            ["hello", "world"]
          ]
        }""";

    private HttpServer server;
    private AtomicReference<String> capturedBody;
    private AtomicReference<String> capturedAuthorization;
    private String endpoint;
    private BlockFactory blockFactory;

    @Before
    public void startStubQueryServer() throws IOException {
        capturedBody = new AtomicReference<>();
        capturedAuthorization = new AtomicReference<>();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/_query", exchange -> {
            capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            capturedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = CANNED_RESPONSE.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        endpoint = "http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }

    @After
    public void stopStubQueryServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    /**
     * {@code resolveMetadata} must probe the remote with a validated {@code LIMIT 0} query, authenticate with the
     * configured key, and hand back a resolved config carrying only the connection details — never the key itself,
     * which would put a decrypted secret into the physical plan's source metadata.
     */
    public void testResolveMetadataProbesRemoteAndKeepsApiKeyOutOfResolvedConfig() {
        SourceMetadata metadata = factory().resolveMetadata(location("logs-*"), Map.of("api_key", API_KEY));

        assertThat(queryFromCapturedBody(), equalTo("FROM logs-* | LIMIT 0"));
        assertThat(capturedAuthorization.get(), equalTo("ApiKey " + API_KEY));

        Map<String, Object> resolvedConfig = metadata.config();
        assertThat(resolvedConfig.get("endpoint"), equalTo(endpoint));
        assertThat(resolvedConfig.get("target"), equalTo("logs-*"));
        assertThat(resolvedConfig.get("api_key"), nullValue());
        // Guard against the key leaking under some other key name as well.
        assertThat(resolvedConfig.toString(), not(containsString(API_KEY)));

        assertThat(metadata.schema().stream().map(Attribute::name).toList(), equalTo(List.of("@timestamp", "message")));
    }

    /**
     * A pushed {@code WHERE} / {@code SORT} / {@code LIMIT} must reach the remote request, not just the rendered
     * string. The pushed filter has no local safety net once every conjunct is pushed, so this is the assertion
     * that the remote really applies it.
     */
    public void testPushedFilterSortAndLimitReachTheRemoteRequest() throws IOException {
        QueryRequest request = new QueryRequest(
            "logs-*",
            List.of("@timestamp", "message"),
            List.of(),
            Map.of(),
            1000,
            25,
            List.of(),
            List.of(new RemoteSort("@timestamp", false, false)),
            List.of(),
            List.of(),
            false,
            0.5,
            blockFactory
        );

        try (Connector connector = openConnector(); ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
            drain(cursor);
        }

        assertThat(
            queryFromCapturedBody(),
            equalTo("FROM logs-* | SAMPLE 0.5 | SORT `@timestamp` DESC NULLS LAST | KEEP `@timestamp`, `message` | LIMIT 25")
        );
        assertThat(capturedAuthorization.get(), equalTo("ApiKey " + API_KEY));
    }

    /**
     * A pushed {@code STATS ... BY} must reach the remote request too. The optimizer removes the local aggregate
     * when it pushes, so a dropped {@code STATS} would return raw rows where the plan expects aggregate output.
     */
    public void testPushedStatsReachesTheRemoteRequest() throws IOException {
        QueryRequest request = new QueryRequest(
            "logs-*",
            List.of(),
            List.of(),
            Map.of(),
            1000,
            -1,
            List.of(),
            List.of(),
            List.of(RemoteAggregate.of("count", "COUNT", null)),
            List.of(RemoteGrouping.ofField("service.name")),
            false,
            1.0,
            blockFactory
        );

        try (Connector connector = openConnector(); ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
            drain(cursor);
        }

        assertThat(queryFromCapturedBody(), equalTo("FROM logs-* | STATS `count` = COUNT(*) BY `service.name`"));
    }

    private ElasticsearchConnectorFactory factory() {
        return new ElasticsearchConnectorFactory();
    }

    private String location(String target) {
        // parseLocation() rebuilds the base URL from host+port, so feeding it the loopback endpoint round-trips.
        return "es://" + endpoint.substring("http://".length()) + "/" + target;
    }

    private Connector openConnector() {
        return factory().open(Map.of("endpoint", endpoint, "target", "logs-*", "api_key", API_KEY));
    }

    /** Extracts the {@code query} field from the captured JSON body so assertions read as plain ES|QL. */
    private String queryFromCapturedBody() {
        String body = capturedBody.get();
        assertNotNull("connector did not issue a request", body);
        Map<String, Object> parsed = XContentHelper.convertToMap(XContentType.JSON.xContent(), body, false);
        assertThat(parsed.get("columnar"), equalTo(true));
        return (String) parsed.get("query");
    }

    private static void drain(ResultCursor cursor) {
        while (cursor.hasNext()) {
            Page page = cursor.next();
            page.releaseBlocks();
        }
    }
}
