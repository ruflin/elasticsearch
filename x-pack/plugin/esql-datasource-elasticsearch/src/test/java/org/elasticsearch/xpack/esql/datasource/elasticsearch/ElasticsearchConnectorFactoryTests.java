/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteSort;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;

import java.util.List;
import java.util.Map;

public class ElasticsearchConnectorFactoryTests extends ESTestCase {

    private final ElasticsearchConnectorFactory factory = new ElasticsearchConnectorFactory();

    public void testType() {
        assertEquals("elasticsearch", factory.type());
    }

    public void testCanHandle() {
        assertTrue(factory.canHandle("es://localhost:9200/logs"));
        assertTrue(factory.canHandle("elasticsearch://remote:9200/metrics"));
        assertFalse(factory.canHandle("flight://localhost:47470/employees"));
        assertFalse(factory.canHandle("s3://bucket/key"));
    }

    public void testParseLocationWithExplicitPort() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation(
            "es://remote.example.com:9201/logs-*"
        );
        assertEquals("http://remote.example.com:9201", endpoint.baseUrl());
        assertEquals("logs-*", endpoint.target());
    }

    public void testParseLocationDefaultsPort() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation("es://remote/logs");
        assertEquals("http://remote:9200", endpoint.baseUrl());
        assertEquals("logs", endpoint.target());
    }

    public void testParseLocationSecureDefaultsToNoExplicitPort() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation(
            "es+https://abc.es.europe-west1.gcp.elastic.cloud/logs*"
        );
        assertEquals("https://abc.es.europe-west1.gcp.elastic.cloud", endpoint.baseUrl());
        assertEquals("logs*", endpoint.target());
    }

    public void testParseLocationSecureWithExplicitPort() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation(
            "elasticsearch+https://remote.example.com:9243/metrics"
        );
        assertEquals("https://remote.example.com:9243", endpoint.baseUrl());
        assertEquals("metrics", endpoint.target());
    }

    public void testCanHandleSecureSchemes() {
        assertTrue(factory.canHandle("es+https://host/logs"));
        assertTrue(factory.canHandle("elasticsearch+https://host/logs"));
    }

    public void testParseLocationMissingIndexThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticsearchConnectorFactory.parseLocation("es://remote:9200/")
        );
        assertTrue(e.getMessage().contains("missing index"));
    }

    public void testParseLocationRejectsUserInfo() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticsearchConnectorFactory.parseLocation("es://user:pass@remote:9200/logs")
        );
        assertTrue(e.getMessage().contains("user info is not supported"));
    }

    public void testValidateConfigAcceptsApiKey() {
        factory.validateConfig("es://remote:9200/logs", Map.of("api_key", "secret"));
    }

    public void testValidateConfigAcceptsTimeoutOverrides() {
        factory.validateConfig(
            "es://remote:9200/logs",
            Map.of("api_key", "secret", "connect_timeout_millis", 5000, "socket_timeout_millis", 30000)
        );
    }

    public void testValidateConfigRejectsUnknownKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.validateConfig("es://remote:9200/logs", Map.of("bogus", "x"))
        );
        assertTrue(e.getMessage().contains("unknown option"));
    }

    public void testParseLocationRejectsLinkLocalIpv4Literal() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticsearchConnectorFactory.parseLocation("es://169.254.169.254:9200/logs")
        );
        assertTrue(e.getMessage().contains("link-local addresses are not allowed"));
    }

    public void testParseLocationRejectsLinkLocalIpv6Literal() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticsearchConnectorFactory.parseLocation("es://[fe80::1]:9200/logs")
        );
        assertTrue(e.getMessage().contains("link-local addresses are not allowed"));
    }

    public void testParseLocationAllowsPrivateIpv4Literal() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation("es://10.0.0.5:9200/logs");
        assertEquals("http://10.0.0.5:9200", endpoint.baseUrl());
        assertEquals("logs", endpoint.target());
    }

    public void testParseLocationAllowsLoopbackLiteral() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation("es://127.0.0.1:9200/logs");
        assertEquals("http://127.0.0.1:9200", endpoint.baseUrl());
        assertEquals("logs", endpoint.target());
    }

    public void testOpenRequiresEndpoint() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.open(Map.of()));
        assertTrue(e.getMessage().contains("endpoint"));
    }

    public void testBuildRemoteQueryProjectsColumns() {
        var request = new org.elasticsearch.xpack.esql.datasources.spi.QueryRequest(
            "logs",
            List.of("@timestamp", "message"),
            List.of(),
            Map.of(),
            1000,
            null
        );
        assertEquals("FROM logs | KEEP `@timestamp`, `message`", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryNoProjection() {
        var request = new QueryRequest("logs", List.of(), List.of(), Map.of(), 1000, null);
        assertEquals("FROM logs", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryPushesLimit() {
        var request = new QueryRequest("logs", List.of("message"), List.of(), Map.of(), 1000, 5, null);
        assertEquals("FROM logs | KEEP `message` | LIMIT 5", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryPushesFilter() {
        Expression filter = new GreaterThan(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "count",
                new EsField("count", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );
        var request = new QueryRequest(
            "logs",
            List.of("count"),
            List.of(),
            Map.of(),
            1000,
            FormatReader.NO_LIMIT,
            List.of(filter),
            List.of(),
            List.of(),
            List.of(),
            false,
            null
        );
        assertEquals("FROM logs | WHERE `count` > 10 | KEEP `count`", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryPushesFilterAndLimit() {
        Expression filter = new GreaterThan(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "count",
                new EsField("count", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );
        var request = new QueryRequest(
            "logs",
            List.of(),
            List.of(),
            Map.of(),
            1000,
            3,
            List.of(filter),
            List.of(),
            List.of(),
            List.of(),
            false,
            null
        );
        assertEquals("FROM logs | WHERE `count` > 10 | LIMIT 3", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryPushesSortBeforeKeepAndLimit() {
        var request = new QueryRequest(
            "logs",
            List.of("message"),
            List.of(),
            Map.of(),
            1000,
            5,
            List.of(),
            List.of(new RemoteSort("@timestamp", false, false), new RemoteSort("message", true, true)),
            List.of(),
            List.of(),
            false,
            null
        );
        assertEquals(
            "FROM logs | SORT `@timestamp` DESC NULLS LAST, `message` ASC NULLS FIRST | KEEP `message` | LIMIT 5",
            ElasticsearchConnector.buildRemoteQuery(request)
        );
    }

    public void testBuildRemoteQueryPushesUngroupedStats() {
        var request = new QueryRequest(
            "logs",
            List.of(),
            List.of(),
            Map.of(),
            1000,
            5,
            List.of(),
            List.of(),
            List.of(new RemoteAggregate("c", "COUNT", null)),
            List.of(),
            false,
            null
        );
        assertEquals("FROM logs | STATS `c` = COUNT(*) | LIMIT 5", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryPushesFilterBeforeStats() {
        Expression filter = new GreaterThan(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "count",
                new EsField("count", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new Literal(Source.EMPTY, 10, DataType.INTEGER),
            null
        );
        var request = new QueryRequest(
            "logs",
            List.of(),
            List.of(),
            Map.of(),
            1000,
            FormatReader.NO_LIMIT,
            List.of(filter),
            List.of(),
            List.of(new RemoteAggregate("c", "COUNT", null)),
            List.of(),
            false,
            null
        );
        assertEquals("FROM logs | WHERE `count` > 10 | STATS `c` = COUNT(*)", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryPushesStatsWithFieldAndGrouping() {
        var request = new QueryRequest(
            "logs",
            List.of(),
            List.of(),
            Map.of(),
            1000,
            FormatReader.NO_LIMIT,
            List.of(),
            List.of(),
            List.of(new RemoteAggregate("c", "COUNT", null), new RemoteAggregate("mx", "MAX", "bytes")),
            List.of("data_stream.type"),
            false,
            null
        );
        assertEquals(
            "FROM logs | STATS `c` = COUNT(*), `mx` = MAX(`bytes`) BY `data_stream.type`",
            ElasticsearchConnector.buildRemoteQuery(request)
        );
    }

    public void testBuildRemoteQueryPushesSortAndLimitAfterStats() {
        var request = new QueryRequest(
            "logs",
            List.of(),
            List.of(),
            Map.of(),
            1000,
            5,
            List.of(),
            List.of(new RemoteSort("c", false, false)),
            List.of(new RemoteAggregate("c", "COUNT", null)),
            List.of("service.name"),
            false,
            null
        );
        assertEquals(
            "FROM logs | STATS `c` = COUNT(*) BY `service.name` | SORT `c` DESC NULLS LAST | LIMIT 5",
            ElasticsearchConnector.buildRemoteQuery(request)
        );
    }

    public void testBuildRemoteQueryRejectsInjectableTarget() {
        var request = new QueryRequest("logs | DROP age", List.of(), List.of(), Map.of(), 1000, null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ElasticsearchConnector.buildRemoteQuery(request));
        assertTrue(e.getMessage().contains("illegal character"));
    }

}
