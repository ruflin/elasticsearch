/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.test.ESTestCase;

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
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation("es://remote.example.com:9201/logs-*");
        assertEquals("http://remote.example.com:9201", endpoint.baseUrl());
        assertEquals("logs-*", endpoint.target());
    }

    public void testParseLocationDefaultsPort() {
        ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation("es://remote/logs");
        assertEquals("http://remote:9200", endpoint.baseUrl());
        assertEquals("logs", endpoint.target());
    }

    public void testParseLocationMissingIndexThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticsearchConnectorFactory.parseLocation("es://remote:9200/")
        );
        assertTrue(e.getMessage().contains("missing index"));
    }

    public void testValidateConfigAcceptsApiKey() {
        factory.validateConfig("es://remote:9200/logs", Map.of("api_key", "secret"));
    }

    public void testValidateConfigRejectsUnknownKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.validateConfig("es://remote:9200/logs", Map.of("bogus", "x"))
        );
        assertTrue(e.getMessage().contains("unknown option"));
    }

    public void testOpenRequiresEndpoint() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.open(Map.of()));
        assertTrue(e.getMessage().contains("endpoint"));
    }

    public void testSplitProviderIsSingle() {
        assertSame(org.elasticsearch.xpack.esql.datasources.spi.SplitProvider.SINGLE, factory.splitProvider());
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
        assertEquals("FROM logs | KEEP @timestamp, message", ElasticsearchConnector.buildRemoteQuery(request));
    }

    public void testBuildRemoteQueryNoProjection() {
        var request = new org.elasticsearch.xpack.esql.datasources.spi.QueryRequest("logs", List.of(), List.of(), Map.of(), 1000, null);
        assertEquals("FROM logs", ElasticsearchConnector.buildRemoteQuery(request));
    }
}
