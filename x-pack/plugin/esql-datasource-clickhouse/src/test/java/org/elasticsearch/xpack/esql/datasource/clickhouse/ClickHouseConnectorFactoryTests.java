/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class ClickHouseConnectorFactoryTests extends ESTestCase {

    private final ClickHouseConnectorFactory factory = new ClickHouseConnectorFactory();

    // -------- type / canHandle ----------

    public void testType() {
        assertEquals("clickhouse", factory.type());
    }

    public void testCanHandleClickhouseUri() {
        assertTrue(factory.canHandle("clickhouse://localhost:8123/mydb/mytable"));
        assertTrue(factory.canHandle("clickhouse://clickhouse.example.com:8123/prod/events"));
    }

    public void testCanHandleClickhouseHttpsUri() {
        assertTrue(factory.canHandle("clickhouse+https://secure.example.com:8443/mydb/mytable"));
    }

    public void testCanHandleIsCaseInsensitive() {
        assertTrue(factory.canHandle("CLICKHOUSE://localhost:8123/mydb/mytable"));
        assertTrue(factory.canHandle("ClickHouse+HTTPS://secure.example.com/mydb/mytable"));
    }

    public void testCanHandleRejectsOtherSchemes() {
        assertFalse(factory.canHandle("http://example.com/data.parquet"));
        assertFalse(factory.canHandle("flight://server:9090/table"));
        assertFalse(factory.canHandle("s3://bucket/key.parquet"));
        assertFalse(factory.canHandle("jdbc:clickhouse://host:8123/db"));
    }

    // -------- validateConfig ----------

    public void testValidateConfigAcceptsEmptyConfig() {
        factory.validateConfig("clickhouse://host:8123/db/tbl", Map.of());
    }

    public void testValidateConfigAcceptsUserAndPassword() {
        factory.validateConfig("clickhouse://host:8123/db/tbl", Map.of("user", "alice", "password", "s3cr3t"));
    }

    public void testValidateConfigRejectsUnknownKey() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> factory.validateConfig("clickhouse://host:8123/db/tbl", Map.of("unknownOption", "value"))
        );
        assertTrue("Expected 'unknown option' in message: " + ex.getMessage(), ex.getMessage().contains("unknown option"));
    }

    // -------- parseUri ----------

    public void testParseUriDefaultPort() {
        ClickHouseConnectorFactory.ParsedUri p = ClickHouseConnectorFactory.parseUri("clickhouse://localhost/mydb/mytable");
        assertEquals("localhost", p.host());
        assertEquals(ClickHouseConnectorFactory.DEFAULT_HTTP_PORT, p.port());
        assertEquals("mydb", p.database());
        assertEquals("mytable", p.table());
        assertFalse(p.tls());
    }

    public void testParseUriExplicitPort() {
        ClickHouseConnectorFactory.ParsedUri p = ClickHouseConnectorFactory.parseUri("clickhouse://host:9999/db/tbl");
        assertEquals("host", p.host());
        assertEquals(9999, p.port());
        assertEquals("db", p.database());
        assertEquals("tbl", p.table());
        assertFalse(p.tls());
    }

    public void testParseUriTls() {
        ClickHouseConnectorFactory.ParsedUri p = ClickHouseConnectorFactory.parseUri("clickhouse+https://secure.host:8443/db/tbl");
        assertTrue(p.tls());
        assertEquals(8443, p.port());
    }

    public void testParseUriSchemeIsCaseInsensitive() {
        ClickHouseConnectorFactory.ParsedUri p = ClickHouseConnectorFactory.parseUri("CLICKHOUSE+HTTPS://secure.host/db/tbl");
        assertTrue(p.tls());
        assertEquals(ClickHouseConnectorFactory.DEFAULT_HTTPS_PORT, p.port());
    }

    public void testParseUriDefaultHttpsPort() {
        ClickHouseConnectorFactory.ParsedUri p = ClickHouseConnectorFactory.parseUri("clickhouse+https://secure.host/db/tbl");
        assertEquals(ClickHouseConnectorFactory.DEFAULT_HTTPS_PORT, p.port());
    }

    public void testParseUriMissingTable() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ClickHouseConnectorFactory.parseUri("clickhouse://host:8123/dbonly")
        );
        assertTrue(ex.getMessage().contains("database/table"));
    }

    public void testParseUriMissingPath() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ClickHouseConnectorFactory.parseUri("clickhouse://host:8123/")
        );
        assertTrue(ex.getMessage().contains("database/table"));
    }

    // -------- quoteIdentifier ----------

    public void testQuoteIdentifier() {
        assertEquals("`hello`", ClickHouseConnector.quoteIdentifier("hello"));
    }

    public void testQuoteIdentifierWithBacktick() {
        assertEquals("`back``tick`", ClickHouseConnector.quoteIdentifier("back`tick"));
    }
}
