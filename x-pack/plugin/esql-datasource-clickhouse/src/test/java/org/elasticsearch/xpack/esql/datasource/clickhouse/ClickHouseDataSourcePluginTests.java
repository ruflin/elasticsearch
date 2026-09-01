/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;

import static org.elasticsearch.xpack.esql.datasource.clickhouse.ClickHouseDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_CLICKHOUSE_FEATURE_FLAG;

public class ClickHouseDataSourcePluginTests extends ESTestCase {

    private final ClickHouseDataSourcePlugin plugin = new ClickHouseDataSourcePlugin();

    private static boolean enabled() {
        return ESQL_EXTERNAL_DATASOURCES_CLICKHOUSE_FEATURE_FLAG.isEnabled();
    }

    public void testSchemesRegisteredWhenFlagEnabled() {
        assumeTrue("requires clickhouse datasource feature flag", enabled());
        assertEquals(ClickHouseDataSourcePlugin.SCHEMES, plugin.supportedSchemes());
        assertEquals(ClickHouseDataSourcePlugin.SCHEMES, plugin.supportedConnectorSchemes());
    }

    public void testSourceFactoryRegisteredWhenFlagEnabled() {
        assumeTrue("requires clickhouse datasource feature flag", enabled());
        ExternalSourceFactory factory = plugin.sourceFactories(Settings.EMPTY).get("clickhouse");
        assertNotNull(factory);
        assertEquals("clickhouse", factory.type());
        assertTrue(factory.canHandle("clickhouse://localhost:8123/db/tbl"));
    }

    public void testValidatorRegisteredWhenFlagEnabled() {
        assumeTrue("requires clickhouse datasource feature flag", enabled());
        DataSourceValidator validator = plugin.datasourceValidators(Settings.EMPTY).get("clickhouse");
        assertNotNull(validator);
        assertEquals("clickhouse", validator.type());
    }

    public void testConnectorsLeftEmptyInFavourOfSourceFactories() {
        assumeTrue("requires clickhouse datasource feature flag", enabled());
        assertTrue(plugin.connectors(Settings.EMPTY).isEmpty());
    }
}
