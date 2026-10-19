/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import static org.elasticsearch.xpack.esql.datasource.clickhouse.ClickHouseDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_CLICKHOUSE_FEATURE_FLAG;

/**
 * Registration tests for {@link ClickHouseDataSourcePlugin}. ClickHouse is gated on
 * {@code esql_external_datasources_clickhouse} (snapshot-on, release-off). The shape tests assume
 * the gate is on; a dedicated test asserts nothing is registered when it is off.
 */
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

    public void testConnectorRegisteredWhenFlagEnabled() {
        assumeTrue("requires clickhouse datasource feature flag", enabled());
        ConnectorFactory factory = plugin.connectors(Settings.EMPTY).get(ClickHouseDataSourcePlugin.TYPE);
        assertNotNull(factory);
        assertEquals(ClickHouseDataSourcePlugin.TYPE, factory.type());
        assertTrue(factory.canHandle("clickhouse://localhost:8123/db/tbl"));
    }

    public void testValidatorRegisteredWhenFlagEnabled() {
        assumeTrue("requires clickhouse datasource feature flag", enabled());
        DataSourceValidator validator = plugin.datasourceValidators(Settings.EMPTY).get(ClickHouseDataSourcePlugin.TYPE);
        assertNotNull(validator);
        assertEquals(ClickHouseDataSourcePlugin.TYPE, validator.type());
    }

    public void testDisabledWhenFeatureFlagOff() {
        assumeFalse("only when clickhouse datasource feature flag is off", enabled());
        assertTrue(plugin.supportedSchemes().isEmpty());
        assertTrue(plugin.supportedConnectorSchemes().isEmpty());
        assertTrue(plugin.storageProviders(Settings.EMPTY).isEmpty());
        assertTrue(plugin.connectors(Settings.EMPTY).isEmpty());
        assertTrue(plugin.datasourceValidators(Settings.EMPTY).isEmpty());
    }
}
