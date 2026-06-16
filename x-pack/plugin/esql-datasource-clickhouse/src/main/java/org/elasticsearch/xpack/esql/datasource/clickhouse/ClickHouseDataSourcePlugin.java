/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;

import java.util.Map;
import java.util.Set;

/**
 * Registers the ClickHouse HTTP connector for ESQL external data sources.
 * Handles {@code clickhouse://} and {@code clickhouse+https://} URIs.
 *
 * <p>Queries ClickHouse over HTTP using {@code FORMAT JSONCompactColumns} for columnar
 * data transfer, avoiding per-row overhead compared to row-oriented formats.
 */
public class ClickHouseDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedConnectorSchemes() {
        return Set.of("clickhouse", "clickhouse+https");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("clickhouse", new ClickHouseConnectorFactory());
    }
}
