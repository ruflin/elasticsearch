/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.HashMap;
import java.util.List;
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

    /** Plaintext and TLS ({@code +https}) variants of the ClickHouse scheme. */
    static final Set<String> SCHEMES = Set.of("clickhouse", "clickhouse+https");

    @Override
    public Set<String> supportedSchemes() {
        // A placeholder storage provider so the resolver can register a concrete file-list entry for
        // these schemes; actual reads go through the connector. See ClickHouseStorageProvider.
        return SCHEMES;
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(ClickHouseStorageProvider::new);
        Map<String, StorageProviderFactory> providers = new HashMap<>();
        for (String scheme : SCHEMES) {
            providers.put(scheme, factory);
        }
        return Map.copyOf(providers);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return SCHEMES;
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("clickhouse", new ClickHouseConnectorFactory());
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        // Enables `PUT _query/data_source` with `type: clickhouse`, so the connection user and (encrypted)
        // password are registered once and referenced by datasets instead of being repeated per query.
        DataSourceValidator validator = new ClickHouseDataSourceValidator();
        return Map.of(validator.type(), validator);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> namedWriteables() {
        // ClickHouseSplit travels in the physical plan, so it must be registered for transport serialization.
        return List.of(ClickHouseSplit.ENTRY);
    }
}
