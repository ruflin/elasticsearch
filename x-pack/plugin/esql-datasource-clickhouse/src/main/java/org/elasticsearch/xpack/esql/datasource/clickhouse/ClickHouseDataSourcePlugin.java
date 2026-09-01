/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
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
 *
 * <p>ClickHouse is not in the released ship set yet, so registration of both schemes, the
 * connector, and the named-datasource validator is gated on
 * {@link #ESQL_EXTERNAL_DATASOURCES_CLICKHOUSE_FEATURE_FLAG}: available in snapshot/development
 * builds, disabled in release. When the gate is off the schemes and connector are not registered,
 * so a source targeting ClickHouse resolves to the generic "unsupported storage scheme" rejection.
 */
public class ClickHouseDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates the ClickHouse storage providers, connector, and CRUD validator. Snapshot-on,
     * release-off; override in release with
     * {@code -Des.esql_external_datasources_clickhouse_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_CLICKHOUSE_FEATURE_FLAG = new FeatureFlag(
        "esql_external_datasources_clickhouse"
    );

    /** Plaintext and TLS ({@code +https}) variants of the ClickHouse scheme. */
    static final Set<String> SCHEMES = Set.of("clickhouse", "clickhouse+https");

    private static boolean enabled() {
        return ESQL_EXTERNAL_DATASOURCES_CLICKHOUSE_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        // A placeholder storage provider so the resolver can register a concrete file-list entry for
        // these schemes; actual reads go through the connector. See ClickHouseStorageProvider.
        return SCHEMES;
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(ClickHouseStorageProvider::new);
        Map<String, StorageProviderFactory> providers = new HashMap<>();
        for (String scheme : SCHEMES) {
            providers.put(scheme, factory);
        }
        return Map.copyOf(providers);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        if (enabled() == false) {
            return Set.of();
        }
        return SCHEMES;
    }

    @Override
    public Map<String, ExternalSourceFactory> sourceFactories(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        return Map.of("clickhouse", new ClickHouseConnectorFactory());
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        // Enables `PUT _query/data_source` with `type: clickhouse`, so the connection user and (encrypted)
        // password are registered once and referenced by datasets instead of being repeated per query.
        DataSourceValidator validator = new ClickHouseDataSourceValidator();
        return Map.of(validator.type(), validator);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        // ClickHouseSplit travels in the physical plan, so it must be registered for transport serialization.
        // Plugin#getNamedWriteables is the registry the node actually consults; DataSourcePlugin#namedWriteables
        // is unused on this branch.
        return List.of(ClickHouseSplit.ENTRY);
    }
}
