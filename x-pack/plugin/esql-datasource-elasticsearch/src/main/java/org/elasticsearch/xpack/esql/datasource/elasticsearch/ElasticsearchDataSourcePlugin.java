/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registers the {@code elasticsearch} external data source connector for ES|QL.
 * <p>
 * Lets an ES|QL query pull data from another Elasticsearch cluster over HTTP by running a query
 * against the remote {@code _query} (ES|QL) API and streaming the results back. Handles
 * {@code es://} and {@code elasticsearch://} location URIs.
 * <p>
 * Registration is gated on {@link #ESQL_EXTERNAL_DATASOURCES_ELASTICSEARCH_FEATURE_FLAG}: available in
 * snapshot/development builds, disabled in release (same pattern as the gRPC/Flight connector). When
 * the gate is off the schemes and connector are not registered, so a source targeting them resolves
 * to the generic "unsupported storage scheme" rejection.
 */
public class ElasticsearchDataSourcePlugin extends Plugin implements DataSourcePlugin {

    static final String TYPE = "elasticsearch";

    /**
     * Gates the elasticsearch connector and storage providers. Snapshot-on, release-off; override in
     * release with {@code -Des.esql_external_datasources_elasticsearch_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_ELASTICSEARCH_FEATURE_FLAG = new FeatureFlag(
        "esql_external_datasources_elasticsearch"
    );

    // Plaintext and TLS (+https) variants of both aliases. TLS is opt-in via the scheme suffix.
    static final Set<String> SCHEMES = Set.of("es", "elasticsearch", "es+https", "elasticsearch+https");

    private static boolean enabled() {
        return ESQL_EXTERNAL_DATASOURCES_ELASTICSEARCH_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<String> supportedSchemes() {
        return enabled() ? SCHEMES : Set.of();
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        // A placeholder storage provider so the resolver can register a concrete file-list entry;
        // actual reads go through the connector. See ElasticsearchStorageProvider.
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(ElasticsearchStorageProvider::new);
        Map<String, StorageProviderFactory> providers = new HashMap<>();
        for (String scheme : SCHEMES) {
            providers.put(scheme, factory);
        }
        return Map.copyOf(providers);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return enabled() ? SCHEMES : Set.of();
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        return Map.of(TYPE, new ElasticsearchConnectorFactory());
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        // Enables `PUT _query/data_source` with `type: elasticsearch`, so the endpoint and (encrypted) API key
        // are registered once and referenced by datasets instead of being repeated in every inline query.
        DataSourceValidator validator = new ElasticsearchDataSourceValidator();
        return Map.of(validator.type(), validator);
    }
}
