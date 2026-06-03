/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Set;

/**
 * Registers the {@code elasticsearch} external data source connector for ES|QL.
 * <p>
 * Lets an ES|QL query pull data from another Elasticsearch cluster over HTTP by running a query
 * against the remote {@code _query} (ES|QL) API and streaming the results back. Handles
 * {@code es://} and {@code elasticsearch://} location URIs.
 */
public class ElasticsearchDataSourcePlugin extends Plugin implements DataSourcePlugin {

    static final String TYPE = "elasticsearch";

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("es", "elasticsearch");
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        // A placeholder storage provider so the resolver can register a concrete file-list entry;
        // actual reads go through the connector. See ElasticsearchStorageProvider.
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(ElasticsearchStorageProvider::new);
        return Map.of("es", factory, "elasticsearch", factory);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return Set.of("es", "elasticsearch");
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of(TYPE, new ElasticsearchConnectorFactory());
    }
}
