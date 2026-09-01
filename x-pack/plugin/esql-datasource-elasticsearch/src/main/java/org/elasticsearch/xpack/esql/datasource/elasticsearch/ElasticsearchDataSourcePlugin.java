/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;

import java.io.IOException;
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

    // Plaintext and TLS (+https) variants of both aliases. TLS is opt-in via the scheme suffix.
    static final Set<String> SCHEMES = Set.of("es", "elasticsearch", "es+https", "elasticsearch+https");

    private final ElasticsearchConnectorFactory factory = new ElasticsearchConnectorFactory();

    @Override
    public void close() throws IOException {
        factory.close();
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return SCHEMES;
    }

    @Override
    public Map<String, ExternalSourceFactory> sourceFactories(Settings settings) {
        return Map.of(TYPE, factory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        // Enables `PUT _query/data_source` with `type: elasticsearch`, so the endpoint and (encrypted) API key
        // are registered once and referenced by datasets instead of being repeated in every inline query.
        DataSourceValidator validator = new ElasticsearchDataSourceValidator();
        return Map.of(validator.type(), validator);
    }
}
