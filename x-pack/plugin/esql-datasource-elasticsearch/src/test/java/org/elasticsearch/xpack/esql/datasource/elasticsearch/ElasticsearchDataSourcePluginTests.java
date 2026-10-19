/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;

/**
 * Registration shape for the elasticsearch connector plugin. Registration is gated on
 * {@code esql_external_datasources_elasticsearch} (snapshot-on, release-off), matching the gRPC connector.
 */
public class ElasticsearchDataSourcePluginTests extends ESTestCase {

    private static boolean enabled() {
        return ElasticsearchDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_ELASTICSEARCH_FEATURE_FLAG.isEnabled();
    }

    public void testRegistersSchemesAndValidatorWhenEnabled() {
        assumeTrue("requires elasticsearch connector feature flag", enabled());
        ElasticsearchDataSourcePlugin plugin = new ElasticsearchDataSourcePlugin();
        assertEquals(ElasticsearchDataSourcePlugin.SCHEMES, plugin.supportedSchemes());
        assertEquals(ElasticsearchDataSourcePlugin.SCHEMES, plugin.supportedConnectorSchemes());

        Map<String, StorageProviderFactory> providers = plugin.storageProviders(Settings.EMPTY);
        for (String scheme : ElasticsearchDataSourcePlugin.SCHEMES) {
            assertTrue("should register scheme [" + scheme + "]", providers.containsKey(scheme));
        }

        Map<String, ConnectorFactory> connectors = plugin.connectors(Settings.EMPTY);
        assertTrue(connectors.containsKey(ElasticsearchDataSourcePlugin.TYPE));

        Map<String, DataSourceValidator> validators = plugin.datasourceValidators(Settings.EMPTY);
        assertTrue(validators.containsKey(ElasticsearchDataSourcePlugin.TYPE));
        assertEquals(1, validators.size());
    }

    public void testDisabledWhenFeatureFlagOff() {
        assumeFalse("only when elasticsearch connector feature flag is off", enabled());
        ElasticsearchDataSourcePlugin plugin = new ElasticsearchDataSourcePlugin();
        assertTrue(plugin.supportedSchemes().isEmpty());
        assertTrue(plugin.supportedConnectorSchemes().isEmpty());
        assertTrue(plugin.storageProviders(Settings.EMPTY).isEmpty());
        assertTrue(plugin.connectors(Settings.EMPTY).isEmpty());
        assertTrue(plugin.datasourceValidators(Settings.EMPTY).isEmpty());
    }
}
