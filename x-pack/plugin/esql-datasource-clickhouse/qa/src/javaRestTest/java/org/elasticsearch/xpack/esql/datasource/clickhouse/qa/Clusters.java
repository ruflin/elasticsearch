/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse.qa;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

/**
 * Cluster configuration for the ClickHouse external data source integration test.
 *
 * <p>Mirrors the ES|QL external-data-sources cluster setup: the {@code esql_external_datasources}
 * feature flag enables the {@code _query/data_source} + {@code _query/dataset} CRUD APIs, and the
 * project encryption key (PEK) is required so the data source's (secret) ClickHouse password can be
 * stored encrypted in cluster state. The {@code esql-datasource-clickhouse} plugin is installed via
 * {@code clusterPlugins} in {@code build.gradle}, which is how the {@code clickhouse} data source
 * type and connector reach the node.
 */
final class Clusters {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String ENCRYPTION_PASSWORD = "esql-clickhouse-test-encryption-password";

    private Clusters() {}

    static ElasticsearchCluster testCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.ml.enabled", "false")
            // Enables the _query/data_source + _query/dataset CRUD APIs and FROM <dataset>.
            .feature(FeatureFlag.ESQL_EXTERNAL_DATASOURCES)
            // External data sources require the PEK feature; EsqlPlugin fails fast at startup otherwise.
            .systemProperty("es.project_encryption_key_feature_flag_enabled", "true")
            // Configure a wrapping password so the master installs the PEK and secrets can be encrypted.
            .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, ENCRYPTION_PASSWORD)
            .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
            .shared(true)
            .build();
    }
}
