/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Map;

/**
 * CRUD-time validator for the {@code elasticsearch} named data source.
 * <p>
 * A data source stores the shared credential ({@code api_key}, optional). A dataset binds that credential to a
 * concrete remote cluster + index via its {@code resource}, a full {@code es://host:port/index} (or
 * {@code es+https://host/index}) URI. A query then references the dataset by name (e.g. {@code FROM remote_logs})
 * without repeating the API key, while the per-dataset {@code resource} carries the cluster URL and target.
 * Datasets take no extra settings.
 */
public class ElasticsearchDataSourceValidator implements DataSourceValidator {

    @Override
    public String type() {
        return ElasticsearchDataSourcePlugin.TYPE;
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        // Rejects unknown keys and classifies api_key as a secret so it is encrypted into cluster state.
        // An empty map is allowed: an anonymous remote cluster needs no stored credential.
        ElasticsearchConfiguration config = ElasticsearchConfiguration.fromMap(datasourceSettings);
        return config != null ? config.toStoredSettings() : Map.of();
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();
        if (resource == null || resource.isBlank()) {
            errors.addValidationError(
                "[resource] is required and must be a remote Elasticsearch URI, e.g. es+https://host:443/index-pattern"
            );
        } else if (ElasticsearchConnectorFactory.canHandleLocation(resource) == false) {
            errors.addValidationError(
                "[resource] must use one of the schemes [es://, elasticsearch://, es+https://, elasticsearch+https://] "
                    + "but was ["
                    + resource
                    + "]"
            );
        } else {
            // Reuse the connector's own parsing + target validation so an unusable URI is rejected at
            // registration time rather than at query time.
            try {
                ElasticsearchConnectorFactory.Endpoint endpoint = ElasticsearchConnectorFactory.parseLocation(resource);
                EsqlIdentifiers.validateTarget(endpoint.target());
            } catch (IllegalArgumentException e) {
                errors.addValidationError("[resource] is not a valid remote Elasticsearch URI: " + e.getMessage());
            }
        }
        if (datasetSettings != null && datasetSettings.isEmpty() == false) {
            errors.addValidationError(
                "elasticsearch datasets take no settings; unexpected "
                    + datasetSettings.keySet()
                    + " (the remote cluster URL and target are the dataset resource)"
            );
        }
        errors.throwIfValidationErrorsExist();
        return Map.of();
    }
}
