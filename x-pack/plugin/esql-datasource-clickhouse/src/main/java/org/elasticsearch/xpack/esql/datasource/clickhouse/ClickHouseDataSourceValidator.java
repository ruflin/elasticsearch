/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfiguration;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidationUtils.rejectUnknownFields;

/**
 * CRUD-time validator for the {@code clickhouse} named data source.
 *
 * <p>A data source stores the shared connection credentials ({@code user}, optional; {@code password},
 * optional secret) via {@link ClickHouseConfiguration}. A dataset binds those credentials to a
 * concrete ClickHouse database + table via its {@code resource}, a full
 * {@code clickhouse://host:port/database/table} (or {@code clickhouse+https://...}) URI. A query then
 * references the dataset by name (e.g. {@code FROM remote_logs}) without repeating the credentials.
 * Datasets take no extra settings.
 *
 * <p>Registering this validator under type {@code clickhouse} is what enables
 * {@code PUT _query/data_source} with {@code "type": "clickhouse"}: {@link ClickHouseConfiguration}
 * classifies {@code password} as a secret so it is encrypted into cluster state, while {@code user}
 * is stored in plaintext.
 */
public class ClickHouseDataSourceValidator implements DataSourceValidator {

    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        return validateDatasource(datasourceSettings, Set.of());
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings, Set<String> existingSecretKeys) {
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            return Map.of();
        }
        DataSourceConfiguration config = ClickHouseConfiguration.fromMap(datasourceSettings, existingSecretKeys);
        return config != null ? config.toStoredSettings() : Map.of();
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();
        validateResource(resource, errors);
        if (datasetSettings != null) {
            rejectUnknownFields(datasetSettings, Set.of(), errors);
        }
        errors.throwIfValidationErrorsExist();
        return Map.of();
    }

    private static void validateResource(String resource, ValidationException errors) {
        if (resource == null || resource.isBlank()) {
            errors.addValidationError("[resource] is required and must be a ClickHouse URI, e.g. clickhouse://host:8123/database/table");
            return;
        }
        // Case-insensitive scheme match, appending "://" so e.g. "clickhousefoo://" does not match.
        boolean schemeMatch = false;
        for (String scheme : ClickHouseDataSourcePlugin.SCHEMES) {
            String prefix = scheme + "://";
            if (resource.regionMatches(true, 0, prefix, 0, prefix.length())) {
                schemeMatch = true;
                break;
            }
        }
        if (schemeMatch == false) {
            errors.addValidationError(
                "[resource] must use one of the schemes [clickhouse://, clickhouse+https://] but was [" + resource + "]"
            );
            return;
        }
        // Reuse the connector's own URI parsing so an unusable resource (missing host or database/table
        // path) is rejected at registration time rather than at query time.
        try {
            ClickHouseConnectorFactory.parseUri(resource);
        } catch (IllegalArgumentException e) {
            errors.addValidationError("[resource] is not a valid ClickHouse URI: " + e.getMessage());
        }
    }
}
