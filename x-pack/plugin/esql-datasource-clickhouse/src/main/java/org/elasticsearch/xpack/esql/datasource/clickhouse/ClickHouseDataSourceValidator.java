/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;

/**
 * CRUD-time validator for the {@code clickhouse} named data source.
 *
 * <p>A data source stores the shared connection credentials ({@code user}, optional; {@code password},
 * optional secret). A dataset binds those credentials to a concrete ClickHouse database + table via its
 * {@code resource}, a full {@code clickhouse://host:port/database/table} (or {@code clickhouse+https://...})
 * URI. A query then references the dataset by name (e.g. {@code FROM remote_logs}) without repeating the
 * credentials, while the per-dataset {@code resource} carries the server URL and target table. Datasets take
 * no extra settings.
 *
 * <p>Registering this validator under type {@code clickhouse} is what enables
 * {@code PUT _query/data_source} with {@code "type": "clickhouse"}: it classifies {@code password} as a
 * secret so it is encrypted into cluster state, while {@code user} is stored in plaintext.
 */
public class ClickHouseDataSourceValidator implements DataSourceValidator {

    static final String USER = "user";
    static final String PASSWORD = "password";

    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        Map<String, DataSourceSetting> stored = new HashMap<>();
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            // An anonymous ClickHouse server (default user, no password) needs no stored credentials.
            return stored;
        }

        ValidationException errors = new ValidationException();
        for (Map.Entry<String, Object> entry : datasourceSettings.entrySet()) {
            String key = entry.getKey();
            switch (key) {
                // user travels into cluster state in plaintext; password is classified as a secret so it
                // is encrypted at rest (see DataSourceService#applyEncryption).
                case USER -> stored.put(USER, new DataSourceSetting(entry.getValue(), false));
                case PASSWORD -> stored.put(PASSWORD, new DataSourceSetting(entry.getValue(), true));
                default -> errors.addValidationError(
                    "unknown clickhouse data source setting [" + key + "]; recognised settings are [user, password]"
                );
            }
        }
        errors.throwIfValidationErrorsExist();
        return stored;
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();
        if (resource == null || resource.isBlank()) {
            errors.addValidationError("[resource] is required and must be a ClickHouse URI, e.g. clickhouse://host:8123/database/table");
        } else if (resource.startsWith("clickhouse://") == false && resource.startsWith("clickhouse+https://") == false) {
            errors.addValidationError(
                "[resource] must use one of the schemes [clickhouse://, clickhouse+https://] but was [" + resource + "]"
            );
        } else {
            // Reuse the connector's own URI parsing so an unusable resource (missing host or database/table
            // path) is rejected at registration time rather than at query time.
            try {
                ClickHouseConnectorFactory.parseUri(resource);
            } catch (IllegalArgumentException e) {
                errors.addValidationError("[resource] is not a valid ClickHouse URI: " + e.getMessage());
            }
        }
        if (datasetSettings != null && datasetSettings.isEmpty() == false) {
            errors.addValidationError(
                "clickhouse datasets take no settings; unexpected "
                    + datasetSettings.keySet()
                    + " (the server URL and target table are the dataset resource)"
            );
        }
        errors.throwIfValidationErrorsExist();
        return Map.of();
    }
}
