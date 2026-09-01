/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfiguration;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.plaintext;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.secret;

/**
 * Named-datasource settings for ClickHouse: optional connection {@code user} (plaintext) and
 * {@code password} (secret). Both are optional so an anonymous ClickHouse server (default user,
 * empty password) can be registered with an empty settings map. Unknown fields are rejected by
 * {@link DataSourceConfiguration}.
 */
public final class ClickHouseConfiguration extends DataSourceConfiguration {

    static final DataSourceConfigDefinition USER = plaintext("user");
    static final DataSourceConfigDefinition PASSWORD = secret("password");

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(USER, PASSWORD);

    private ClickHouseConfiguration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    private ClickHouseConfiguration(Map<String, Object> raw, Set<String> preexistingSecretKeys) {
        super(raw, FIELDS, preexistingSecretKeys);
    }

    @Override
    protected void validate(ValidationException errors) {
        // user and password are independently optional; no cross-field constraints.
    }

    /** Returns {@code null} for empty input so callers treat a settings-less datasource as "no configuration". */
    public static ClickHouseConfiguration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new ClickHouseConfiguration(raw);
    }

    /**
     * Like {@link #fromMap(Map)}, but for a PUT-as-update: an omitted {@code password} still
     * satisfies secret-presence checks when its name is in {@code preexistingSecretKeys}.
     */
    public static ClickHouseConfiguration fromMap(Map<String, Object> raw, Set<String> preexistingSecretKeys) {
        return raw == null || raw.isEmpty() ? null : new ClickHouseConfiguration(raw, preexistingSecretKeys);
    }
}
