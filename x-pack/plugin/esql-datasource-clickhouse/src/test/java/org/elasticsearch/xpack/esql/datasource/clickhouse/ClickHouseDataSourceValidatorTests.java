/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractDataSourceValidatorTests;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class ClickHouseDataSourceValidatorTests extends AbstractDataSourceValidatorTests {

    private final DataSourceValidator validator = new ClickHouseDataSourceValidator();

    @Override
    protected DataSourceValidator validator() {
        return validator;
    }

    @Override
    protected String expectedType() {
        return "clickhouse";
    }

    @Override
    protected Map<String, Object> sampleConfigWithAllSecrets() {
        return Map.of("user", "default", "password", "s3cr3t");
    }

    @Override
    protected Set<String> expectedSecretFieldNames() {
        return Set.of("password");
    }

    @Override
    protected String sampleResource() {
        return "clickhouse://localhost:8123/logs/app_logs";
    }

    @Override
    protected String wrongSchemeResource() {
        return "s3://bucket/path";
    }

    @Override
    protected Map<String, DataSourceSetting> storedSettingsFromSampleConfig() {
        return ClickHouseConfiguration.fromMap(sampleConfigWithAllSecrets()).toStoredSettings();
    }

    @Override
    protected Map<String, Object> datasetSettingsWithMultipleErrors() {
        return Map.of("format", "csv", "delimiter", ";");
    }

    /**
     * {@code password} is optional, so omitting it is valid even without existing-secret context.
     * The merge-aware path must still avoid re-emitting a carried-forward secret.
     */
    @Override
    public void testMergeAwareValidateDatasourceOmitsExistingSecrets() {
        Map<String, Object> withoutSecrets = new HashMap<>(sampleConfigWithAllSecrets());
        expectedSecretFieldNames().forEach(withoutSecrets::remove);

        Map<String, DataSourceSetting> without = validator().validateDatasource(withoutSecrets);
        for (String key : expectedSecretFieldNames()) {
            assertFalse(without.containsKey(key));
        }

        Map<String, DataSourceSetting> merged = validator().validateDatasource(withoutSecrets, expectedSecretFieldNames());
        for (String key : expectedSecretFieldNames()) {
            assertFalse(
                "merge-aware validateDatasource should not re-emit an omitted, carried-forward secret [" + key + "]",
                merged.containsKey(key)
            );
        }
    }

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("user", "alice", "password", "s3cr3t"));
        assertEquals("alice", result.get("user").nonSecretValue());
        assertFalse(result.get("user").secret());
        assertTrue(result.get("password").secret());
        assertEquals("s3cr3t", result.get("password").rawValue());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("endpoint", "http://localhost")));
    }

    public void testValidateDatasetValid() {
        assertTrue(validator.validateDataset(Map.of(), sampleResource(), Map.of()).isEmpty());
        assertTrue(validator.validateDataset(Map.of(), "clickhouse+https://secure.host:8443/db/tbl", Map.of()).isEmpty());
    }

    public void testValidateDatasetSchemeIsCaseInsensitive() {
        assertTrue(validator.validateDataset(Map.of(), "CLICKHOUSE://localhost:8123/db/tbl", Map.of()).isEmpty());
        assertTrue(validator.validateDataset(Map.of(), "ClickHouse+HTTPS://secure.host/db/tbl", Map.of()).isEmpty());
    }

    public void testValidateDatasetRejectsPrefixCollision() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "clickhousefoo://host/db/tbl", Map.of()));
    }

    public void testValidateDatasetRejectsIncompletePath() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "clickhouse://localhost:8123/dbonly", Map.of())
        );
        assertThat(e.getMessage(), containsString("database/table"));
    }
}
