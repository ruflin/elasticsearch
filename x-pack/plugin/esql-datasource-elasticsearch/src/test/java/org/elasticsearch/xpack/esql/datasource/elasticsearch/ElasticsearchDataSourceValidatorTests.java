/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ElasticsearchDataSourceValidatorTests extends ESTestCase {

    private final ElasticsearchDataSourceValidator validator = new ElasticsearchDataSourceValidator();

    public void testType() {
        assertEquals("elasticsearch", validator.type());
    }

    public void testDatasourceStoresApiKeyAsSecret() {
        Map<String, DataSourceSetting> stored = validator.validateDatasource(Map.of("api_key", "encoded-key"));
        assertThat(stored.keySet(), equalTo(Set.of("api_key")));
        DataSourceSetting apiKey = stored.get("api_key");
        assertThat(apiKey.secret(), is(true));
        assertThat(apiKey.rawValue(), equalTo("encoded-key"));
    }

    public void testDatasourceAllowsEmptySettingsForAnonymousCluster() {
        assertThat(validator.validateDatasource(Map.of()), equalTo(Map.of()));
        assertThat(validator.validateDatasource(null), equalTo(Map.of()));
    }

    public void testDatasourceRejectsUnknownKey() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("endpoint", "https://host:443"))
        );
        assertThat(e.getMessage(), containsString("endpoint"));
    }

    public void testDatasetResourceMustBeElasticsearchUri() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://bucket/key", Map.of())
        );
        assertThat(e.getMessage(), containsString("es://"));
    }

    public void testDatasetResourceRequired() {
        ValidationException e = expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "  ", Map.of()));
        assertThat(e.getMessage(), containsString("[resource] is required"));
    }

    public void testDatasetResourceMustIncludeIndex() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "es+https://host:443", Map.of())
        );
        assertThat(e.getMessage(), containsString("missing index"));
    }

    public void testDatasetRejectsInjectionInTarget() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "es://host:9200/bad|index", Map.of())
        );
        assertThat(e.getMessage(), containsString("[resource]"));
    }

    public void testDatasetRejectsSettings() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "es+https://host:443/logs*", Map.of("format", "json"))
        );
        assertThat(e.getMessage(), containsString("take no settings"));
    }

    public void testValidDatasetReturnsNoSettings() {
        assertThat(validator.validateDataset(Map.of(), "es+https://host:443/logs*", Map.of()), equalTo(Map.of()));
        assertThat(validator.validateDataset(Map.of(), "es://localhost:9200/metrics-*", null), equalTo(Map.of()));
    }
}
