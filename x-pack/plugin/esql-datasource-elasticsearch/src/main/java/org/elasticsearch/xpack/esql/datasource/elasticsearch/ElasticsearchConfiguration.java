/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfiguration;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceConfigDefinition.secret;

/**
 * Configuration for a named {@code elasticsearch} data source. It holds the credential that is shared across
 * the data source's datasets, so it is stored (encrypted) once rather than repeated in every query:
 * <ul>
 *   <li>{@code api_key} — the base64 API key used for {@code Authorization: ApiKey ...} (secret, optional;
 *       omit for an unauthenticated cluster).</li>
 * </ul>
 * The remote cluster URL and target index/data-stream pattern come from each dataset's {@code resource}
 * (a full {@code es://host:port/index} or {@code es+https://host/index} URI), so different datasets can point
 * at different clusters or indices while reusing the same stored credential.
 */
public class ElasticsearchConfiguration extends DataSourceConfiguration {

    static final DataSourceConfigDefinition API_KEY = secret("api_key");

    private static final Map<String, DataSourceConfigDefinition> FIELDS = DataSourceConfigDefinition.mapOf(API_KEY);

    private ElasticsearchConfiguration(Map<String, Object> raw) {
        super(raw, FIELDS);
    }

    public static ElasticsearchConfiguration fromMap(Map<String, Object> raw) {
        return raw == null || raw.isEmpty() ? null : new ElasticsearchConfiguration(raw);
    }

    /** Lenient factory for query-time config maps that may carry keys this configuration does not own. */
    public static Configured<ElasticsearchConfiguration> fromQueryConfig(Map<String, Object> raw) {
        return filterAndConstruct(raw, FIELDS, ElasticsearchConfiguration::new);
    }

    @Override
    protected void validate(ValidationException errors) {
        // api_key is optional; anonymous clusters need none. When present, reject CRLF sequences to
        // prevent HTTP header injection: the value is concatenated verbatim into an Authorization header
        // and Apache HttpClient 4.5.x does not strip control characters from BasicHeader values.
        String key = apiKey();
        if (key != null && (key.indexOf('\r') >= 0 || key.indexOf('\n') >= 0)) {
            errors.addValidationError("api_key must not contain line-break characters");
        }
    }

    public String apiKey() {
        return get(API_KEY.name());
    }
}
