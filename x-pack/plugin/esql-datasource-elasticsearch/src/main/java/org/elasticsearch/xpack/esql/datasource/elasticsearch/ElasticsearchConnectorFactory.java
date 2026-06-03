/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory for the {@code elasticsearch} connector. Handles {@code es://} and {@code elasticsearch://}
 * location URIs of the form {@code es://host:port/index}.
 * <p>
 * Schema resolution runs {@code FROM <index> | LIMIT 0} against the remote {@code _query} API so the
 * resolved schema matches exactly what a real query returns. The remote endpoint, target index, and
 * (optional) API key are stored in the resolved config and handed back to {@link #open} at execution
 * time.
 */
class ElasticsearchConnectorFactory implements ConnectorFactory {

    static final String CONFIG_ENDPOINT = "endpoint";
    static final String CONFIG_TARGET = "target";
    static final String CONFIG_API_KEY = "api_key";

    static final int DEFAULT_PORT = 9200;

    @Override
    public String type() {
        return ElasticsearchDataSourcePlugin.TYPE;
    }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("es://") || location.startsWith("elasticsearch://");
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(config, List.of(Set.of(CONFIG_API_KEY)));
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        // The remote source speaks ES|QL, so filters are pushed by re-rendering them into a remote WHERE clause.
        return EsqlFilterTranslator.INSTANCE;
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        Endpoint endpoint = parseLocation(location);
        String apiKey = Objects.toString(config.get(CONFIG_API_KEY), null);
        try (RestClient client = buildClient(endpoint.baseUrl(), apiKey)) {
            List<Attribute> attributes = resolveSchema(client, endpoint.target());
            Map<String, Object> resolvedConfig = new HashMap<>();
            resolvedConfig.put(CONFIG_ENDPOINT, endpoint.baseUrl());
            resolvedConfig.put(CONFIG_TARGET, endpoint.target());
            if (apiKey != null) {
                resolvedConfig.put(CONFIG_API_KEY, apiKey);
            }
            return new SimpleSourceMetadata(attributes, type(), location, null, null, null, resolvedConfig);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve schema for remote Elasticsearch [" + location + "]", e);
        }
    }

    @Override
    public Connector open(Map<String, Object> config) {
        String baseUrl = Objects.toString(config.get(CONFIG_ENDPOINT), null);
        if (baseUrl == null) {
            throw new IllegalArgumentException("Elasticsearch connector requires '" + CONFIG_ENDPOINT + "' in config");
        }
        String apiKey = Objects.toString(config.get(CONFIG_API_KEY), null);
        return new ElasticsearchConnector(buildClient(baseUrl, apiKey));
    }

    private static RestClient buildClient(String baseUrl, String apiKey) {
        var builder = RestClient.builder(HttpHost.create(baseUrl));
        if (apiKey != null) {
            builder.setDefaultHeaders(new org.apache.http.Header[] { new BasicHeader("Authorization", "ApiKey " + apiKey) });
        }
        return builder.build();
    }

    private static List<Attribute> resolveSchema(RestClient client, String target) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("format", "json");
        request.setJsonEntity("{\"query\":\"FROM " + target + " | LIMIT 0\",\"columnar\":true}");
        Response response = client.performRequest(request);
        List<EsqlTypeMapping.RemoteColumn> columns = new ArrayList<>();
        try (
            InputStream content = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
        ) {
            parser.nextToken();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.FIELD_NAME && "columns".equals(parser.currentName())) {
                    parser.nextToken(); // START_ARRAY
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        String name = null;
                        String type = null;
                        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                            String field = parser.currentName();
                            parser.nextToken();
                            if ("name".equals(field)) {
                                name = parser.text();
                            } else if ("type".equals(field)) {
                                type = parser.text();
                            } else {
                                parser.skipChildren();
                            }
                        }
                        columns.add(new EsqlTypeMapping.RemoteColumn(name, type));
                    }
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        return EsqlTypeMapping.toAttributes(columns);
    }

    /**
     * Parses {@code es://host:port/index} into a base URL and target index. The scheme is normalized
     * to {@code http} for the HTTP client; TLS is out of scope for v1.
     */
    static Endpoint parseLocation(String location) {
        URI uri = URI.create(location);
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid Elasticsearch location [" + location + "]: missing host");
        }
        int port = uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT;
        String path = uri.getPath();
        if (path == null || path.length() <= 1) {
            throw new IllegalArgumentException("Invalid Elasticsearch location [" + location + "]: missing index in path");
        }
        String target = path.substring(1);
        String baseUrl = "http://" + host + ":" + port;
        return new Endpoint(baseUrl, target);
    }

    record Endpoint(String baseUrl, String target) {}
}
