/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory for ClickHouse HTTP connectors.
 * Handles {@code clickhouse://} and {@code clickhouse+https://} URIs.
 *
 * <p>URI format: {@code clickhouse://host:port/database/table}
 * (port defaults to 8123 for plain HTTP, 8443 for HTTPS)
 */
class ClickHouseConnectorFactory implements ConnectorFactory {

    static final int DEFAULT_HTTP_PORT = 8123;
    static final int DEFAULT_HTTPS_PORT = 8443;

    private static final Set<String> RECOGNISED_KEYS = Set.of("user", "password");
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public boolean canHandle(String location) {
        return location.startsWith("clickhouse://") || location.startsWith("clickhouse+https://");
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(config, List.of(RECOGNISED_KEYS));
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        ParsedUri parsed = parseUri(location);
        String user = Objects.toString(config.get("user"), "default");
        String password = Objects.toString(config.get("password"), "");

        List<Attribute> schema = fetchSchema(parsed, user, password);

        Map<String, Object> resolvedConfig = new HashMap<>();
        resolvedConfig.put("host", parsed.host);
        resolvedConfig.put("port", parsed.port);
        resolvedConfig.put("database", parsed.database);
        resolvedConfig.put("table", parsed.table);
        resolvedConfig.put("tls", parsed.tls);
        resolvedConfig.put("user", user);
        // Carry the password through as-is (may be SecureString or String)
        Object rawPassword = config.get("password");
        resolvedConfig.put("password", rawPassword != null ? rawPassword : "");

        return new SimpleSourceMetadata(schema, "clickhouse", location, null, null, null, resolvedConfig);
    }

    @Override
    public Connector open(Map<String, Object> config) {
        return new ClickHouseConnector(config);
    }

    private List<Attribute> fetchSchema(ParsedUri parsed, String user, String password) {
        String sql = "DESCRIBE TABLE "
            + ClickHouseConnector.quoteIdentifier(parsed.database)
            + "."
            + ClickHouseConnector.quoteIdentifier(parsed.table)
            + " FORMAT JSON";

        String scheme = parsed.tls ? "https" : "http";
        URI uri = URI.create(scheme + "://" + parsed.host + ":" + parsed.port + "/");

        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(uri)
            .header("X-ClickHouse-User", user)
            .header("X-ClickHouse-Key", password)
            .header("Content-Type", "text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
            .timeout(Duration.ofSeconds(60))
            .build();

        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                String errorBody = new String(response.body(), StandardCharsets.UTF_8);
                throw new IllegalStateException("ClickHouse schema fetch failed (HTTP " + response.statusCode() + "): " + errorBody);
            }
            return parseDescribeResponse(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while fetching ClickHouse schema", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to fetch ClickHouse schema: " + e.getMessage(), e);
        }
    }

    /**
     * Parses the JSON response from {@code DESCRIBE TABLE ... FORMAT JSON}.
     * The response has shape {@code {"data": [{"name": "...", "type": "..."}, ...]}}
     */
    private static List<Attribute> parseDescribeResponse(byte[] body) throws IOException {
        List<Attribute> attributes = new ArrayList<>();
        try (JsonParser parser = JSON_FACTORY.createParser(body)) {
            // Navigate to "data" key
            while (parser.nextToken() != null) {
                if (parser.currentToken() == JsonToken.FIELD_NAME && "data".equals(parser.currentName())) {
                    parser.nextToken(); // START_ARRAY
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        // START_OBJECT — one column descriptor
                        String name = null;
                        String chType = null;
                        while (parser.nextToken() != JsonToken.END_OBJECT) {
                            String fieldName = parser.currentName();
                            parser.nextToken(); // value
                            if ("name".equals(fieldName)) {
                                name = parser.getText();
                            } else if ("type".equals(fieldName)) {
                                chType = parser.getText();
                            }
                            // skip other fields (default_type, comment, etc.)
                        }
                        if (name != null && chType != null) {
                            DataType dt = ClickHouseTypeMapping.dataTypeFor(chType);
                            Nullability nullability = ClickHouseTypeMapping.isNullable(chType) ? Nullability.TRUE : Nullability.FALSE;
                            attributes.add(new ReferenceAttribute(Source.EMPTY, null, name, dt, nullability, null, false));
                        }
                    }
                    break;
                }
            }
        }
        return attributes;
    }

    record ParsedUri(String host, int port, String database, String table, boolean tls) {}

    static ParsedUri parseUri(String location) {
        URI uri = URI.create(location);
        boolean tls = "clickhouse+https".equals(uri.getScheme());
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("ClickHouse URI missing host: " + location);
        }
        int port = uri.getPort() > 0 ? uri.getPort() : (tls ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT);

        // Path is expected to be /database/table
        String path = uri.getPath();
        if (path == null || path.equals("/") || path.isBlank()) {
            throw new IllegalArgumentException("ClickHouse URI missing database/table path: " + location);
        }
        // Strip leading slash
        String[] parts = path.startsWith("/") ? path.substring(1).split("/", 2) : path.split("/", 2);
        if (parts.length < 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("ClickHouse URI path must be /database/table, got: " + path + " in " + location);
        }
        return new ParsedUri(host, port, parts[0], parts[1], tls);
    }
}
