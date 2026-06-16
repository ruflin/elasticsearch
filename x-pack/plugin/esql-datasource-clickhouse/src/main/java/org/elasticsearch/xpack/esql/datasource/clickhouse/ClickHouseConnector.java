/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Live connection to a ClickHouse instance via HTTP.
 * Builds a SQL {@code SELECT} statement from the {@link QueryRequest} and posts it to
 * ClickHouse's HTTP API, returning the response as a {@link ClickHouseResultCursor}.
 */
class ClickHouseConnector implements Connector {

    private static final Logger logger = LogManager.getLogger(ClickHouseConnector.class);
    private static final int QUERY_TIMEOUT_SECONDS = 300;

    private final HttpClient httpClient;
    private final String host;
    private final int port;
    private final String database;
    private final String table;
    private final String user;
    private final String password;
    private final boolean useTls;

    ClickHouseConnector(Map<String, Object> config) {
        this.host = Objects.toString(config.get("host"), "localhost");
        this.port = config.get("port") instanceof Integer p ? p : 8123;
        this.database = Objects.toString(config.get("database"), "default");
        this.table = Objects.toString(config.get("table"), "");
        this.user = Objects.toString(config.get("user"), "default");
        this.password = Objects.toString(config.get("password"), "");
        this.useTls = Boolean.TRUE.equals(config.get("tls"));

        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        return doExecute(request);
    }

    @Override
    public ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return doExecute(request);
    }

    private ResultCursor doExecute(QueryRequest request) {
        String sql = buildSql(request);
        logger.debug("Executing ClickHouse query: {}", sql);

        String scheme = useTls ? "https" : "http";
        URI uri = URI.create(scheme + "://" + host + ":" + port + "/");

        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(uri)
            .header("X-ClickHouse-User", user)
            .header("X-ClickHouse-Key", password)
            .header("Content-Type", "text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
            .timeout(Duration.ofSeconds(QUERY_TIMEOUT_SECONDS))
            .build();

        try {
            HttpResponse<byte[]> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                String errorBody = new String(response.body(), StandardCharsets.UTF_8);
                throw new IOException("ClickHouse returned HTTP " + response.statusCode() + ": " + errorBody);
            }
            return new ClickHouseResultCursor(
                new java.io.ByteArrayInputStream(response.body()),
                request.attributes(),
                request.blockFactory()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while executing ClickHouse query", e);
        } catch (IOException e) {
            throw new RuntimeException("ClickHouse query failed: " + e.getMessage(), e);
        }
    }

    private String buildSql(QueryRequest request) {
        List<String> columns = request.projectedColumns();
        String selectClause;
        if (columns == null || columns.isEmpty()) {
            selectClause = "*";
        } else {
            StringJoiner sj = new StringJoiner(", ");
            for (String col : columns) {
                sj.add(quoteIdentifier(col));
            }
            selectClause = sj.toString();
        }

        String fromClause = quoteIdentifier(database) + "." + quoteIdentifier(table);
        StringBuilder sql = new StringBuilder("SELECT ").append(selectClause).append(" FROM ").append(fromClause);

        int rowLimit = request.rowLimit();
        if (rowLimit > 0) {
            sql.append(" LIMIT ").append(rowLimit);
        }

        sql.append(" FORMAT JSONCompactColumns");
        return sql.toString();
    }

    /** Wraps a ClickHouse identifier in backtick quotes and escapes any backticks within it. */
    static String quoteIdentifier(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    @Override
    public void close() {
        // HttpClient is managed by the JVM; no explicit close needed
    }

    @Override
    public String toString() {
        return "ClickHouseConnector{host=" + host + ", port=" + port + ", database=" + database + ", table=" + table + "}";
    }
}
