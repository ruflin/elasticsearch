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
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.http.HttpClient;
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
    private static final Duration QUERY_TIMEOUT = Duration.ofSeconds(300);

    private final HttpClient httpClient;
    private final ClickHouseEndpoint endpoint;
    private final String database;
    private final String table;

    ClickHouseConnector(Map<String, Object> config, HttpClient httpClient) {
        this.httpClient = httpClient;
        this.endpoint = new ClickHouseEndpoint(
            Objects.toString(config.get("host"), "localhost"),
            config.get("port") instanceof Integer p ? p : ClickHouseConnectorFactory.DEFAULT_HTTP_PORT,
            Boolean.TRUE.equals(config.get("tls")),
            Objects.toString(config.get("user"), "default"),
            Objects.toString(config.get("password"), "")
        );
        this.database = Objects.toString(config.get("database"), "default");
        this.table = Objects.toString(config.get("table"), "");
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        // ClickHouseSplit is ignored: the single split covers the whole table, so the default
        // ExternalSplit overload in Connector routes here as well.
        // An ungrouped COUNT(*) projects no columns. Returning column data would be wasteful and, more
        // importantly, a columnar response with zero columns carries no row count, so the count operator
        // would see zero rows. Instead ask ClickHouse for count() and emit a positions-only page.
        boolean countOnly = request.attributes() == null || request.attributes().isEmpty();
        String sql = buildSql(request, countOnly);
        logger.debug("Executing ClickHouse query: {}", sql);

        byte[] body = endpoint.post(httpClient, sql, QUERY_TIMEOUT, "query");
        try {
            if (countOnly) {
                return ClickHouseResultCursor.forRowCount(new ByteArrayInputStream(body), request.blockFactory());
            }
            return new ClickHouseResultCursor(new ByteArrayInputStream(body), request.attributes(), request.blockFactory());
        } catch (IOException e) {
            throw ClickHouseFailures.parse(e);
        }
    }

    private String buildSql(QueryRequest request, boolean countOnly) {
        String fromClause = quoteIdentifier(database) + "." + quoteIdentifier(table);
        if (countOnly) {
            // FORMAT JSONCompactColumns yields [[N]] for a single aggregate column.
            return "SELECT count() FROM " + fromClause + " FORMAT JSONCompactColumns";
        }

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
        // The HttpClient is owned by ClickHouseConnectorFactory and closed with the plugin.
    }

    @Override
    public String toString() {
        return "ClickHouseConnector{database=" + database + ", table=" + table + "}";
    }
}
