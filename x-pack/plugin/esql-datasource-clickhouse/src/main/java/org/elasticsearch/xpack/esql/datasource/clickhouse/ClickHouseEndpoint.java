/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Connection details for one ClickHouse HTTP endpoint, shared by schema discovery and query execution.
 * A {@code POST} of one SQL statement with the user and password in headers is the whole protocol, so
 * both paths go through {@link #post} and get the same status and failure mapping.
 *
 * <p>Deliberately not a record: the password must not appear in {@code toString()} output.
 */
final class ClickHouseEndpoint {

    private final String host;
    private final int port;
    private final boolean tls;
    private final String user;
    private final String password;

    ClickHouseEndpoint(String host, int port, boolean tls, String user, String password) {
        this.host = host;
        this.port = port;
        this.tls = tls;
        this.user = user;
        this.password = password;
    }

    /**
     * Posts {@code sql} and returns the response body. A non-200 status or a transport failure is thrown as the
     * matching {@link org.elasticsearch.xpack.esql.datasources.spi.ExternalException}; {@code operation} names the
     * step in the message, e.g. {@code "query"}.
     */
    byte[] post(HttpClient client, String sql, Duration timeout, String operation) {
        URI uri = URI.create((tls ? "https" : "http") + "://" + host + ":" + port + "/");
        HttpRequest request = HttpRequest.newBuilder()
            .uri(uri)
            .header("X-ClickHouse-User", user)
            .header("X-ClickHouse-Key", password)
            .header("Content-Type", "text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(sql, StandardCharsets.UTF_8))
            .timeout(timeout)
            .build();
        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                String errorBody = new String(response.body(), StandardCharsets.UTF_8);
                throw ClickHouseFailures.httpStatus(response.statusCode(), errorBody, operation);
            }
            return response.body();
        } catch (InterruptedException e) {
            throw ClickHouseFailures.interrupted(e, operation);
        } catch (IOException e) {
            throw ClickHouseFailures.transport(e, operation);
        }
    }
}
