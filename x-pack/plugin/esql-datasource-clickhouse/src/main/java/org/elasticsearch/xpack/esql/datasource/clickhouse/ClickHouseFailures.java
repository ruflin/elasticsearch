/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;

/**
 * Maps ClickHouse HTTP failures onto the ES|QL {@code ExternalException} family so REST status is
 * 400 / 500 / 503 rather than a bare {@code IllegalStateException} or {@code UncheckedIOException}.
 *
 * <p>Mirrors {@code HttpStorageObject}'s status mapping: retryable HTTP statuses and transport
 * failures become {@link ExternalUnavailableException} (503); other non-success statuses and
 * undecodable bodies become {@link ExternalClientException} (400); interrupts stay
 * {@link ExternalServerException} (500).
 */
final class ClickHouseFailures {

    /** Cap on the error-response body snippet folded into a failure message, in bytes. */
    private static final int MAX_ERROR_BODY_CHARS = 512;

    private ClickHouseFailures() {}

    /**
     * Maps a non-200 ClickHouse HTTP status. Retryable codes (429 / 5xx that a retry can clear)
     * become 503; everything else is a client-class 400 (bad table, auth, query).
     */
    static RuntimeException httpStatus(int statusCode, String body, String context) {
        String suffix = truncateBody(body);
        if (ExternalUnavailableException.isRetryableStatus(statusCode)) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(statusCode);
            return new ExternalUnavailableException(throttling, "ClickHouse {} (HTTP {}){}", context, statusCode, suffix);
        }
        return new ExternalClientException("ClickHouse {} (HTTP {}){}", context, statusCode, suffix);
    }

    /** Connect / timeout / reset → 503; other I/O (malformed body, unexpected close) → 400. */
    static RuntimeException transport(IOException e, String context) {
        if (isTransient(e)) {
            return new ExternalUnavailableException(e, "ClickHouse {}: {}", context, detail(e));
        }
        return new ExternalClientException(e, "ClickHouse {}: {}", context, detail(e));
    }

    static RuntimeException parse(IOException e) {
        return new ExternalClientException(e, "ClickHouse response parse failed: {}", detail(e));
    }

    static RuntimeException interrupted(InterruptedException e, String context) {
        Thread.currentThread().interrupt();
        return new ExternalServerException(e, "Interrupted while {}", context);
    }

    private static boolean isTransient(IOException e) {
        return e instanceof ConnectException
            || e instanceof HttpTimeoutException
            || e instanceof UnknownHostException
            || e instanceof SocketException;
    }

    private static String truncateBody(String body) {
        if (body == null) {
            return "";
        }
        String text = body.strip();
        if (text.isEmpty()) {
            return "";
        }
        if (text.length() > MAX_ERROR_BODY_CHARS) {
            text = text.substring(0, MAX_ERROR_BODY_CHARS);
        }
        return ": " + text;
    }

    private static String detail(Throwable failure) {
        return failure.getMessage() != null ? failure.getMessage() : failure.getClass().getSimpleName();
    }
}
