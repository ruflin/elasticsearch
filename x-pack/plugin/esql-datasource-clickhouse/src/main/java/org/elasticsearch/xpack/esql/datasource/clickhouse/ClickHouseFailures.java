/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

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
 * {@link ExternalServerException} (500). The structured constructors take a {@link Condition}, so
 * the ClickHouse context travels in the detail code and the error text is attached via
 * {@link ExternalException#setDetail}.
 */
final class ClickHouseFailures {

    /** Cap on the error-response body snippet folded into a failure message, in chars. */
    private static final int MAX_ERROR_BODY_CHARS = 512;

    private ClickHouseFailures() {}

    /**
     * Maps a non-200 ClickHouse HTTP status. Retryable codes (429 / 5xx that a retry can clear)
     * become 503; authentication and missing-object statuses get their specific condition; everything
     * else is a client-class 400 (bad query or table).
     */
    static RuntimeException httpStatus(int statusCode, String body, String operation) {
        String detailCode = "ClickHouse " + operation + " failed (HTTP " + statusCode + ")";
        if (ExternalUnavailableException.isRetryableStatus(statusCode)) {
            boolean throttling = ExternalUnavailableException.isThrottlingStatus(statusCode);
            return withBody(
                new ExternalUnavailableException(
                    throttling ? Condition.STORE_THROTTLED : Condition.STORE_UNAVAILABLE,
                    StoragePath.NONE,
                    detailCode,
                    "",
                    throttling,
                    0L
                ),
                body
            );
        }
        Condition condition;
        if (statusCode == 401 || statusCode == 403) {
            condition = Condition.ACCESS_DENIED;
        } else if (statusCode == 404) {
            condition = Condition.OBJECT_NOT_FOUND;
        } else {
            condition = Condition.MALFORMED_DATA;
        }
        return withBody(new ExternalClientException(condition, StoragePath.NONE, detailCode, ""), body);
    }

    /** Connect / timeout / reset → 503; other I/O (malformed body, unexpected close) → 400. */
    static RuntimeException transport(IOException e, String operation) {
        String detailCode = "ClickHouse " + operation + " failed: " + detail(e);
        if (isTransient(e)) {
            return new ExternalUnavailableException(Condition.STORE_UNAVAILABLE, StoragePath.NONE, detailCode, "", false, 0L, e);
        }
        return new ExternalClientException(Condition.MALFORMED_DATA, StoragePath.NONE, detailCode, "", e);
    }

    static RuntimeException parse(IOException e) {
        return new ExternalClientException(
            Condition.MALFORMED_DATA,
            StoragePath.NONE,
            "ClickHouse response parse failed: " + detail(e),
            "",
            e
        );
    }

    static RuntimeException interrupted(InterruptedException e, String operation) {
        Thread.currentThread().interrupt();
        return new ExternalServerException(
            Condition.STORE_UNAVAILABLE,
            StoragePath.NONE,
            "Interrupted during ClickHouse " + operation,
            "",
            e
        );
    }

    private static boolean isTransient(IOException e) {
        return e instanceof ConnectException
            || e instanceof HttpTimeoutException
            || e instanceof UnknownHostException
            || e instanceof SocketException;
    }

    private static <E extends ExternalException> E withBody(E failure, String body) {
        String text = truncateBody(body);
        if (text.isEmpty() == false) {
            failure.setDetail(text);
        }
        return failure;
    }

    private static String truncateBody(String body) {
        if (body == null) {
            return "";
        }
        String text = body.strip();
        if (text.length() > MAX_ERROR_BODY_CHARS) {
            text = text.substring(0, MAX_ERROR_BODY_CHARS);
        }
        return text;
    }

    private static String detail(Throwable failure) {
        return failure.getMessage() != null ? failure.getMessage() : failure.getClass().getSimpleName();
    }
}
