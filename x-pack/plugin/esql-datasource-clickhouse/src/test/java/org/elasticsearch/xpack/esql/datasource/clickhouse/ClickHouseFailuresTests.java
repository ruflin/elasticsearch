/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpTimeoutException;

public class ClickHouseFailuresTests extends ESTestCase {

    public void testClientHttpStatus() {
        RuntimeException e = ClickHouseFailures.httpStatus(404, "Unknown table", "query failed");
        assertTrue(e instanceof ExternalClientException);
        assertEquals(RestStatus.BAD_REQUEST, ((ExternalClientException) e).status());
        assertTrue(e.getMessage().contains("HTTP 404"));
        assertTrue(e.getMessage().contains("Unknown table"));
    }

    public void testRetryableHttpStatus() {
        RuntimeException e = ClickHouseFailures.httpStatus(503, "overloaded", "query failed");
        assertTrue(e instanceof ExternalUnavailableException);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((ExternalUnavailableException) e).status());
        assertTrue(((ExternalUnavailableException) e).throttling());
    }

    public void testTransientTransport() {
        RuntimeException e = ClickHouseFailures.transport(new ConnectException("Connection refused"), "query failed");
        assertTrue(e instanceof ExternalUnavailableException);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ((ExternalUnavailableException) e).status());
    }

    public void testTimeoutTransport() {
        RuntimeException e = ClickHouseFailures.transport(new HttpTimeoutException("timed out"), "failed to fetch schema");
        assertTrue(e instanceof ExternalUnavailableException);
    }

    public void testParseIsClientError() {
        RuntimeException e = ClickHouseFailures.parse(new IOException("Unexpected end-of-input"));
        assertTrue(e instanceof ExternalClientException);
        assertEquals(RestStatus.BAD_REQUEST, ((ExternalClientException) e).status());
    }

    public void testInterruptedRestoresFlag() {
        Thread.interrupted(); // clear
        RuntimeException e = ClickHouseFailures.interrupted(new InterruptedException("stop"), "executing ClickHouse query");
        assertTrue(e instanceof ExternalServerException);
        assertTrue(Thread.interrupted());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((ExternalServerException) e).status());
    }
}
