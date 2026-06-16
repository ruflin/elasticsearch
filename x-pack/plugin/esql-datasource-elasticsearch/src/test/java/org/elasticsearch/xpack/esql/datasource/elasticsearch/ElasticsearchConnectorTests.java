/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ElasticsearchConnectorTests extends ESTestCase {

    public void testRemoteClientErrorMapsToClientException() {
        // A remote 4xx (e.g. the remote rejected a pushed STATS ... BY a, b) must surface as a client (400) error.
        String body = "{\"error\":{\"type\":\"verification_exception\",\"reason\":\"Unknown column [field2]\"},\"status\":400}";
        RuntimeException e = ElasticsearchConnector.remoteError(
            400,
            "HTTP/1.1 400 Bad Request",
            body,
            "FROM logs | STATS c = COUNT(*) BY a, b",
            null
        );

        assertThat(e, instanceOf(ExternalClientException.class));
        assertThat(e.getMessage(), containsString("400 Bad Request"));
        // The remote's actual reason is preserved so the failure is debuggable, not an opaque "failed to run".
        assertThat(e.getMessage(), containsString("Unknown column [field2]"));
        // The rendered ES|QL that was rejected is included.
        assertThat(e.getMessage(), containsString("FROM logs | STATS c = COUNT(*) BY a, b"));
    }

    public void testRemoteServerErrorMapsToServerException() {
        // A remote 5xx must NOT be mislabeled as a client error: it maps to a server (500) external exception.
        RuntimeException e = ElasticsearchConnector.remoteError(
            503,
            "HTTP/1.1 503 Service Unavailable",
            "{\"error\":\"unavailable\"}",
            "FROM logs | STATS c = COUNT(*)",
            null
        );

        assertThat(e, instanceOf(ExternalServerException.class));
        assertThat(e.getMessage(), containsString("503 Service Unavailable"));
    }

    public void testTruncateErrorBodyCollapsesWhitespace() {
        assertEquals("a b c", ElasticsearchConnector.truncateErrorBody("  a\n  b\t c \n"));
    }

    public void testTruncateErrorBodyHandlesNullAndEmpty() {
        assertEquals("<empty response body>", ElasticsearchConnector.truncateErrorBody(null));
        assertEquals("<empty response body>", ElasticsearchConnector.truncateErrorBody("   \n\t "));
    }

    public void testRemoteErrorBodyTruncationBound() {
        // The body snippet the message carries should never exceed the connector's documented bound; this guards
        // against a misconfigured remote returning a huge HTML error page. The reader caps the bytes it reads, so
        // here we assert the bound itself is sane and applied by the caller via readNBytes(MAX_ERROR_BODY_CHARS).
        assertEquals(2048, ElasticsearchConnector.MAX_ERROR_BODY_CHARS);
    }
}
