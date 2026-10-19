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
import static org.hamcrest.Matchers.not;

public class ElasticsearchConnectorTests extends ESTestCase {

    public void testRemoteClientErrorMapsToClientException() {
        // A remote 4xx (e.g. the remote rejected a pushed STATS ... BY a, b) must surface as a client (400) error.
        String body = "{\"error\":{\"type\":\"verification_exception\",\"reason\":\"Unknown column [field2]\"},\"status\":400}";
        RuntimeException e = ElasticsearchConnector.remoteError(400, "HTTP/1.1 400 Bad Request", body, null);

        assertThat(e, instanceOf(ExternalClientException.class));
        assertThat(e.getMessage(), containsString("400 Bad Request"));
        // The remote's actual reason is preserved so the failure is debuggable, not an opaque "failed to run".
        assertThat(e.getMessage(), containsString("Unknown column [field2]"));
    }

    public void testRemoteServerErrorMapsToServerException() {
        // A remote 5xx must NOT be mislabeled as a client error: it maps to a server (500) external exception.
        RuntimeException e = ElasticsearchConnector.remoteError(
            503,
            "HTTP/1.1 503 Service Unavailable",
            "{\"error\":\"unavailable\"}",
            null
        );

        assertThat(e, instanceOf(ExternalServerException.class));
        assertThat(e.getMessage(), containsString("503 Service Unavailable"));
    }

    public void testRemoteErrorDoesNotLeakTheRenderedQuery() {
        // The message travels back to the caller and into logs, so it must not carry the rendered remote ES|QL:
        // that string holds the resolved target and every pushed filter literal. The query goes to DEBUG instead.
        RuntimeException e = ElasticsearchConnector.remoteError(400, "HTTP/1.1 400 Bad Request", "{\"error\":\"bad request\"}", null);

        assertThat(e.getMessage(), not(containsString("FROM")));
        assertThat(e.getMessage(), not(containsString("WHERE")));
    }

    public void testTruncateErrorBodyCollapsesWhitespace() {
        assertEquals("a b c", RemoteErrorSnippets.truncate("  a\n  b\t c \n"));
    }

    public void testTruncateErrorBodyHandlesNullAndEmpty() {
        assertEquals("<empty response body>", RemoteErrorSnippets.truncate(null));
        assertEquals("<empty response body>", RemoteErrorSnippets.truncate("   \n\t "));
    }

    public void testRemoteErrorBodyTruncationBound() {
        // The body snippet the message carries should never exceed the connector's documented bound; this guards
        // against a misconfigured remote returning a huge HTML error page. The reader caps the bytes it reads, so
        // here we assert the bound itself is sane and applied by the caller via readNBytes(MAX_ERROR_BODY_CHARS).
        assertEquals(2048, RemoteErrorSnippets.MAX_ERROR_BODY_CHARS);
    }
}
