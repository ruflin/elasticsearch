/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.client.Response;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Shared helpers for reading a bounded, single-line snippet from a remote error response body.
 * Used by both schema-resolution and query-execution error reporting so the truncation limit and
 * whitespace collapsing stay consistent and cannot drift between the two call sites.
 */
final class RemoteErrorSnippets {

    private RemoteErrorSnippets() {}

    /**
     * Upper bound on how many characters of the remote error body are included in a surfaced error
     * message. Remote ES|QL errors (a {@code root_cause}/{@code reason} JSON object) are small, but a
     * misconfigured remote could return an unbounded HTML error page or stack trace; truncating keeps
     * the message useful for debugging without flooding logs or the API response.
     */
    static final int MAX_ERROR_BODY_CHARS = 2048;

    /**
     * Reads the remote error response body as a bounded, single-line snippet for inclusion in an error message.
     * Returns a placeholder when the body is absent or cannot be read, so error reporting never throws while
     * reporting an error.
     */
    static String snippet(Response response) {
        if (response.getEntity() == null) {
            return "<no response body>";
        }
        try (InputStream content = response.getEntity().getContent()) {
            return truncate(new String(content.readNBytes(MAX_ERROR_BODY_CHARS), StandardCharsets.UTF_8));
        } catch (IOException ioe) {
            return "<unreadable response body: " + ioe.getMessage() + ">";
        }
    }

    /** Collapses whitespace and trims a remote error body into a compact single-line snippet for an error message. */
    static String truncate(String body) {
        String text = body == null ? "" : body.strip().replaceAll("\\s+", " ");
        return text.isEmpty() ? "<empty response body>" : text;
    }
}
