/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.client.Request;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Builds the HTTP {@code POST /_query} request this connector sends to the remote cluster.
 * <p>
 * The body is rendered with an {@link XContentBuilder} so the ES|QL string (which can contain
 * quotes, backslashes, and control characters from pushed literals) is always escaped to valid
 * JSON. {@code columnar=true} groups response values by column, which maps directly onto ES|QL
 * {@code Block}s.
 */
final class RemoteQuery {

    private RemoteQuery() {}

    static Request request(String esqlQuery) {
        Request request = new Request("POST", "/_query");
        request.addParameter("format", "json");
        request.setJsonEntity(body(esqlQuery));
        return request;
    }

    static String body(String esqlQuery) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject().field("query", esqlQuery).field("columnar", true).endObject();
            return org.elasticsearch.common.Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build remote ES|QL request body", e);
        }
    }
}
