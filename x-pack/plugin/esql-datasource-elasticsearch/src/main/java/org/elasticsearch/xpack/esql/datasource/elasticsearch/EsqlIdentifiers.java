/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

/**
 * Helpers for rendering identifiers (index targets and field names) into the remote ES|QL string
 * that this connector sends to the remote {@code _query} API.
 * <p>
 * Everything this connector interpolates into a remote ES|QL query (the {@code FROM} target, pushed
 * {@code WHERE} fields, and {@code KEEP} columns) flows through here so quoting is consistent and a
 * value can never break out of its clause into executable ES|QL text.
 */
final class EsqlIdentifiers {

    private EsqlIdentifiers() {}

    /**
     * Backtick-quotes an identifier so names containing dots, spaces, reserved words, or other
     * special characters remain a single valid ES|QL identifier. Embedded backticks are doubled.
     */
    static String quote(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    /**
     * Validates the {@code FROM} target before it is interpolated into a remote ES|QL query.
     * <p>
     * The target comes from the decoded URI path, so without validation a crafted location such as
     * {@code es://host:9200/x | DROP ...} could inject ES|QL. An index/data-stream/alias selector is
     * a comma-separated list of name patterns; we reject the structural ES|QL characters (quotes,
     * backticks, pipes) and control characters that would let a value escape the {@code FROM} clause.
     * <p>
     * Whitespace is rejected too. Elasticsearch index names cannot contain spaces, so no legitimate
     * target needs them, and allowing them would let a percent-encoded space in the URI path append a
     * clause to the rendered {@code FROM} — {@code es://host/logs%20METADATA%20_id} would decode to
     * {@code FROM logs METADATA _id} and silently change what the remote returns.
     */
    static String validateTarget(String target) {
        if (target == null || target.isEmpty()) {
            throw new IllegalArgumentException("Elasticsearch connector target must not be empty");
        }
        for (int i = 0; i < target.length(); i++) {
            char c = target.charAt(i);
            if (c == '"' || c == '`' || c == '|' || c == '\\' || c < ' ' || Character.isWhitespace(c)) {
                throw new IllegalArgumentException(
                    "Invalid Elasticsearch connector target [" + target + "]: illegal character at position " + i
                );
            }
        }
        return target;
    }
}
