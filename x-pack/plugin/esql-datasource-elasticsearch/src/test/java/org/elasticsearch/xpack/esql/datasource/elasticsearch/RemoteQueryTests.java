/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

public class RemoteQueryTests extends ESTestCase {

    public void testBodyIsValidJson() throws IOException {
        assertRoundTrip("FROM logs | LIMIT 0");
    }

    public void testBodyEscapesQuotesBackslashesAndControlChars() throws IOException {
        // A literal pushed into the remote query can contain characters that must be JSON-escaped.
        assertRoundTrip("FROM logs | WHERE name == \"a\\\"b\" AND path == \"c\\\\d\"");
        assertRoundTrip("FROM logs | WHERE msg == \"line1\nline2\tend\"");
    }

    private static void assertRoundTrip(String esqlQuery) throws IOException {
        String body = RemoteQuery.body(esqlQuery);
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, body)) {
            Map<String, Object> parsed = parser.map();
            assertEquals(esqlQuery, parsed.get("query"));
            assertEquals(Boolean.TRUE, parsed.get("columnar"));
        }
    }
}
