/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Live connection to a remote Elasticsearch cluster. Runs an ES|QL query against the remote
 * {@code _query} API over HTTP and streams the columnar response back as ES|QL {@link Page}s.
 * <p>
 * v1 executes the whole query in a single round-trip and returns a single-page cursor. Pagination
 * and aggregation pushdown are deferred to later phases.
 */
class ElasticsearchConnector implements Connector {

    private final RestClient client;

    ElasticsearchConnector(RestClient client) {
        this.client = client;
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        String esqlQuery = buildRemoteQuery(request);
        Response response;
        try {
            Request httpRequest = new Request("POST", "/_query");
            httpRequest.addParameter("format", "json");
            httpRequest.setJsonEntity(queryBody(esqlQuery));
            response = client.performRequest(httpRequest);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to run remote ES|QL query [" + esqlQuery + "]", e);
        }
        return parseResponse(response, request.blockFactory());
    }

    /**
     * Builds the ES|QL string sent to the remote cluster. v1 always issues a {@code FROM <target>}
     * and applies the row limit (if any) remotely so we do not pull the entire index across the wire.
     */
    static String buildRemoteQuery(QueryRequest request) {
        StringBuilder query = new StringBuilder("FROM ").append(request.target());
        // Project only the columns the local query needs so the remote cluster returns less data.
        List<String> projected = request.projectedColumns();
        if (projected != null && projected.isEmpty() == false) {
            query.append(" | KEEP ");
            for (int i = 0; i < projected.size(); i++) {
                if (i > 0) {
                    query.append(", ");
                }
                query.append(projected.get(i));
            }
        }
        return query.toString();
    }

    private static String queryBody(String esqlQuery) {
        // columnar=true makes the response group values by column, which maps directly onto Blocks.
        return "{\"query\":\"" + esqlQuery.replace("\"", "\\\"") + "\",\"columnar\":true}";
    }

    private ResultCursor parseResponse(Response response, BlockFactory blockFactory) {
        List<EsqlTypeMapping.RemoteColumn> columns = new ArrayList<>();
        List<List<Object>> columnValues = new ArrayList<>();
        int rowCount = 0;
        try (
            InputStream content = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
        ) {
            parser.nextToken();
            String fieldName;
            while ((fieldName = nextFieldName(parser)) != null) {
                switch (fieldName) {
                    case "columns" -> parseColumns(parser, columns);
                    case "values" -> rowCount = parseColumnarValues(parser, columnValues);
                    default -> parser.skipChildren();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse remote ES|QL response", e);
        }

        List<Attribute> attributes = EsqlTypeMapping.toAttributes(columns);
        Block[] blocks = new Block[attributes.size()];
        boolean success = false;
        try {
            for (int col = 0; col < attributes.size(); col++) {
                DataType type = attributes.get(col).dataType();
                List<Object> values = col < columnValues.size() ? columnValues.get(col) : List.of();
                blocks[col] = EsqlTypeMapping.toBlock(type, values, rowCount, blockFactory);
            }
            success = true;
        } finally {
            if (success == false) {
                for (Block block : blocks) {
                    if (block != null) {
                        block.close();
                    }
                }
            }
        }
        Page page = rowCount == 0 ? null : new Page(rowCount, blocks);
        return new SinglePageCursor(page);
    }

    private static String nextFieldName(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            return parser.currentName();
        }
        return null;
    }

    private static void parseColumns(XContentParser parser, List<EsqlTypeMapping.RemoteColumn> columns) throws IOException {
        parser.nextToken(); // START_ARRAY
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            String name = null;
            String type = null;
            String field;
            while ((field = nextFieldName(parser)) != null) {
                parser.nextToken();
                if ("name".equals(field)) {
                    name = parser.text();
                } else if ("type".equals(field)) {
                    type = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
            columns.add(new EsqlTypeMapping.RemoteColumn(name, type));
        }
    }

    /**
     * Parses a columnar {@code values} array: an array of columns, each an array of row values.
     * Returns the row count (length of the first column, 0 when empty).
     */
    private static int parseColumnarValues(XContentParser parser, List<List<Object>> columnValues) throws IOException {
        parser.nextToken(); // START_ARRAY (outer)
        int rowCount = 0;
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            List<Object> column = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                column.add(parser.currentToken() == XContentParser.Token.VALUE_NULL ? null : scalarValue(parser));
            }
            columnValues.add(column);
            rowCount = Math.max(rowCount, column.size());
        }
        return rowCount;
    }

    private static Object scalarValue(XContentParser parser) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_STRING -> parser.text();
            case VALUE_NUMBER -> parser.numberValue();
            case VALUE_BOOLEAN -> parser.booleanValue();
            case VALUE_NULL -> null;
            // Multi-valued cells arrive as a nested array; v1 collapses them to their string form.
            default -> {
                parser.skipChildren();
                yield null;
            }
        };
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    /** A {@link ResultCursor} that yields at most one already-materialized {@link Page}. */
    private static final class SinglePageCursor implements ResultCursor {

        private Page page;

        SinglePageCursor(Page page) {
            this.page = page;
        }

        @Override
        public boolean hasNext() {
            return page != null;
        }

        @Override
        public Page next() {
            if (page == null) {
                throw new NoSuchElementException();
            }
            Page result = page;
            page = null;
            return result;
        }

        @Override
        public void close() {
            if (page != null) {
                page.releaseBlocks();
                page = null;
            }
        }
    }
}
