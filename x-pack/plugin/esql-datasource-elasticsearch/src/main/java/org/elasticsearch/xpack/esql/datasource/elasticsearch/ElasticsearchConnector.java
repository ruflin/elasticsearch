/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

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
            response = client.performRequest(RemoteQuery.request(esqlQuery));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to run remote ES|QL query [" + esqlQuery + "]", e);
        }
        return parseResponse(response, request.blockFactory());
    }

    /**
     * Builds the ES|QL string sent to the remote cluster. Issues a {@code FROM <target>} and, where
     * possible, pushes work to the remote cluster so less data crosses the wire:
     * <ul>
     *   <li>{@code WHERE} for the pushed filter expressions (best-effort; see {@link EsqlFilterTranslator}),</li>
     *   <li>{@code KEEP} for the projected columns,</li>
     *   <li>{@code LIMIT} for the pushed row limit (if any).</li>
     * </ul>
     * The local plan keeps a safety-net {@code FilterExec}/{@code LimitExec} above the source, so an
     * over- or under-rendered pushdown can never produce wrong results — it only affects how much data
     * is transferred.
     */
    static String buildRemoteQuery(QueryRequest request) {
        StringBuilder query = new StringBuilder("FROM ").append(EsqlIdentifiers.validateTarget(request.target()));
        // Filter remotely so the remote cluster discards non-matching rows before returning them.
        EsqlFilterTranslator.toWhereClause(request.pushedFilters()).ifPresent(where -> query.append(" | WHERE ").append(where));
        // Project only the columns the local query needs so the remote cluster returns less data.
        List<String> projected = request.projectedColumns();
        if (projected != null && projected.isEmpty() == false) {
            query.append(" | KEEP ");
            for (int i = 0; i < projected.size(); i++) {
                if (i > 0) {
                    query.append(", ");
                }
                // Quote like WHERE fields so dotted / special / reserved column names stay valid remote ES|QL.
                query.append(EsqlIdentifiers.quote(projected.get(i)));
            }
        }
        // Push the row limit so the remote cluster stops early instead of returning every matching row.
        int rowLimit = request.rowLimit();
        if (rowLimit != FormatReader.NO_LIMIT && rowLimit >= 0) {
            query.append(" | LIMIT ").append(rowLimit);
        }
        return query.toString();
    }

    @SuppressWarnings("unchecked")
    private ResultCursor parseResponse(Response response, BlockFactory blockFactory) {
        Map<String, Object> body;
        try (
            InputStream content = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
        ) {
            body = parser.map();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse remote ES|QL response", e);
        }

        List<EsqlTypeMapping.RemoteColumn> columns = new ArrayList<>();
        for (Object columnObj : (List<Object>) body.getOrDefault("columns", List.of())) {
            Map<String, Object> column = (Map<String, Object>) columnObj;
            columns.add(
                new EsqlTypeMapping.RemoteColumn(Objects.toString(column.get("name"), null), Objects.toString(column.get("type"), null))
            );
        }

        // columnar=true => values is an array of columns, each an array of row values.
        List<Object> columnValues = (List<Object>) body.getOrDefault("values", List.of());
        int rowCount = 0;
        for (Object columnObj : columnValues) {
            rowCount = Math.max(rowCount, ((List<Object>) columnObj).size());
        }

        List<Attribute> attributes = EsqlTypeMapping.toAttributes(columns);
        Block[] blocks = new Block[attributes.size()];
        boolean success = false;
        try {
            for (int col = 0; col < attributes.size(); col++) {
                DataType type = attributes.get(col).dataType();
                List<Object> values = col < columnValues.size() ? (List<Object>) columnValues.get(col) : List.of();
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
