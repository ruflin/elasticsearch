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
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteSort;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Live connection to a remote Elasticsearch cluster. Runs an ES|QL query against the remote
 * {@code _query} API over HTTP and materializes the columnar response into ES|QL {@link Page}s.
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
            throw new UncheckedIOException("Failed to run remote ES|QL query", e);
        }
        return parseResponse(response, request);
    }

    /**
     * Builds the ES|QL string sent to the remote cluster. Issues a {@code FROM <target>} and, where
     * possible, pushes work to the remote cluster so less data crosses the wire:
     * <ul>
     *   <li>{@code WHERE} for the pushed filter expressions (best-effort; see {@link EsqlFilterTranslator}),</li>
     *   <li>{@code SORT} for the pushed sort keys (paired with the limit, this makes the remote return the
     *       correct global top-N instead of an arbitrary page),</li>
     *   <li>{@code KEEP} for the projected columns,</li>
     *   <li>{@code LIMIT} for the pushed row limit (if any).</li>
     * </ul>
     * Stage order is {@code FROM | WHERE | SORT | KEEP | LIMIT}: SORT runs before KEEP so a sort key dropped
     * by the projection is still available when the remote sorts, and before LIMIT so the limit keeps the
     * top-N rather than the first page. The local plan keeps a safety-net {@code FilterExec}/{@code TopNExec}/
     * {@code LimitExec} above the source, so an over- or under-rendered pushdown can never produce wrong
     * results — it only affects how much data is transferred.
     */
    static String buildRemoteQuery(QueryRequest request) {
        StringBuilder query = new StringBuilder("FROM ").append(EsqlIdentifiers.validateTarget(request.target()));
        // Filter remotely so the remote cluster discards non-matching rows before returning them.
        EsqlFilterTranslator.toWhereClause(request.pushedFilters()).ifPresent(where -> query.append(" | WHERE ").append(where));
        // A pushed aggregate replaces row materialization entirely: render STATS and return aggregate output rows.
        // SORT/LIMIT may still apply to those aggregate rows (for STATS ... | SORT ... | LIMIT ...).
        List<RemoteAggregate> aggregates = request.pushedAggregates();
        if (aggregates != null && aggregates.isEmpty() == false) {
            appendStats(query, aggregates, request.pushedGroupings());
            appendSort(query, request.pushedSort());
            appendLimit(query, request.rowLimit());
            return query.toString();
        }
        // Sort remotely so a paired LIMIT returns the correct global top-N. Before KEEP so a sort key the
        // projection drops is still present when the remote sorts.
        appendSort(query, request.pushedSort());
        // Project only the columns the local query needs so the remote cluster returns less data.
        appendKeep(query, request.projectedColumns());
        // Push the row limit so the remote cluster stops early instead of returning every matching row.
        appendLimit(query, request.rowLimit());
        return query.toString();
    }

    private static void appendSort(StringBuilder query, List<RemoteSort> sort) {
        if (sort != null && sort.isEmpty() == false) {
            query.append(" | SORT ");
            for (int i = 0; i < sort.size(); i++) {
                if (i > 0) {
                    query.append(", ");
                }
                RemoteSort s = sort.get(i);
                query.append(EsqlIdentifiers.quote(s.field())).append(s.ascending() ? " ASC" : " DESC");
                query.append(s.nullsFirst() ? " NULLS FIRST" : " NULLS LAST");
            }
        }
    }

    private static void appendLimit(StringBuilder query, int rowLimit) {
        if (rowLimit != FormatReader.NO_LIMIT && rowLimit >= 0) {
            query.append(" | LIMIT ").append(rowLimit);
        }
    }

    private static void appendKeep(StringBuilder query, List<String> projected) {
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
    }

    /**
     * Renders {@code | STATS out = FN(field), ... [BY key, ...]} for a pushed aggregate. Output names, fields,
     * and group keys are backtick-quoted; argument-less aggregates (a null field, e.g. {@code COUNT(*)}) render
     * as {@code FN(*)}.
     */
    private static void appendStats(StringBuilder query, List<RemoteAggregate> aggregates, List<String> groupings) {
        query.append(" | STATS ");
        for (int i = 0; i < aggregates.size(); i++) {
            if (i > 0) {
                query.append(", ");
            }
            RemoteAggregate agg = aggregates.get(i);
            query.append(EsqlIdentifiers.quote(agg.outputName())).append(" = ").append(agg.function()).append('(');
            query.append(agg.field() == null ? "*" : EsqlIdentifiers.quote(agg.field()));
            query.append(')');
        }
        if (groupings != null && groupings.isEmpty() == false) {
            query.append(" BY ");
            for (int i = 0; i < groupings.size(); i++) {
                if (i > 0) {
                    query.append(", ");
                }
                query.append(EsqlIdentifiers.quote(groupings.get(i)));
            }
        }
    }

    private ResultCursor parseResponse(Response response, QueryRequest request) {
        BlockFactory blockFactory = request.blockFactory();
        List<EsqlTypeMapping.RemoteColumn> columns = new ArrayList<>();
        // columnar=true => values is an array of columns, each an array of row values.
        List<List<Object>> columnValues = new ArrayList<>();
        try (
            InputStream content = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
        ) {
            parser.nextToken(); // START_OBJECT
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String field = parser.currentName();
                parser.nextToken(); // move to field value
                if ("columns".equals(field)) {
                    EsqlTypeMapping.parseColumns(parser, columns);
                } else if ("values".equals(field)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        List<Object> colValues = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            colValues.add(readScalar(parser));
                        }
                        columnValues.add(colValues);
                    }
                } else {
                    parser.skipChildren();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse remote ES|QL response", e);
        }

        int rowCount = 0;
        for (List<Object> col : columnValues) {
            rowCount = Math.max(rowCount, col.size());
        }

        if (rowCount == 0) {
            return new SinglePageCursor(null);
        }

        boolean intermediateAggregateState = request.aggregateIntermediateState()
            && request.pushedAggregates() != null
            && request.pushedAggregates().isEmpty() == false;
        Block[] blocks = intermediateAggregateState
            ? buildIntermediateAggregateBlocks(columns, columnValues, rowCount, blockFactory, request)
            : buildPassthroughBlocks(columns, columnValues, rowCount, blockFactory);

        return new SinglePageCursor(new Page(rowCount, blocks));
    }

    /** Decodes each remote column to a block in response order (the result is the connector's output as-is). */
    private static Block[] buildPassthroughBlocks(
        List<EsqlTypeMapping.RemoteColumn> columns,
        List<List<Object>> columnValues,
        int rowCount,
        BlockFactory blockFactory
    ) {
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
                releaseAll(blocks);
            }
        }
        return blocks;
    }

    /**
     * Builds the intermediate aggregator-state layout the {@code INITIAL} position of a {@code FINAL(INITIAL(source))}
     * plan expects, so the surviving {@code FINAL} aggregate merges the connector's single partial. That layout is
     * {@code [group key(s)..., per-aggregate (value, seen)...]} — grouping keys first, then for each aggregate a
     * value block followed by an all-true {@code seen} boolean. The remote {@code STATS ... BY} response instead
     * returns {@code [aggregate(s)..., group key(s)...]}, so we look columns up by name and reorder. All supported
     * aggregates (COUNT, MIN, MAX) use the two-channel {@code [value, seen]} state, so a single {@code seen} per
     * aggregate is correct.
     */
    private static Block[] buildIntermediateAggregateBlocks(
        List<EsqlTypeMapping.RemoteColumn> columns,
        List<List<Object>> columnValues,
        int rowCount,
        BlockFactory blockFactory,
        QueryRequest request
    ) {
        Map<String, Integer> columnIndexByName = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnIndexByName.put(columns.get(i).name(), i);
        }
        List<String> groupings = request.pushedGroupings();
        List<RemoteAggregate> aggregates = request.pushedAggregates();
        Block[] blocks = new Block[groupings.size() + aggregates.size() * 2];
        boolean success = false;
        try {
            int out = 0;
            // Grouping keys pass through as-is, in the order the planner expects them.
            for (String grouping : groupings) {
                blocks[out++] = decodeColumnByName(grouping, columnIndexByName, columns, columnValues, rowCount, blockFactory);
            }
            // Each aggregate becomes a (value, seen=true) pair.
            for (RemoteAggregate aggregate : aggregates) {
                blocks[out++] = decodeColumnByName(
                    aggregate.outputName(),
                    columnIndexByName,
                    columns,
                    columnValues,
                    rowCount,
                    blockFactory
                );
                blocks[out++] = blockFactory.newConstantBooleanBlockWith(true, rowCount);
            }
            success = true;
        } finally {
            if (success == false) {
                releaseAll(blocks);
            }
        }
        return blocks;
    }

    private static Block decodeColumnByName(
        String name,
        Map<String, Integer> columnIndexByName,
        List<EsqlTypeMapping.RemoteColumn> columns,
        List<List<Object>> columnValues,
        int rowCount,
        BlockFactory blockFactory
    ) {
        Integer index = columnIndexByName.get(name);
        if (index == null) {
            throw new IllegalStateException("Remote ES|QL response is missing expected column [" + name + "]");
        }
        DataType type = DataType.fromNameOrAlias(columns.get(index).type());
        List<Object> values = index < columnValues.size() ? columnValues.get(index) : List.of();
        return EsqlTypeMapping.toBlock(type, values, rowCount, blockFactory);
    }

    /**
     * Reads a single scalar JSON value at the parser's current token. Nested objects/arrays are
     * skipped and reported as {@code null} — ES|QL columnar values are always scalars, so this
     * only happens on a malformed or future-version response.
     */
    private static Object readScalar(XContentParser parser) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING -> parser.text();
            case VALUE_NUMBER -> parser.numberValue();
            case VALUE_BOOLEAN -> parser.booleanValue();
            default -> {
                parser.skipChildren();
                yield null;
            }
        };
    }

    private static void releaseAll(Block[] blocks) {
        for (Block block : blocks) {
            if (block != null) {
                block.close();
            }
        }
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
