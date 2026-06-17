/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregateState;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteGrouping;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteSort;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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

    /**
     * Upper bound on how many characters of the remote error body are included in a surfaced error
     * message. Remote ES|QL errors (a {@code root_cause}/{@code reason} JSON object) are small, but a
     * misconfigured remote could return an unbounded HTML error page or stack trace; truncating keeps
     * the message useful for debugging without flooding logs or the API response.
     */
    static final int MAX_ERROR_BODY_CHARS = 2048;

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        String esqlQuery = buildRemoteQuery(request);
        Response response;
        try {
            response = client.performRequest(RemoteQuery.request(esqlQuery));
        } catch (ResponseException e) {
            // The remote cluster answered with a non-2xx status: the query was rejected (e.g. an unsupported
            // pushed STATS/grouping, an unmapped or non-groupable field, a permissions problem). Surface the
            // remote status and a bounded snippet of its error body so the failure is actionable instead of an
            // opaque "failed to run" string, and map the status class through so a remote 5xx is not mislabeled
            // as a client (400) error.
            throw remoteErrorException(esqlQuery, e);
        } catch (IOException e) {
            // A transport-level failure (connection refused, timeout, TLS, DNS): no remote status to forward.
            throw new UncheckedIOException("Failed to run remote ES|QL query [" + esqlQuery + "]", e);
        }
        return parseResponse(response, request);
    }

    /**
     * Builds the exception for a remote non-2xx response by reading the remote status and error body off the
     * {@link ResponseException} and delegating to {@link #remoteError}. Kept thin so the message/status-mapping
     * logic in {@link #remoteError} stays unit-testable without constructing a (package-private) {@link Response}.
     */
    private static RuntimeException remoteErrorException(String esqlQuery, ResponseException e) {
        int statusCode = e.getResponse().getStatusLine().getStatusCode();
        String statusLine = e.getResponse().getStatusLine().toString();
        return remoteError(statusCode, statusLine, errorBodySnippet(e.getResponse()), esqlQuery, e);
    }

    /**
     * Maps a remote non-2xx response to the matching external-source exception: a remote {@code 5xx} becomes an
     * {@link ExternalServerException} (500) and anything else (a {@code 4xx} client error such as a bad pushed
     * query or a permissions failure) becomes an {@link ExternalClientException} (400). The message carries the
     * remote status line and a snippet of the remote error body plus the rendered ES|QL that was rejected, so the
     * failure is actionable instead of an opaque "failed to run" string.
     */
    static RuntimeException remoteError(int statusCode, String statusLine, String bodySnippet, String esqlQuery, Throwable cause) {
        String message = "Remote ES|QL query failed with [" + statusLine + "]: " + bodySnippet + " (query [" + esqlQuery + "])";
        return statusCode >= 500 ? new ExternalServerException(message, cause) : new ExternalClientException(message, cause);
    }

    /**
     * Reads the remote error response body as a bounded, single-line snippet for inclusion in an error message.
     * Returns a placeholder when the body is absent or cannot be read, so error reporting never throws while
     * reporting an error.
     */
    private static String errorBodySnippet(Response response) {
        if (response.getEntity() == null) {
            return "<no response body>";
        }
        try (InputStream content = response.getEntity().getContent()) {
            return truncateErrorBody(new String(content.readNBytes(MAX_ERROR_BODY_CHARS), StandardCharsets.UTF_8));
        } catch (IOException ioe) {
            return "<unreadable response body: " + ioe.getMessage() + ">";
        }
    }

    /** Collapses whitespace and trims a remote error body into a compact single-line snippet for an error message. */
    static String truncateErrorBody(String body) {
        String text = body == null ? "" : body.strip().replaceAll("\\s+", " ");
        return text.isEmpty() ? "<empty response body>" : text;
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
        // Request metadata columns (e.g. _id, _source) via the FROM ... METADATA option. Without this option the
        // remote response carries no metadata columns, so a KEEP/projection referencing _id or _source would read
        // nothing. The Kibana KI `match` rule path reads back _id and _source, so this unblocks that use case.
        appendMetadata(query, request);
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

    /**
     * Appends a {@code METADATA <name>, ...} option to the {@code FROM} clause for every projected column that is a
     * supported ES|QL metadata attribute (e.g. {@code _id}, {@code _source}, {@code _index}). The names are taken
     * from the requested columns and recognized via {@link MetadataAttribute#isSupported(String)}, so only genuine
     * metadata fields trigger the option; ordinary fields and {@code @timestamp} (which is not a metadata attribute)
     * are left to the normal {@code FROM}/projection path. Metadata names are emitted unquoted because the ES|QL
     * {@code METADATA} option does not accept backtick-quoted identifiers and metadata names are fixed, safe tokens.
     */
    private static void appendMetadata(StringBuilder query, QueryRequest request) {
        List<String> projected = request.projectedColumns();
        if (projected == null || projected.isEmpty()) {
            return;
        }
        List<String> metadataColumns = new ArrayList<>();
        for (String column : projected) {
            if (MetadataAttribute.isSupported(column)) {
                metadataColumns.add(column);
            }
        }
        if (metadataColumns.isEmpty()) {
            return;
        }
        query.append(" METADATA ");
        for (int i = 0; i < metadataColumns.size(); i++) {
            if (i > 0) {
                query.append(", ");
            }
            query.append(metadataColumns.get(i));
        }
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
    private static void appendStats(StringBuilder query, List<RemoteAggregate> aggregates, List<RemoteGrouping> groupings) {
        query.append(" | STATS ");
        for (int i = 0; i < aggregates.size(); i++) {
            if (i > 0) {
                query.append(", ");
            }
            RemoteAggregate agg = aggregates.get(i);
            query.append(EsqlIdentifiers.quote(agg.outputName())).append(" = ").append(agg.function()).append('(');
            if (agg.field() == null) {
                query.append('*');
            } else if (agg.fieldFunction() != null) {
                // Input wrapped in a scalar function (e.g. SUM(TO_DOUBLE(`event.duration`))). The connector owns
                // identifier quoting, so only the field is quoted; the function name is rendered verbatim.
                query.append(agg.fieldFunction()).append('(').append(EsqlIdentifiers.quote(agg.field())).append(')');
            } else {
                query.append(EsqlIdentifiers.quote(agg.field()));
            }
            query.append(')');
            if (agg.filter() != null) {
                // Per-aggregate filter: COUNT(*) WHERE <already-rendered remote boolean fragment>.
                query.append(" WHERE ").append(agg.filter());
            }
        }
        if (groupings != null && groupings.isEmpty() == false) {
            query.append(" BY ");
            for (int i = 0; i < groupings.size(); i++) {
                if (i > 0) {
                    query.append(", ");
                }
                RemoteGrouping grouping = groupings.get(i);
                if (grouping.isPlainField()) {
                    // Plain field: render the quoted field reference (e.g. BY `service.name`).
                    query.append(EsqlIdentifiers.quote(grouping.outputName()));
                } else {
                    // Computed grouping (e.g. a time BUCKET): render `out` = <already-rendered remote expression>.
                    query.append(EsqlIdentifiers.quote(grouping.outputName())).append(" = ").append(grouping.expression());
                }
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
                            colValues.add(readValue(parser));
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
     * returns {@code [aggregate(s)..., group key(s)...]}, so we look columns up by name and reorder.
     *
     * <p>Each aggregate's intermediate layout is driven by its {@link RemoteAggregate#intermediateState()} recipe:
     * the primary {@code VALUE} channel is filled from the decoded remote column, the {@code SEEN} channel from the
     * value's nullness (so a null per-group MIN/MAX/SUM is skipped by the FINAL merge), and any auxiliary channel
     * (SUM-double's Kahan {@code delta}, SUM-long's overflow {@code failed}) with its identity value. COUNT / MIN /
     * MAX carry no recipe and fall back to the two-channel {@code [value, seen]} layout.
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
        List<RemoteGrouping> groupings = request.pushedGroupings();
        List<RemoteAggregate> aggregates = request.pushedAggregates();
        int aggregateBlocks = 0;
        for (RemoteAggregate aggregate : aggregates) {
            aggregateBlocks += intermediateState(aggregate).channels().size();
        }
        Block[] blocks = new Block[groupings.size() + aggregateBlocks];
        boolean success = false;
        try {
            int out = 0;
            // Grouping keys pass through as-is, in the order the planner expects them. A computed grouping (e.g. a
            // time BUCKET) is returned by the remote under its output-name alias, so look it up by output name.
            for (RemoteGrouping grouping : groupings) {
                blocks[out++] = decodeColumnByName(grouping.outputName(), columnIndexByName, columns, columnValues, rowCount, blockFactory);
            }
            for (RemoteAggregate aggregate : aggregates) {
                out = appendIntermediateChannels(blocks, out, aggregate, columnIndexByName, columns, columnValues, rowCount, blockFactory);
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
     * Appends the intermediate-state blocks for one aggregate, returning the next output index. Every channel is
     * driven by the aggregate's {@link RemoteAggregateState} recipe: the primary {@code VALUE} channel is decoded
     * from the remote column, the {@code SEEN} channel is derived from value nullness (so a null per-group result is
     * skipped by the FINAL merge), and any auxiliary {@code NEUTRAL} channel is filled with its identity value.
     */
    private static int appendIntermediateChannels(
        Block[] blocks,
        int out,
        RemoteAggregate aggregate,
        Map<String, Integer> columnIndexByName,
        List<EsqlTypeMapping.RemoteColumn> columns,
        List<List<Object>> columnValues,
        int rowCount,
        BlockFactory blockFactory
    ) {
        Integer index = columnIndexByName.get(aggregate.outputName());
        if (index == null) {
            throw new IllegalStateException("Remote ES|QL response is missing expected column [" + aggregate.outputName() + "]");
        }
        // The aggregate's own column drives both its VALUE block (decoded by type) and its SEEN marker (nullness).
        List<Object> values = index < columnValues.size() ? columnValues.get(index) : List.of();
        for (RemoteAggregateState.Channel channel : intermediateState(aggregate).channels()) {
            blocks[out++] = switch (channel.role()) {
                case VALUE -> decodeColumnByName(aggregate.outputName(), columnIndexByName, columns, columnValues, rowCount, blockFactory);
                case SEEN -> buildSeenBlock(values, rowCount, blockFactory);
                case NEUTRAL -> buildNeutralBlock(channel.type(), rowCount, blockFactory);
            };
        }
        return out;
    }

    /**
     * The intermediate-state recipe for a pushed aggregate. The optimizer always attaches one (see
     * {@code PushConnectorStatsToExternalSource}); a missing recipe means a connector was asked to emit partial
     * state without the layout the FINAL merge expects, which is a planning bug rather than a runtime condition.
     */
    private static RemoteAggregateState intermediateState(RemoteAggregate aggregate) {
        RemoteAggregateState state = aggregate.intermediateState();
        if (state == null) {
            throw new IllegalStateException("Pushed aggregate [" + aggregate.outputName() + "] is missing its intermediate-state recipe");
        }
        return state;
    }

    /**
     * Builds an auxiliary intermediate-state channel filled with its identity value: {@code 0} for numeric channels
     * (e.g. SUM-double's Kahan {@code delta}) and {@code false} for boolean channels (e.g. SUM-long's overflow
     * {@code failed}). For a single already-computed partial these identities leave the merged result unchanged.
     */
    private static Block buildNeutralBlock(ElementType type, int rowCount, BlockFactory blockFactory) {
        return switch (type) {
            case DOUBLE -> blockFactory.newConstantDoubleBlockWith(0.0, rowCount);
            case LONG -> blockFactory.newConstantLongBlockWith(0L, rowCount);
            case INT -> blockFactory.newConstantIntBlockWith(0, rowCount);
            case BOOLEAN -> blockFactory.newConstantBooleanBlockWith(false, rowCount);
            default -> throw new IllegalStateException("Unsupported neutral intermediate channel type [" + type + "]");
        };
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
     * Builds the {@code seen} marker block for an aggregate's intermediate state: {@code seen[i] = value[i] != null}.
     * The FINAL aggregator skips rows whose marker is false, so a per-row marker keeps a null group result (e.g. a
     * group whose MIN/MAX inputs are all null) from being merged as a real value. A missing value list defaults to
     * all-false, matching an empty remote result.
     */
    private static Block buildSeenBlock(List<Object> values, int rowCount, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                builder.appendBoolean(i < values.size() && values.get(i) != null);
            }
            return builder.build();
        }
    }

    /**
     * Reads a single value at the parser's current token. Scalars are returned as their decoded Java value;
     * a nested object or array (e.g. a {@code _source} column value) is captured as its compact JSON text so a
     * {@code SOURCE}/{@code OBJECT} column can be decoded into a {@link BytesRef} block (see
     * {@link EsqlTypeMapping#toBlock}). Most ES|QL columnar values are scalars, so the object/array branch is
     * only hit for structural columns such as {@code _source}.
     */
    private static Object readValue(XContentParser parser) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING -> parser.text();
            case VALUE_NUMBER -> parser.numberValue();
            case VALUE_BOOLEAN -> parser.booleanValue();
            case START_OBJECT, START_ARRAY -> copyStructuredValueAsJson(parser);
            default -> {
                parser.skipChildren();
                yield null;
            }
        };
    }

    /**
     * Copies the object/array sub-tree at the parser's current token into a compact JSON string. Used to capture a
     * structural column value (e.g. {@code _source}) that the remote {@code _query} response renders as a nested
     * JSON object rather than a scalar, so it can be surfaced as a {@code SOURCE}/{@code OBJECT} {@link BytesRef}
     * column instead of being dropped.
     */
    private static String copyStructuredValueAsJson(XContentParser parser) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.copyCurrentStructure(parser);
            return Strings.toString(builder);
        }
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
