/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * Describes a query to execute against a connector.
 * Immutable; use {@link #withBlockFactory} to create a copy bound to a specific driver context.
 *
 * @param pushedFilters AND-separated ESQL filter expressions the optimizer asked the connector to apply
 *        remotely (see {@link FilterPushdownSupport}). Empty when no filter was pushed. These are the
 *        original {@link Expression}s; connectors that natively understand ESQL (e.g. the elasticsearch
 *        connector) translate them back into a remote {@code WHERE} clause. Never serialized: connectors
 *        execute on the coordinator only, so the expressions are produced and consumed in the same JVM.
 * @param pushedSort sort keys the optimizer asked the connector to apply remotely, paired with
 *        {@link #rowLimit()}. Empty when no sort was pushed. Connectors that natively understand ESQL
 *        sorting (e.g. the elasticsearch connector) render these into a remote {@code SORT}. Never
 *        serialized, for the same coordinator-only reason as {@code pushedFilters}.
 * @param pushedAggregates aggregate outputs the optimizer asked the connector to compute remotely (with
 *        {@link #pushedGroupings()} as the group keys). Empty when no aggregate was pushed. When non-empty the
 *        connector renders a remote {@code STATS} and the result columns are the aggregate outputs (the
 *        {@link #projectedColumns()} are not applied). Never serialized, coordinator-only.
 * @param pushedGroupings group keys for {@link #pushedAggregates()} ({@code STATS ... BY <these>}). Empty for an
 *        ungrouped aggregate.
 * @param aggregateIntermediateState when {@code true} (and {@link #pushedAggregates()} is non-empty), the
 *        connector must emit the aggregate result in <em>intermediate</em> aggregator-state format rather than
 *        final values: for each pushed aggregate a typed value block immediately followed by a {@code seen}
 *        boolean block (all {@code true}), matching the {@code INITIAL} aggregate's intermediate attributes so a
 *        surviving {@code FINAL} aggregate can merge it. When {@code false} the connector emits one final value
 *        block per aggregate. Set by the planner from the aggregate's {@code AggregatorMode}: external-source
 *        STATS is planned as {@code FINAL(INITIAL(source))}, so the pushed source sits in the {@code INITIAL}
 *        position and must produce intermediate state. Never serialized, coordinator-only.
 */
public record QueryRequest(
    String target,
    List<String> projectedColumns,
    List<Attribute> attributes,
    Map<String, Object> config,
    int batchSize,
    int rowLimit,
    List<Expression> pushedFilters,
    List<RemoteSort> pushedSort,
    List<RemoteAggregate> pushedAggregates,
    List<RemoteGrouping> pushedGroupings,
    boolean aggregateIntermediateState,
    BlockFactory blockFactory
) {

    public QueryRequest {
        projectedColumns = projectedColumns != null ? List.copyOf(projectedColumns) : List.of();
        attributes = attributes != null ? List.copyOf(attributes) : List.of();
        config = config != null ? Map.copyOf(config) : Map.of();
        pushedFilters = pushedFilters != null ? List.copyOf(pushedFilters) : List.of();
        pushedSort = pushedSort != null ? List.copyOf(pushedSort) : List.of();
        pushedAggregates = pushedAggregates != null ? List.copyOf(pushedAggregates) : List.of();
        pushedGroupings = pushedGroupings != null ? List.copyOf(pushedGroupings) : List.of();
    }

    public QueryRequest(
        String target,
        List<String> projectedColumns,
        List<Attribute> attributes,
        Map<String, Object> config,
        int batchSize,
        BlockFactory blockFactory
    ) {
        this(
            target,
            projectedColumns,
            attributes,
            config,
            batchSize,
            FormatReader.NO_LIMIT,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            false,
            blockFactory
        );
    }

    public QueryRequest(
        String target,
        List<String> projectedColumns,
        List<Attribute> attributes,
        Map<String, Object> config,
        int batchSize,
        int rowLimit,
        BlockFactory blockFactory
    ) {
        this(
            target,
            projectedColumns,
            attributes,
            config,
            batchSize,
            rowLimit,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            false,
            blockFactory
        );
    }

    public QueryRequest withBlockFactory(BlockFactory blockFactory) {
        return new QueryRequest(
            target,
            projectedColumns,
            attributes,
            config,
            batchSize,
            rowLimit,
            pushedFilters,
            pushedSort,
            pushedAggregates,
            pushedGroupings,
            aggregateIntermediateState,
            blockFactory
        );
    }

    /**
     * Builds a connector {@link QueryRequest} from a {@link SourceOperatorContext}, forwarding the
     * pushdown fields ({@code pushedSort}, {@code pushedAggregates}, {@code pushedGroupings},
     * {@code aggregateIntermediateState}) that exist on both records. The {@code target} and
     * {@code projectedColumns} are derived by the caller from the context's config / split.
     */
    public static QueryRequest forConnector(String target, List<String> projectedColumns, SourceOperatorContext context) {
        return new QueryRequest(
            target,
            projectedColumns,
            context.attributes(),
            context.config(),
            context.batchSize(),
            context.rowLimit(),
            context.pushedExpressions(),
            context.pushedSort(),
            context.pushedAggregates(),
            context.pushedGroupings(),
            context.aggregateIntermediateState(),
            null
        );
    }
}
