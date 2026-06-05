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
    BlockFactory blockFactory
) {

    public QueryRequest {
        pushedFilters = pushedFilters != null ? List.copyOf(pushedFilters) : List.of();
        pushedSort = pushedSort != null ? List.copyOf(pushedSort) : List.of();
    }

    public QueryRequest(
        String target,
        List<String> projectedColumns,
        List<Attribute> attributes,
        Map<String, Object> config,
        int batchSize,
        BlockFactory blockFactory
    ) {
        this(target, projectedColumns, attributes, config, batchSize, FormatReader.NO_LIMIT, List.of(), List.of(), blockFactory);
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
        this(target, projectedColumns, attributes, config, batchSize, rowLimit, List.of(), List.of(), blockFactory);
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
            blockFactory
        );
    }
}
