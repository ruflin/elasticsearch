/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;

import java.util.Map;

/**
 * Container for external-source planning state attached to {@link LocalPhysicalOptimizerContext}.
 * <p>
 * Carries the {@link FormatReaderRegistry} (consulted to discover what a file-based source's
 * underlying reader supports: filter pushdown, aggregate pushdown, deferred column extraction) and
 * the registered {@link ExternalSourceFactory} instances (consulted to discover what a connector-based
 * source supports, e.g. {@link ExternalSourceFactory#filterPushdownSupport()}). Encapsulating both here
 * keeps the parent context's signature stable as new external-source-only fields appear: future
 * additions land on this record, never on {@link LocalPhysicalOptimizerContext}.
 * <p>
 * Instances are constructed once per local-plan invocation by
 * {@code PlannerUtils.localPlan(... FormatReaderRegistry ...)}; rules read through
 * {@link LocalPhysicalOptimizerContext#external()}. Use {@link #NONE} for callers (e.g.
 * coordinator-side optimization, lookup-service planning, tests) that have no external sources
 * in scope.
 */
public record ExternalOptimizerContext(
    FormatReaderRegistry formatReaderRegistry,
    Map<String, ExternalSourceFactory> sourceFactories
) {

    public ExternalOptimizerContext {
        sourceFactories = sourceFactories != null ? Map.copyOf(sourceFactories) : Map.of();
    }

    /**
     * Convenience constructor for callers (mostly tests) that only have a {@link FormatReaderRegistry}
     * in scope. Connector filter pushdown is disabled when constructed this way.
     */
    public ExternalOptimizerContext(FormatReaderRegistry formatReaderRegistry) {
        this(formatReaderRegistry, Map.of());
    }

    /**
     * Sentinel for callers without any external-source state. Rules that consult external
     * capabilities must treat {@code formatReaderRegistry == null} as "no information" and bail
     * out of the optimization, mirroring the previous behavior when the registry field was
     * unset on the parent context.
     */
    public static final ExternalOptimizerContext NONE = new ExternalOptimizerContext(null, Map.of());
}
