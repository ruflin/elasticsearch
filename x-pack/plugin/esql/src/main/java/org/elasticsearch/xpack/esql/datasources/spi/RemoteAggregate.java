/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * A single aggregate output the optimizer asked a connector to compute remotely, e.g.
 * {@code c = COUNT(*)} or {@code mx = MAX(price)}. This is the SPI-level projection of an ESQL aggregate: it
 * carries only the output name, the aggregate function, and the optional input field a connector needs to
 * render a remote {@code STATS}, so the {@code datasources.spi} leaf module and connectors do not depend on the
 * full ESQL expression tree.
 *
 * <p>Never serialized: connectors execute on the coordinator only, so the aggregate is produced and consumed in
 * the same JVM.
 *
 * @param outputName the name of the aggregate's output column (the {@code c} in {@code STATS c = COUNT(*)})
 * @param function the aggregate function name, upper-cased (e.g. {@code COUNT}, {@code MIN}, {@code MAX},
 *        {@code SUM}, {@code AVG})
 * @param field the input field name (the connector quotes it as an identifier), or {@code null} for argument-less
 *        aggregates such as {@code COUNT(*)}
 * @param fieldFunction an ES|QL scalar function name wrapping {@code field} (the {@code TO_DOUBLE} in
 *        {@code STATS s = SUM(TO_DOUBLE(event.duration))}), or {@code null} when the input is the plain {@code field}.
 *        Only meaningful together with a non-null {@code field}: the connector renders
 *        {@code FUNC(fieldFunction(<quoted field>))}, keeping identifier quoting in the connector. Used for the
 *        {@code ToDouble} that {@code AVG}-over-long's surrogate introduces around its {@code SUM} input.
 * @param filter the already-rendered remote ES|QL boolean fragment of a per-aggregate filter (the
 *        {@code level == "error"} in {@code STATS c = COUNT(*) WHERE level == "error"}), or {@code null} for an
 *        unfiltered aggregate. The connector renders {@code out = FUNC(field) WHERE <filter>}.
 * @param intermediateState the intermediate aggregator-state layout the connector must emit when producing partial
 *        state (see {@link RemoteAggregateState}), or {@code null} to fall back to the legacy two-channel
 *        {@code [value, seen]} layout shared by COUNT / MIN / MAX. SUM (whose layout is type-dependent and may carry
 *        a Kahan {@code delta} or overflow {@code failed} channel) requires an explicit recipe.
 */
public record RemoteAggregate(
    String outputName,
    String function,
    String field,
    String fieldFunction,
    String filter,
    RemoteAggregateState intermediateState
) {

    public RemoteAggregate {
        if (outputName == null || outputName.isEmpty()) {
            throw new IllegalArgumentException("RemoteAggregate outputName must not be null or empty");
        }
        if (function == null || function.isEmpty()) {
            throw new IllegalArgumentException("RemoteAggregate function must not be null or empty");
        }
        if (fieldFunction != null) {
            if (fieldFunction.isBlank()) {
                throw new IllegalArgumentException("RemoteAggregate fieldFunction must not be blank when present");
            }
            if (field == null) {
                throw new IllegalArgumentException("RemoteAggregate fieldFunction requires a non-null field to wrap");
            }
        }
        if (filter != null && filter.isBlank()) {
            throw new IllegalArgumentException("RemoteAggregate filter must not be blank when present");
        }
    }

    /** A remote aggregate over a plain field (or {@code COUNT(*)} when {@code field} is {@code null}). */
    public RemoteAggregate(String outputName, String function, String field, String filter, RemoteAggregateState intermediateState) {
        this(outputName, function, field, null, filter, intermediateState);
    }

    /**
     * A remote aggregate over a plain field (or {@code COUNT(*)} when {@code field} is {@code null}) with no filter
     * and no explicit intermediate-state recipe. Convenience for query-rendering call sites that do not exercise
     * partial-state decode (the recipe and filter do not affect the rendered remote {@code STATS}).
     */
    public static RemoteAggregate of(String outputName, String function, String field) {
        return new RemoteAggregate(outputName, function, field, null, null, null);
    }

    /**
     * A remote aggregate whose input is {@code fieldFunction(field)} — a scalar function wrapping a source column,
     * e.g. {@code SUM(TO_DOUBLE(event.duration))} from AVG-over-long's surrogate.
     */
    public static RemoteAggregate ofWrappedField(
        String outputName,
        String function,
        String fieldFunction,
        String field,
        String filter,
        RemoteAggregateState intermediateState
    ) {
        return new RemoteAggregate(outputName, function, field, fieldFunction, filter, intermediateState);
    }
}
