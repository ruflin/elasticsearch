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
 * @param field the input field name, or {@code null} for argument-less aggregates such as {@code COUNT(*)}
 * @param filter the already-rendered remote ES|QL boolean fragment of a per-aggregate filter (the
 *        {@code level == "error"} in {@code STATS c = COUNT(*) WHERE level == "error"}), or {@code null} for an
 *        unfiltered aggregate. The connector renders {@code out = FUNC(field) WHERE <filter>}.
 */
public record RemoteAggregate(String outputName, String function, String field, String filter) {

    public RemoteAggregate {
        if (outputName == null || outputName.isEmpty()) {
            throw new IllegalArgumentException("RemoteAggregate outputName must not be null or empty");
        }
        if (function == null || function.isEmpty()) {
            throw new IllegalArgumentException("RemoteAggregate function must not be null or empty");
        }
        if (filter != null && filter.isBlank()) {
            throw new IllegalArgumentException("RemoteAggregate filter must not be blank when present");
        }
    }

    /** A remote aggregate without a per-aggregate filter. */
    public RemoteAggregate(String outputName, String function, String field) {
        this(outputName, function, field, null);
    }
}
