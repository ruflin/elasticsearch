/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * A single group key the optimizer asked a connector to compute remotely ({@code STATS ... BY <this>}). This is the
 * SPI-level projection of an ESQL grouping: it carries the output column name plus, for computed groupings, a
 * pre-rendered remote ES|QL fragment, so the {@code datasources.spi} leaf module and connectors do not depend on the
 * full ESQL expression tree.
 *
 * <p>Two shapes are represented:
 * <ul>
 *   <li>a plain field grouping ({@code STATS ... BY service.name}) — built with {@link #ofField(String)}, where
 *       {@link #outputName()} is the field name and {@link #expression()} is {@code null}; the connector renders the
 *       (quoted) field reference; and</li>
 *   <li>a computed grouping such as a time bucket ({@code STATS ... BY bucket = BUCKET(@timestamp, 1 hour)}) — built
 *       with {@link #ofExpression(String, String)}, where {@link #outputName()} is the result column alias and
 *       {@link #expression()} is the already-rendered grouping function (using remote-quoted field references).</li>
 * </ul>
 * The connector renders {@code `outputName` = expression} for a computed grouping and just the quoted field for a
 * plain field, and looks results up by {@link #outputName()} when decoding the remote response.
 *
 * <p>Never serialized: connectors execute on the coordinator only, so the grouping is produced and consumed in the
 * same JVM.
 *
 * @param outputName the name of the grouping's output column (the {@code bucket} in {@code BY bucket = BUCKET(...)},
 *        or the field name for a plain field grouping)
 * @param expression the already-rendered remote ES|QL fragment that produces a computed group key, or {@code null}
 *        for a plain field grouping (whose reference the connector renders from {@link #outputName()})
 */
public record RemoteGrouping(String outputName, String expression) {

    public RemoteGrouping {
        if (outputName == null || outputName.isEmpty()) {
            throw new IllegalArgumentException("RemoteGrouping outputName must not be null or empty");
        }
        if (expression != null && expression.isEmpty()) {
            throw new IllegalArgumentException("RemoteGrouping expression must not be empty when present");
        }
    }

    /** A plain field grouping ({@code STATS ... BY <field>}); the connector renders the quoted field reference. */
    public static RemoteGrouping ofField(String field) {
        return new RemoteGrouping(field, null);
    }

    /**
     * A computed grouping ({@code STATS ... BY <outputName> = <expression>}); {@code expression} is the already-rendered
     * remote ES|QL (e.g. {@code BUCKET(`@timestamp`, 1 hour)}).
     */
    public static RemoteGrouping ofExpression(String outputName, String expression) {
        return new RemoteGrouping(outputName, expression);
    }

    /** Whether this grouping is a plain field reference (no rendered expression). */
    public boolean isPlainField() {
        return expression == null;
    }
}
