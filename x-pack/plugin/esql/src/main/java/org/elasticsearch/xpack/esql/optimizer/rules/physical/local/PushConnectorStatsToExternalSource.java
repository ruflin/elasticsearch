/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes a {@code STATS} aggregation directly over an {@link ExternalSourceExec} into the source for connector
 * sources that compute the aggregate remotely and return the final result rows (see
 * {@link ExternalSourceFactory#aggregatePushdownSupported()}). The elasticsearch connector renders the aggregate
 * into the remote {@code _query}, so the remote cluster runs the whole {@code STATS} and returns one final row per
 * group.
 *
 * <p>Unlike filter / limit / sort pushdown, this rule does not keep a plain local aggregate as a safety net: a
 * connector aggregate returns already-computed results, so a local aggregate re-reading the source rows would
 * double-count. Instead it pushes into the position the aggregate occupied and rewrites the source output to that
 * position's contract:
 * <ul>
 *   <li>{@code SINGLE}: the rule <b>removes</b> the aggregate and the source emits final values directly (output
 *       = the aggregate's final output).</li>
 *   <li>{@code INITIAL}: external-source STATS is planned as {@code FINAL(INITIAL(source))}. The rule replaces the
 *       {@code INITIAL} aggregate with the source and asks it to emit <b>intermediate</b> aggregator state
 *       ({@code [value, seen]} per aggregate, output = {@link AggregateExec#intermediateAttributes()}); the
 *       surviving {@code FINAL} aggregate merges that single partial into the correct final result.</li>
 * </ul>
 * {@code FINAL} is never matched here because a FINAL aggregate's child is the INITIAL aggregate, not the source.
 *
 * <p>The rule can combine a pushed filter with the aggregate because connectors render {@code WHERE} before
 * {@code STATS}. It bails out when the source already carries a pushed sort or limit, which would change the row
 * set the remote aggregates over relative to what the surviving plan expects.
 *
 * <p>This step supports {@code COUNT(*)} / {@code COUNT(field)} with or without groupings, and ungrouped
 * {@code MIN(field)} / {@code MAX(field)}. Anything not yet supported leaves the plan untouched so the local
 * aggregate runs normally.
 */
public class PushConnectorStatsToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        if (aggregateExec.child() instanceof ExternalSourceExec == false) {
            return aggregateExec;
        }
        ExternalSourceExec ext = (ExternalSourceExec) aggregateExec.child();
        if (aggregatePushdownSupported(ext.sourceType(), ctx) == false) {
            return aggregateExec;
        }
        // SINGLE -> remove the aggregate, source emits final values.
        // INITIAL -> replace this aggregate, source emits intermediate state for the surviving FINAL to merge.
        // FINAL (and any other mode) -> its child is never the source, so leave it untouched.
        AggregatorMode mode = aggregateExec.getMode();
        boolean intermediate;
        if (mode == AggregatorMode.SINGLE) {
            intermediate = false;
        } else if (mode == AggregatorMode.INITIAL) {
            intermediate = true;
        } else {
            return aggregateExec;
        }
        // The remote aggregate runs over the rows the remote scan produces. Pushed filters are safe because the
        // connector renders them before STATS, but a pushed sort/limit on the same source would change the row set
        // the aggregate sees relative to the surviving plan.
        if (ext.pushedSort().isEmpty() == false
            || ext.pushedLimit() != FormatReader.NO_LIMIT
            || ext.pushedAggregates().isEmpty() == false) {
            return aggregateExec;
        }
        // Supports ungrouped STATS and STATS ... BY one or more plain-attribute keys. Non-attribute groupings
        // (e.g. BY bucket(...) or other grouping functions) are out of scope and left for a later step.
        List<? extends Expression> groupings = aggregateExec.groupings();
        List<String> remoteGroupings = new ArrayList<>(groupings.size());
        for (Expression grouping : groupings) {
            Attribute groupAttribute = Expressions.attribute(grouping);
            if (groupAttribute == null) {
                return aggregateExec;
            }
            remoteGroupings.add(groupAttribute.name());
        }

        boolean grouped = groupings.isEmpty() == false;
        List<RemoteAggregate> remoteAggregates = new ArrayList<>(aggregateExec.aggregates().size());
        for (NamedExpression agg : aggregateExec.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction fn) {
                // Grouped MIN/MAX would need a dense (non-null) value channel per group with the seen marker
                // carrying presence; the connector only emits seen=true with the remote value, which is unsafe when
                // a group's MIN/MAX is null. COUNT is always a dense non-null long, so it is the only grouped
                // aggregate supported here. Ungrouped MIN/MAX remain supported (single-row, non-grouping state).
                if (grouped && fn instanceof Count == false) {
                    return aggregateExec;
                }
                RemoteAggregate remote = toRemoteAggregate(alias.name(), fn);
                if (remote == null) {
                    return aggregateExec;
                }
                remoteAggregates.add(remote);
            } else if (isGroupingOutput(agg, groupings)) {
                // The grouping key also appears in the aggregates as a bare attribute (it passes through to the
                // output). It is rendered by the BY clause, not as an aggregate, so skip it here.
            } else {
                // A non-aggregate, non-grouping expression: not in this step's scope.
                return aggregateExec;
            }
        }
        if (remoteAggregates.isEmpty()) {
            return aggregateExec;
        }
        // The source must produce exactly what the replaced aggregate node produced: its final output for SINGLE,
        // or its intermediate attributes for INITIAL (so the FINAL aggregate above consumes the right schema).
        List<Attribute> output = intermediate ? aggregateExec.intermediateAttributes() : aggregateExec.output();
        return ext.withPushedAggregate(remoteAggregates, remoteGroupings, output, intermediate);
    }

    /** Whether {@code agg} is a bare reference to one of the grouping keys (so the connector renders it via BY). */
    private static boolean isGroupingOutput(NamedExpression agg, List<? extends Expression> groupings) {
        Attribute attribute = agg instanceof Alias alias ? Expressions.attribute(alias.child()) : Expressions.attribute(agg);
        if (attribute == null) {
            return false;
        }
        for (Expression grouping : groupings) {
            Attribute groupAttribute = Expressions.attribute(grouping);
            if (groupAttribute != null && groupAttribute.semanticEquals(attribute)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Projects a supported aggregate function to its {@link RemoteAggregate}, or {@code null} when the function is
     * outside this step's scope (which leaves the plan untouched).
     *
     * <p>Only aggregates whose intermediate aggregator state is the two-channel {@code [value, seen]} layout are
     * supported, because the connector emits intermediate state by interleaving a single {@code seen} marker after
     * each value block. COUNT, MIN and MAX share that layout. SUM (DOUBLE adds a Kahan {@code delta} channel) and
     * AVG (a {@code sum}+{@code count} pair) do not and are handled in a later step.
     */
    private static RemoteAggregate toRemoteAggregate(String outputName, AggregateFunction fn) {
        if (fn.hasFilter()) {
            return null;
        }
        if (fn instanceof Count count) {
            Expression field = count.field();
            if (field.foldable()) {
                // COUNT(*) / COUNT(<literal>): no input field.
                return new RemoteAggregate(outputName, "COUNT", null);
            }
            return field instanceof Attribute attr ? new RemoteAggregate(outputName, "COUNT", attr.name()) : null;
        }
        if (fn instanceof Min min) {
            return fieldAggregate(outputName, "MIN", min.field());
        }
        if (fn instanceof Max max) {
            return fieldAggregate(outputName, "MAX", max.field());
        }
        return null;
    }

    /** Builds a {@link RemoteAggregate} for a single-field aggregate, or {@code null} when the input is not a plain attribute. */
    private static RemoteAggregate fieldAggregate(String outputName, String function, Expression field) {
        return field instanceof Attribute attr ? new RemoteAggregate(outputName, function, attr.name()) : null;
    }

    private static boolean aggregatePushdownSupported(String sourceType, LocalPhysicalOptimizerContext ctx) {
        return ctx.external() != null && ctx.external().aggregatePushdownSupported(sourceType);
    }
}
