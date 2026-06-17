/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregateState;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteGrouping;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * <p>This step supports {@code COUNT(*)} / {@code COUNT(field)} and {@code MIN(field)} / {@code MAX(field)}, with
 * or without groupings (the connector derives each aggregate's {@code seen} marker from value nullness, so a
 * grouped MIN/MAX that is null for a group is correctly skipped by the FINAL merge). {@code SUM} / {@code AVG}
 * have a different intermediate layout and are not yet supported. Anything not supported leaves the plan
 * untouched so the local aggregate runs normally.
 */
public class PushConnectorStatsToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext ctx) {
        // The aggregate sits directly over the source, or over an EvalExec that computes grouping keys (e.g. a time
        // BUCKET). ReplaceAggregateNestedExpressionWithEval extracts evaluatable grouping functions into an Eval and
        // replaces the grouping with a reference to the eval output, so the histogram shape arrives here as
        // AggregateExec(EvalExec(ExternalSourceExec)). Look through that Eval and render its fields remotely.
        EvalExec evalExec = null;
        PhysicalPlan child = aggregateExec.child();
        if (child instanceof EvalExec ee && ee.child() instanceof ExternalSourceExec) {
            evalExec = ee;
            child = ee.child();
        }
        if (child instanceof ExternalSourceExec == false) {
            return aggregateExec;
        }
        ExternalSourceExec ext = (ExternalSourceExec) child;
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
        // Build the map of eval-field name -> rendered remote expression for a looked-through Eval (e.g. a time
        // BUCKET). Only push when EVERY eval field is consumed as a grouping key below; a leftover eval field would
        // be dropped by removing the Eval, changing the result. Each field is rendered from its source text, which
        // is valid remote ES|QL because the remote runs the same query language over the same field names.
        Map<String, String> evalRenderings = evalGroupingRenderings(evalExec);
        if (evalRenderings == null) {
            return aggregateExec;
        }
        // Supports ungrouped STATS and STATS ... BY one or more keys, where each key is a plain source attribute or
        // an eval-computed expression (e.g. BUCKET). The grouping is always a reference here; computed keys reference
        // the looked-through Eval's output.
        List<? extends Expression> groupings = aggregateExec.groupings();
        List<RemoteGrouping> remoteGroupings = new ArrayList<>(groupings.size());
        Set<String> consumedEvalFields = new HashSet<>();
        for (Expression grouping : groupings) {
            Attribute groupAttribute = Expressions.attribute(grouping);
            if (groupAttribute == null) {
                return aggregateExec;
            }
            String name = groupAttribute.name();
            String rendered = evalRenderings.get(name);
            if (rendered != null) {
                remoteGroupings.add(RemoteGrouping.ofExpression(name, rendered));
                consumedEvalFields.add(name);
            } else {
                remoteGroupings.add(RemoteGrouping.ofField(name));
            }
        }
        // Every eval field must be a grouping key; otherwise removing the Eval would drop a needed computed column.
        if (consumedEvalFields.size() != evalRenderings.size()) {
            return aggregateExec;
        }

        // A per-aggregate filter (STATS c = COUNT(*) WHERE <pred>) is forwarded by its source text, which is only
        // valid remotely when the predicate references plain source columns the remote knows under the same name.
        Set<String> sourceFieldNames = new HashSet<>();
        for (Attribute attr : ext.output()) {
            sourceFieldNames.add(attr.name());
        }
        boolean grouped = groupings.isEmpty() == false;
        List<RemoteAggregate> remoteAggregates = new ArrayList<>(aggregateExec.aggregates().size());
        for (NamedExpression agg : aggregateExec.aggregates()) {
            if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction fn) {
                // COUNT/MIN/MAX/SUM are supported both ungrouped and grouped. The connector emits each aggregate's
                // intermediate state from a recipe (RemoteAggregateState) the rule derives from the function's
                // intermediate-state descriptor, deriving the seen marker from value nullness (so a null per-group
                // result is skipped by the FINAL merge) and filling auxiliary channels (SUM-double's Kahan delta,
                // SUM-long's overflow failed) with their identity value. AVG is a surrogate over SUM+COUNT and never
                // reaches here. Anything else returns null from toRemoteAggregate and leaves the STATS local.
                RemoteAggregate remote = toRemoteAggregate(alias.name(), fn, sourceFieldNames, grouped);
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

    /**
     * Renders the fields of a looked-through grouping {@link EvalExec} into remote ES|QL fragments, keyed by output
     * name. Returns an empty map when there is no Eval (the aggregate sits directly over the source). Returns
     * {@code null} (bail) when any eval field cannot be safely forwarded — i.e. its source text is unavailable, or it
     * references another eval output rather than only source columns (which the remote would not have under that
     * name). The source text of a parsed expression (e.g. {@code BUCKET(@timestamp, 5 minutes)}) is valid remote
     * ES|QL because the remote runs the same query language over the same field names.
     */
    private static Map<String, String> evalGroupingRenderings(EvalExec evalExec) {
        if (evalExec == null) {
            return Map.of();
        }
        Map<String, String> renderings = new HashMap<>();
        for (Alias field : evalExec.fields()) {
            String sourceText = field.child().sourceText();
            if (sourceText == null || sourceText.isBlank()) {
                // No original text to forward (e.g. a synthesized expression). Bail rather than guess a rendering.
                return null;
            }
            // A field that references a prior eval field cannot be forwarded as-is: the remote has no such column.
            for (Alias other : evalExec.fields()) {
                if (other != field && field.child().anyMatch(e -> e instanceof Attribute a && a.name().equals(other.name()))) {
                    return null;
                }
            }
            renderings.put(field.name(), sourceText);
        }
        return renderings;
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
     * <p>COUNT, MIN and MAX use the two-channel {@code [value, seen]} layout and carry no explicit recipe (the
     * connector falls back to it). SUM carries an explicit {@link RemoteAggregateState} recipe because its layout is
     * type-dependent (SUM-double adds a Kahan {@code delta}, SUM-long may add an overflow {@code failed} channel).
     * AVG is a surrogate over SUM+COUNT and is rewritten before the physical plan, so it never reaches this rule.
     *
     * <p>A per-aggregate filter ({@code COUNT(*) WHERE <pred>}) is forwarded by rendering the predicate's source
     * text; it is pushed only when {@code <pred>} references plain source columns (in {@code sourceFieldNames}) the
     * remote knows under the same name, otherwise the aggregate is left for local execution.
     */
    private static RemoteAggregate toRemoteAggregate(
        String outputName,
        AggregateFunction fn,
        Set<String> sourceFieldNames,
        boolean grouped
    ) {
        String filter = renderAggregateFilter(fn, sourceFieldNames);
        if (fn.hasFilter() && filter == null) {
            // A filter is present but cannot be safely forwarded; leave the whole aggregate to local execution.
            return null;
        }
        if (fn instanceof Count count) {
            Expression field = count.field();
            if (field.foldable()) {
                // COUNT(*) / COUNT(<literal>): no input field.
                return new RemoteAggregate(outputName, "COUNT", null, filter);
            }
            return field instanceof Attribute attr ? new RemoteAggregate(outputName, "COUNT", attr.name(), filter) : null;
        }
        if (fn instanceof Min min) {
            return fieldAggregate(outputName, "MIN", min.field(), filter);
        }
        if (fn instanceof Max max) {
            return fieldAggregate(outputName, "MAX", max.field(), filter);
        }
        if (fn instanceof Sum sum) {
            // SUM needs an explicit intermediate-state recipe (delta / failed channels). A windowed SUM has a
            // different intermediate layout this recipe does not capture, so only push the non-windowed form. (A
            // per-aggregate filter is handled separately above and is compatible.)
            if (AggregateFunction.NO_WINDOW.equals(sum.window()) == false) {
                return null;
            }
            if (sum.field() instanceof Attribute attr) {
                return new RemoteAggregate(outputName, "SUM", attr.name(), filter, intermediateState(fn, grouped));
            }
            return null;
        }
        return null;
    }

    /**
     * Builds the {@link RemoteAggregateState} recipe for an aggregate from its intermediate-state descriptor: the
     * first channel is the primary {@code VALUE}, a {@code seen} channel is {@code SEEN}, and every other channel is
     * {@code NEUTRAL} (filled by the connector with its identity value). This mirrors the channel conventions shared
     * by the pushed aggregates and lets the connector emit intermediate state without depending on aggregator
     * internals.
     */
    private static RemoteAggregateState intermediateState(AggregateFunction fn, boolean grouped) {
        List<IntermediateStateDesc> descs = AggregateMapper.intermediateStateDesc(fn, grouped);
        List<RemoteAggregateState.Channel> channels = new ArrayList<>(descs.size());
        for (int i = 0; i < descs.size(); i++) {
            IntermediateStateDesc desc = descs.get(i);
            RemoteAggregateState.Role role;
            if (i == 0) {
                role = RemoteAggregateState.Role.VALUE;
            } else if (desc.name().equals("seen")) {
                role = RemoteAggregateState.Role.SEEN;
            } else {
                role = RemoteAggregateState.Role.NEUTRAL;
            }
            channels.add(new RemoteAggregateState.Channel(desc.name(), desc.type(), role));
        }
        return new RemoteAggregateState(channels);
    }

    /**
     * Renders a per-aggregate filter to remote ES|QL, or {@code null} when there is no filter to push or it cannot be
     * forwarded safely. Returns {@code null} (no filter) when {@code fn.hasFilter()} is false. Otherwise returns the
     * predicate's source text when it is present and references only plain source columns, or {@code null} (bail)
     * when the source text is missing or the predicate references a column the remote does not have.
     */
    private static String renderAggregateFilter(AggregateFunction fn, Set<String> sourceFieldNames) {
        if (fn.hasFilter() == false) {
            return null;
        }
        Expression filter = fn.filter();
        String sourceText = filter.sourceText();
        if (sourceText == null || sourceText.isBlank()) {
            return null;
        }
        for (Attribute referenced : filter.references()) {
            if (sourceFieldNames.contains(referenced.name()) == false) {
                return null;
            }
        }
        return sourceText;
    }

    /** Builds a {@link RemoteAggregate} for a single-field aggregate, or {@code null} when the input is not a plain attribute. */
    private static RemoteAggregate fieldAggregate(String outputName, String function, Expression field, String filter) {
        return field instanceof Attribute attr ? new RemoteAggregate(outputName, function, attr.name(), filter) : null;
    }

    private static boolean aggregatePushdownSupported(String sourceType, LocalPhysicalOptimizerContext ctx) {
        return ctx.external() != null && ctx.external().aggregatePushdownSupported(sourceType);
    }
}
