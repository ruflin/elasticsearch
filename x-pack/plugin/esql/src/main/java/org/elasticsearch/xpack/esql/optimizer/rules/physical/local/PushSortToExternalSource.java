/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

/**
 * Pushes a {@code SORT ... | LIMIT n} (represented as a {@link TopNExec}) directly over an
 * {@link ExternalSourceExec} into the source, but only for connector sources that apply the sort remotely
 * (see {@link ExternalSourceFactory#sortPushdownSupported()}). The elasticsearch connector renders the sort
 * and limit into the remote {@code _query}, so the remote cluster returns the correct global top-N rather than
 * an arbitrary page.
 *
 * <p>The {@link TopNExec} is <em>not</em> removed: it stays as the correctness safety net. If the source
 * under- or over-applies the sort, the local Top-N still produces the right answer; the pushdown only reduces how
 * much data crosses the wire.
 *
 * <p>Conditions for the rule to fire:
 * <ul>
 *   <li>The {@link TopNExec}'s child is an {@link ExternalSourceExec}.</li>
 *   <li>The source has not already been annotated with a pushed sort.</li>
 *   <li>The source type's {@link ExternalSourceFactory} reports {@link ExternalSourceFactory#sortPushdownSupported()}.</li>
 *   <li>The limit is a non-negative integer literal.</li>
 *   <li>Every sort key resolves to a plain attribute.</li>
 * </ul>
 *
 * <p>Distinct from {@link PushTopNIntoExternalSource}, which annotates a {@code BlockHash} Top-N pruning hint for
 * an intervening {@code STATS} aggregation; this rule handles the plain {@code SORT | LIMIT} over a source.
 */
public class PushSortToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<TopNExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(TopNExec topNExec, LocalPhysicalOptimizerContext ctx) {
        if (topNExec.child() instanceof ExternalSourceExec ext) {
            if (ext.pushedSort().isEmpty() == false) {
                return topNExec;
            }
            if (sortPushdownSupported(ext.sourceType(), ctx) == false) {
                return topNExec;
            }
            int limit = foldLimit(topNExec.limit(), ctx);
            if (limit == FormatReader.NO_LIMIT) {
                return topNExec;
            }
            List<Order> orders = topNExec.order();
            if (orders.isEmpty()) {
                return topNExec;
            }
            if (canPushAllSortKeys(orders) == false) {
                return topNExec;
            }
            // Set the limit too so the remote SORT is paired with a LIMIT and returns the top-N, not the whole
            // sorted set. The enclosing TopNExec stays as the safety net.
            ExternalSourceExec annotated = ext.withPushedSort(orders).withPushedLimit(limit);
            return topNExec.replaceChild(annotated);
        }
        return topNExec;
    }

    private static boolean sortPushdownSupported(String sourceType, LocalPhysicalOptimizerContext ctx) {
        return ctx.external() != null && ctx.external().sortPushdownSupported(sourceType);
    }

    private static boolean canPushAllSortKeys(List<Order> orders) {
        for (Order order : orders) {
            if (Expressions.attribute(order.child()) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Folds a limit expression to a non-negative int, returning {@link FormatReader#NO_LIMIT} when it is not a
     * foldable non-negative integer literal so the caller leaves the plan untouched.
     */
    private static int foldLimit(Expression limitExpr, LocalPhysicalOptimizerContext ctx) {
        if (limitExpr instanceof Literal == false) {
            return FormatReader.NO_LIMIT;
        }
        Object folded = limitExpr.fold(ctx.foldCtx());
        if (folded instanceof Number n) {
            long value = n.longValue();
            if (value < 0 || value > Integer.MAX_VALUE) {
                return FormatReader.NO_LIMIT;
            }
            return (int) value;
        }
        return FormatReader.NO_LIMIT;
    }
}
