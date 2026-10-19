/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;

/**
 * Pushes a {@code SAMPLE p} (a {@link SampleExec}) directly over an {@link ExternalSourceExec} into the source,
 * but only for connector sources that apply the sample remotely (see
 * {@link ExternalSourceFactory#samplePushdownSupported()}). The elasticsearch connector renders the sample into
 * the remote {@code _query} so the remote cluster draws the random sample over the <em>full</em> dataset, instead
 * of the connector materializing a bounded page locally and sampling only that page.
 *
 * <p>Unlike filter / sort / limit pushdown, this rule <b>removes</b> the local {@link SampleExec}: a sample is a
 * random row filter, so a local {@code SampleExec} left over a remotely-sampled stream would sample twice and keep
 * roughly {@code p * p} of the rows. The rule therefore only fires when it is certain the remote applies an
 * equivalent sample, and then replaces the {@code SampleExec} with the annotated source. (Mirrors how
 * {@code PushConnectorStatsToExternalSource} removes the aggregate it pushes.)
 *
 * <p>Conditions for the rule to fire:
 * <ul>
 *   <li>The {@link SampleExec}'s child is an {@link ExternalSourceExec}.</li>
 *   <li>The source type's {@link ExternalSourceFactory} reports {@link ExternalSourceFactory#samplePushdownSupported()}.</li>
 *   <li>The source has not already been annotated with a pushed sample, sort, limit, or aggregate. A sample changes
 *       the row set every other pushed op operates over, so composing it with an already-pushed op would change
 *       that op's input inconsistently relative to the surviving plan; v1 declines and leaves the {@code SAMPLE}
 *       local in that case (correct, just not pushed).</li>
 *   <li>The probability folds to a value in the open interval {@code (0, 1)}. A probability of {@code 1.0} (keep
 *       all) is a no-op and not worth a remote rewrite; {@code 0.0} or out-of-range values are left local.</li>
 * </ul>
 */
public class PushSampleToExternalSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    SampleExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(SampleExec sampleExec, LocalPhysicalOptimizerContext ctx) {
        if (sampleExec.child() instanceof ExternalSourceExec ext) {
            if (samplePushdownSupported(ext.sourceType(), ctx) == false) {
                return sampleExec;
            }
            // A sample changes the row set the source produces, so only push it onto a "clean" source: if a sample,
            // sort, limit, or aggregate is already pushed, composing them here would change that op's input set
            // relative to the surviving plan. Leave the SAMPLE local in that case (still correct, just not pushed).
            if (ext.pushedSample() != FormatReader.NO_SAMPLE
                || ext.pushedSort().isEmpty() == false
                || ext.pushedLimit() != FormatReader.NO_LIMIT
                || ext.pushedAggregates().isEmpty() == false) {
                return sampleExec;
            }
            Double probability = foldProbability(sampleExec, ctx);
            if (probability == null) {
                return sampleExec;
            }
            // Remove the SampleExec: the remote now applies the sample, and keeping the local one would sample twice.
            return ext.withPushedSample(probability);
        }
        return sampleExec;
    }

    private static boolean samplePushdownSupported(String sourceType, LocalPhysicalOptimizerContext ctx) {
        return ctx.external() != null && ctx.external().samplePushdownSupported(sourceType);
    }

    /**
     * Folds the sample probability to a {@link Double} in the open interval {@code (0, 1)}, or {@code null} when it
     * is not foldable to a number in that range so the caller leaves the plan untouched. The ES|QL analyzer already
     * validates that {@code SAMPLE}'s argument is a constant in {@code (0, 1)}, so this is a defensive re-check that
     * also yields the primitive value the source carries.
     */
    private static Double foldProbability(SampleExec sampleExec, LocalPhysicalOptimizerContext ctx) {
        Object folded = Foldables.valueOf(ctx.foldCtx(), sampleExec.probability());
        if (folded instanceof Number n) {
            double value = n.doubleValue();
            if (value > 0.0 && value < 1.0) {
                return value;
            }
        }
        return null;
    }
}
