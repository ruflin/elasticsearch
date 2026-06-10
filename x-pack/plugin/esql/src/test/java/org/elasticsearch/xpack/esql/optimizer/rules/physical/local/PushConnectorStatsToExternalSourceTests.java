/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.instanceOf;

public class PushConnectorStatsToExternalSourceTests extends ESTestCase {

    private static final String CONNECTOR_TYPE = "elasticsearch";
    private static final ReferenceAttribute MESSAGE = referenceAttribute("message", DataType.KEYWORD);

    public void testCountStarSingleModeReplacesAggregateWithFinalSource() {
        ExternalSourceExec ext = connectorSource();
        AggregateExec agg = singleAggregate(ext, countStar());

        PhysicalPlan result = applyRule(agg, true);

        // SINGLE: the AggregateExec is removed; the source becomes the leaf carrying the pushed aggregate.
        assertThat(result, instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals(1, resultExt.pushedAggregates().size());
        RemoteAggregate remote = resultExt.pushedAggregates().get(0);
        assertEquals("COUNT", remote.function());
        assertNull(remote.field());
        assertTrue(resultExt.pushedGroupings().isEmpty());
        // SINGLE emits final values, output is the aggregate's final output schema.
        assertFalse(resultExt.pushedAggregateIntermediate());
        assertEquals(1, resultExt.output().size());
        assertEquals(remote.outputName(), resultExt.output().get(0).name());
    }

    public void testCountStarInitialModeReplacesInitialWithIntermediateSource() {
        // The real external-source plan is FINAL(INITIAL(source)); this rule fires on the INITIAL aggregate.
        ExternalSourceExec ext = connectorSource();
        List<Attribute> intermediate = countIntermediateAttributes();
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(),
            List.of(countStar()),
            AggregatorMode.INITIAL,
            intermediate,
            null
        );

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals(1, resultExt.pushedAggregates().size());
        assertEquals("COUNT", resultExt.pushedAggregates().get(0).function());
        // INITIAL emits intermediate aggregator state; output must match the INITIAL intermediate attributes
        // so the surviving FINAL aggregate consumes the right schema.
        assertTrue(resultExt.pushedAggregateIntermediate());
        assertEquals(intermediate, resultExt.output());
    }

    public void testCountFieldReplacesAggregate() {
        ExternalSourceExec ext = connectorSource();
        AggregateExec agg = singleAggregate(ext, alias("c", new Count(Source.EMPTY, MESSAGE)));

        PhysicalPlan result = applyRule(agg, true);
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals("message", resultExt.pushedAggregates().get(0).field());
    }

    public void testCountStarPushedWithPushedFilter() {
        Expression filter = new Equals(Source.EMPTY, MESSAGE, Literal.keyword(Source.EMPTY, "error"), null);
        ExternalSourceExec ext = connectorSource().withPushedFilterAndExpressions("elasticsearch-where", List.of(filter));
        AggregateExec agg = singleAggregate(ext, countStar());

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals("elasticsearch-where", resultExt.pushedFilter());
        assertEquals(List.of(filter), resultExt.pushedExpressions());
        assertEquals(1, resultExt.pushedAggregates().size());
        assertEquals("COUNT", resultExt.pushedAggregates().get(0).function());
    }

    public void testNotPushedWithPushedLimit() {
        AggregateExec agg = singleAggregate(connectorSource().withPushedLimit(10), countStar());

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testNotPushedWhenConnectorDoesNotSupportAggregates() {
        AggregateExec agg = singleAggregate(connectorSource(), countStar());
        PhysicalPlan result = applyRule(agg, false);
        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testNotPushedWhenSortAlreadyPushed() {
        // A source with a pushed sort cannot also receive a pushed aggregate: the sort changes the row set
        // the remote aggregates over, relative to what the surviving plan expects.
        Order sortOrder = new Order(Source.EMPTY, MESSAGE, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        ExternalSourceExec ext = connectorSource().withPushedSort(List.of(sortOrder));
        AggregateExec agg = singleAggregate(ext, countStar());

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testNotPushedWhenAggregatesAlreadyPushed() {
        // A source that already carries a pushed aggregate must not be annotated a second time.
        ExternalSourceExec ext = connectorSource().withPushedAggregate(
            List.of(new RemoteAggregate("c", "COUNT", null)),
            List.of(),
            List.of(referenceAttribute("c", DataType.LONG)),
            false
        );
        AggregateExec agg = singleAggregate(ext, countStar());

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testGroupedStatsSingleKeyPushedInitialMode() {
        // STATS c = COUNT(*) BY message, INITIAL mode: the source emits the grouped intermediate state.
        ExternalSourceExec ext = connectorSource();
        List<Attribute> intermediate = List.of(
            MESSAGE,
            referenceAttribute("c", DataType.LONG),
            referenceAttribute("c$seen", DataType.BOOLEAN)
        );
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(MESSAGE),
            List.of(countStar(), MESSAGE),
            AggregatorMode.INITIAL,
            intermediate,
            null
        );

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals(List.of("message"), resultExt.pushedGroupings());
        assertEquals(1, resultExt.pushedAggregates().size());
        assertEquals("COUNT", resultExt.pushedAggregates().get(0).function());
        assertTrue(resultExt.pushedAggregateIntermediate());
        assertEquals(intermediate, resultExt.output());
    }

    public void testGroupedStatsMultipleKeysPushed() {
        // STATS c = COUNT(*) BY message, level
        ExternalSourceExec ext = connectorSource();
        ReferenceAttribute level = referenceAttribute("level", DataType.KEYWORD);
        List<Attribute> intermediate = List.of(
            MESSAGE,
            level,
            referenceAttribute("c", DataType.LONG),
            referenceAttribute("c$seen", DataType.BOOLEAN)
        );
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(MESSAGE, level),
            List.of(countStar(), MESSAGE, level),
            AggregatorMode.INITIAL,
            intermediate,
            null
        );

        PhysicalPlan result = applyRule(agg, true);

        assertThat(result, instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals(List.of("message", "level"), resultExt.pushedGroupings());
        assertEquals(1, resultExt.pushedAggregates().size());
        assertTrue(resultExt.pushedAggregateIntermediate());
        assertEquals(intermediate, resultExt.output());
    }

    public void testNotPushedForGroupedMinMax() {
        // STATS m = MAX(message) BY level: grouped MIN/MAX would need a dense per-group value channel with the seen
        // marker carrying presence; the connector cannot represent a null per-group MIN/MAX, so it stays local.
        ExternalSourceExec ext = connectorSource();
        ReferenceAttribute level = referenceAttribute("level", DataType.KEYWORD);
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(level),
            List.of(alias("m", new Max(Source.EMPTY, MESSAGE)), level),
            AggregatorMode.INITIAL,
            List.of(level, referenceAttribute("m", DataType.KEYWORD), referenceAttribute("m$seen", DataType.BOOLEAN)),
            null
        );
        PhysicalPlan result = applyRule(agg, true);
        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testNotPushedForNonAttributeGrouping() {
        // A grouping that is not a plain attribute (here a literal stands in for e.g. BY bucket(...)) is out of scope.
        ExternalSourceExec ext = connectorSource();
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            ext,
            List.of(Literal.keyword(Source.EMPTY, "g")),
            List.of(countStar()),
            AggregatorMode.INITIAL,
            List.of(referenceAttribute("c", DataType.LONG), referenceAttribute("c$seen", DataType.BOOLEAN)),
            null
        );
        PhysicalPlan result = applyRule(agg, true);
        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testFinalModeNotPushed() {
        // A FINAL aggregate's child is the INITIAL aggregate, never the source; even if we hand it a source
        // child directly, FINAL must not be pushed (it would merge nothing and drop the remote partial).
        AggregateExec agg = new AggregateExec(
            Source.EMPTY,
            connectorSource(),
            List.of(),
            List.of(countStar()),
            AggregatorMode.FINAL,
            countIntermediateAttributes(),
            null
        );
        PhysicalPlan result = applyRule(agg, true);
        assertThat(result, instanceOf(AggregateExec.class));
    }

    public void testMinFieldPushed() {
        ExternalSourceExec ext = connectorSource();
        AggregateExec agg = singleAggregate(ext, alias("m", new Min(Source.EMPTY, MESSAGE)));

        PhysicalPlan result = applyRule(agg, true);
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals("MIN", resultExt.pushedAggregates().get(0).function());
        assertEquals("message", resultExt.pushedAggregates().get(0).field());
    }

    public void testMaxFieldPushed() {
        ExternalSourceExec ext = connectorSource();
        AggregateExec agg = singleAggregate(ext, alias("m", new Max(Source.EMPTY, MESSAGE)));

        PhysicalPlan result = applyRule(agg, true);
        ExternalSourceExec resultExt = (ExternalSourceExec) result;
        assertEquals("MAX", resultExt.pushedAggregates().get(0).function());
        assertEquals("message", resultExt.pushedAggregates().get(0).field());
    }

    public void testNotPushedForUnsupportedFunctionYet() {
        // SUM has a multi-channel intermediate state and is outside this step's scope; the local aggregate remains.
        AggregateExec agg = singleAggregate(connectorSource(), alias("s", new Sum(Source.EMPTY, MESSAGE)));
        PhysicalPlan result = applyRule(agg, true);
        assertThat(result, instanceOf(AggregateExec.class));
    }

    private static ExternalSourceExec connectorSource() {
        List<Attribute> attrs = List.of(MESSAGE);
        return new ExternalSourceExec(Source.EMPTY, "es+https://host/logs*", CONNECTOR_TYPE, attrs, Map.of(), Map.of(), null);
    }

    private static AggregateExec singleAggregate(ExternalSourceExec child, NamedExpression aggregate) {
        return new AggregateExec(Source.EMPTY, child, List.of(), List.of(aggregate), AggregatorMode.SINGLE, List.of(), null);
    }

    private static Alias countStar() {
        return alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
    }

    /** COUNT's two-channel intermediate aggregator state: a long count value and a boolean {@code seen} marker. */
    private static List<Attribute> countIntermediateAttributes() {
        return List.of(referenceAttribute("c", DataType.LONG), referenceAttribute("c$seen", DataType.BOOLEAN));
    }

    private static Alias alias(String name, Expression child) {
        return new Alias(Source.EMPTY, name, child);
    }

    private static PhysicalPlan applyRule(AggregateExec agg, boolean aggregatesSupported) {
        PushConnectorStatsToExternalSource rule = new PushConnectorStatsToExternalSource();
        ExternalOptimizerContext external = new ExternalOptimizerContext(
            null,
            Map.of(CONNECTOR_TYPE, new StubFactory(CONNECTOR_TYPE, aggregatesSupported))
        );
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null,
            external
        );
        return rule.apply(agg, ctx);
    }

    private record StubFactory(String type, boolean aggregatesSupported) implements ExternalSourceFactory {
        @Override
        public boolean canHandle(String location) {
            return true;
        }

        @Override
        public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateConfig(String location, Map<String, Object> config) {}

        @Override
        public boolean aggregatePushdownSupported() {
            return aggregatesSupported;
        }
    }
}
