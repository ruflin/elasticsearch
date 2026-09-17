/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.instanceOf;

public class PushSortToExternalSourceTests extends ESTestCase {

    private static final String CONNECTOR_TYPE = "elasticsearch";
    private static final String FILE_TYPE = "file";

    public void testSortPushedToSupportedConnector() {
        ExternalSourceExec ext = connectorSource();
        assertTrue(ext.pushedSort().isEmpty());
        assertEquals(FormatReader.NO_LIMIT, ext.pushedLimit());

        TopNExec topN = new TopNExec(Source.EMPTY, ext, List.of(order("x", true)), literal(10), null);
        PhysicalPlan result = applyRule(topN, CONNECTOR_TYPE, true);

        // TopNExec stays as the safety net; only the source is annotated.
        assertThat(result, instanceOf(TopNExec.class));
        TopNExec resultTopN = (TopNExec) result;
        assertThat(resultTopN.child(), instanceOf(ExternalSourceExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) resultTopN.child();
        assertEquals(1, resultExt.pushedSort().size());
        assertEquals(10, resultExt.pushedLimit());
    }

    public void testSortNotPushedWhenConnectorDoesNotSupportIt() {
        ExternalSourceExec ext = connectorSource();
        TopNExec topN = new TopNExec(Source.EMPTY, ext, List.of(order("x", true)), literal(10), null);

        PhysicalPlan result = applyRule(topN, CONNECTOR_TYPE, false);

        assertThat(result, instanceOf(TopNExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((TopNExec) result).child();
        assertTrue(resultExt.pushedSort().isEmpty());
        assertEquals(FormatReader.NO_LIMIT, resultExt.pushedLimit());
    }

    public void testSortNotPushedForUnknownSourceType() {
        ExternalSourceExec ext = new ExternalSourceExec(Source.EMPTY, "file:///test.csv", FILE_TYPE, attrs(), Map.of(), Map.of(), null);
        TopNExec topN = new TopNExec(Source.EMPTY, ext, List.of(order("x", true)), literal(10), null);

        // No factory registered for the file source type, so the sort is left for the local TopN.
        PhysicalPlan result = applyRule(topN, CONNECTOR_TYPE, true);
        ExternalSourceExec resultExt = (ExternalSourceExec) ((TopNExec) result).child();
        assertTrue(resultExt.pushedSort().isEmpty());
    }

    public void testSortNotPushedForExpressionKey() {
        ExternalSourceExec ext = connectorSource();
        Order expressionOrder = new Order(
            Source.EMPTY,
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            Order.OrderDirection.ASC,
            Order.NullsPosition.LAST
        );
        TopNExec topN = new TopNExec(Source.EMPTY, ext, List.of(expressionOrder), literal(10), null);

        PhysicalPlan result = applyRule(topN, CONNECTOR_TYPE, true);

        ExternalSourceExec resultExt = (ExternalSourceExec) ((TopNExec) result).child();
        assertTrue(resultExt.pushedSort().isEmpty());
        assertEquals(FormatReader.NO_LIMIT, resultExt.pushedLimit());
    }

    public void testSortPushedAfterAggregatePushdown() {
        ExternalSourceExec ext = connectorSource().withPushedAggregate(
            List.of(RemoteAggregate.of("c", "COUNT", null)),
            List.of(),
            List.of(referenceAttribute("c", DataType.LONG)),
            false
        );
        TopNExec topN = new TopNExec(Source.EMPTY, ext, List.of(order("c", false)), literal(5), null);

        PhysicalPlan result = applyRule(topN, CONNECTOR_TYPE, true);

        ExternalSourceExec resultExt = (ExternalSourceExec) ((TopNExec) result).child();
        assertEquals(1, resultExt.pushedAggregates().size());
        assertEquals(1, resultExt.pushedSort().size());
        assertEquals(5, resultExt.pushedLimit());
    }

    public void testSortNotPushedWithoutExternalContext() {
        ExternalSourceExec ext = connectorSource();
        TopNExec topN = new TopNExec(Source.EMPTY, ext, List.of(order("x", true)), literal(10), null);

        PushSortToExternalSource rule = new PushSortToExternalSource();
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null
        );
        PhysicalPlan result = rule.apply(topN, ctx);
        ExternalSourceExec resultExt = (ExternalSourceExec) ((TopNExec) result).child();
        assertTrue(resultExt.pushedSort().isEmpty());
    }

    public void testWithPushedSortReturnsNewInstance() {
        ExternalSourceExec ext = connectorSource();
        ExternalSourceExec withSort = ext.withPushedSort(List.of(order("x", false)));

        assertNotSame(ext, withSort);
        assertTrue(ext.pushedSort().isEmpty());
        assertEquals(1, withSort.pushedSort().size());
        assertEquals(ext.sourcePath(), withSort.sourcePath());
    }

    private static ExternalSourceExec connectorSource() {
        return new ExternalSourceExec(Source.EMPTY, "es+https://host/logs*", CONNECTOR_TYPE, attrs(), Map.of(), Map.of(), null);
    }

    private static List<Attribute> attrs() {
        return List.of(referenceAttribute("x", DataType.INTEGER));
    }

    private static Order order(String name, boolean asc) {
        return new Order(
            Source.EMPTY,
            referenceAttribute(name, DataType.INTEGER),
            asc ? Order.OrderDirection.ASC : Order.OrderDirection.DESC,
            Order.NullsPosition.LAST
        );
    }

    private static Literal literal(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static PhysicalPlan applyRule(TopNExec topN, String connectorType, boolean sortSupported) {
        PushSortToExternalSource rule = new PushSortToExternalSource();
        ExternalOptimizerContext external = new ExternalOptimizerContext(
            null,
            Map.of(connectorType, new StubFactory(connectorType, sortSupported))
        );
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null,
            external
        );
        return rule.apply(topN, ctx);
    }

    /** Minimal {@link ExternalSourceFactory} that only answers the sort-pushdown capability the rule consults. */
    private record StubFactory(String type, boolean sortSupported) implements ExternalSourceFactory {
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
        public boolean sortPushdownSupported() {
            return sortSupported;
        }
    }
}
