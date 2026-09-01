/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.instanceOf;

public class PushSampleToExternalSourceTests extends ESTestCase {

    private static final String CONNECTOR_TYPE = "elasticsearch";
    private static final String FILE_TYPE = "file";

    public void testSamplePushedToSupportedConnector() {
        ExternalSourceExec ext = connectorSource();
        assertEquals(FormatReader.NO_SAMPLE, ext.pushedSample(), 0.0);

        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(0.1));
        PhysicalPlan result = applyRule(sample, CONNECTOR_TYPE, true);

        // The SampleExec is removed (no double-sampling); the source carries the pushed probability.
        assertThat(result, instanceOf(ExternalSourceExec.class));
        assertEquals(0.1, ((ExternalSourceExec) result).pushedSample(), 0.0);
    }

    public void testSampleNotPushedWhenConnectorDoesNotSupportIt() {
        ExternalSourceExec ext = connectorSource();
        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(0.1));

        PhysicalPlan result = applyRule(sample, CONNECTOR_TYPE, false);

        // The SampleExec stays and samples locally; the source is unannotated.
        assertThat(result, instanceOf(SampleExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((SampleExec) result).child();
        assertEquals(FormatReader.NO_SAMPLE, resultExt.pushedSample(), 0.0);
    }

    public void testSampleNotPushedForUnknownSourceType() {
        ExternalSourceExec ext = new ExternalSourceExec(Source.EMPTY, "file:///test.csv", FILE_TYPE, attrs(), Map.of(), Map.of(), null);
        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(0.1));

        // No factory registered for the file source type, so the sample is left for the local SampleExec.
        PhysicalPlan result = applyRule(sample, CONNECTOR_TYPE, true);
        assertThat(result, instanceOf(SampleExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((SampleExec) result).child();
        assertEquals(FormatReader.NO_SAMPLE, resultExt.pushedSample(), 0.0);
    }

    public void testSampleNotPushedWhenSortAlreadyPushed() {
        ExternalSourceExec ext = connectorSource().withPushedLimit(10);
        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(0.1));

        // A pushed limit already constrains the row set, so the sample is left local rather than composed.
        PhysicalPlan result = applyRule(sample, CONNECTOR_TYPE, true);
        assertThat(result, instanceOf(SampleExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((SampleExec) result).child();
        assertEquals(FormatReader.NO_SAMPLE, resultExt.pushedSample(), 0.0);
    }

    public void testSampleNotPushedWhenAggregateAlreadyPushed() {
        ExternalSourceExec ext = connectorSource().withPushedAggregate(
            List.of(RemoteAggregate.of("c", "COUNT", null)),
            List.of(),
            List.of(referenceAttribute("c", DataType.LONG)),
            false
        );
        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(0.1));

        PhysicalPlan result = applyRule(sample, CONNECTOR_TYPE, true);
        assertThat(result, instanceOf(SampleExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((SampleExec) result).child();
        assertEquals(FormatReader.NO_SAMPLE, resultExt.pushedSample(), 0.0);
    }

    public void testSampleNotPushedForOutOfRangeProbability() {
        ExternalSourceExec ext = connectorSource();
        // A probability of 1.0 (keep all) is a no-op and not worth a remote rewrite.
        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(1.0));

        PhysicalPlan result = applyRule(sample, CONNECTOR_TYPE, true);
        assertThat(result, instanceOf(SampleExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((SampleExec) result).child();
        assertEquals(FormatReader.NO_SAMPLE, resultExt.pushedSample(), 0.0);
    }

    public void testSampleNotPushedWithoutExternalContext() {
        ExternalSourceExec ext = connectorSource();
        SampleExec sample = new SampleExec(Source.EMPTY, ext, probability(0.1));

        PushSampleToExternalSource rule = new PushSampleToExternalSource();
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null
        );
        PhysicalPlan result = rule.apply(sample, ctx);
        assertThat(result, instanceOf(SampleExec.class));
        ExternalSourceExec resultExt = (ExternalSourceExec) ((SampleExec) result).child();
        assertEquals(FormatReader.NO_SAMPLE, resultExt.pushedSample(), 0.0);
    }

    public void testWithPushedSampleReturnsNewInstance() {
        ExternalSourceExec ext = connectorSource();
        ExternalSourceExec withSample = ext.withPushedSample(0.25);

        assertNotSame(ext, withSample);
        assertEquals(FormatReader.NO_SAMPLE, ext.pushedSample(), 0.0);
        assertEquals(0.25, withSample.pushedSample(), 0.0);
        assertEquals(ext.sourcePath(), withSample.sourcePath());
    }

    private static ExternalSourceExec connectorSource() {
        return new ExternalSourceExec(Source.EMPTY, "es+https://host/logs*", CONNECTOR_TYPE, attrs(), Map.of(), Map.of(), null);
    }

    private static List<Attribute> attrs() {
        return List.of(referenceAttribute("x", DataType.INTEGER));
    }

    private static Expression probability(double value) {
        return new Literal(Source.EMPTY, value, DataType.DOUBLE);
    }

    private static PhysicalPlan applyRule(SampleExec sample, String connectorType, boolean sampleSupported) {
        PushSampleToExternalSource rule = new PushSampleToExternalSource();
        ExternalOptimizerContext external = new ExternalOptimizerContext(
            null,
            Map.of(connectorType, new StubFactory(connectorType, sampleSupported))
        );
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            null,
            FoldContext.small(),
            null,
            external
        );
        return rule.apply(sample, ctx);
    }

    /** Minimal {@link ExternalSourceFactory} that only answers the sample-pushdown capability the rule consults. */
    private record StubFactory(String type, boolean sampleSupported) implements ExternalSourceFactory {
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
        public boolean samplePushdownSupported() {
            return sampleSupported;
        }
    }
}
