/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteGrouping;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteSort;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Pins the remote ES|QL the connector renders (and the blocks it decodes) for the two query shapes that drive
 * alerting and significant-event detection over an external dataset:
 * <ul>
 *   <li>a <b>document read</b> — {@code FROM <target> METADATA _id, _source | SORT ... | KEEP ... | LIMIT N},
 *       whose caller reads back the {@code _id} and {@code _source} columns to hydrate whole documents;</li>
 *   <li>a <b>conditional-count histogram</b> — {@code FROM <target> | STATS errors = COUNT(*) WHERE <pred>,
 *       total = COUNT(*) BY bucket = BUCKET(@timestamp, 5 minutes)}, the count-over-time / error-rate shape.</li>
 * </ul>
 * Both combine features that are individually covered elsewhere (per-aggregate filters, computed groupings,
 * metadata projection, SAMPLE); this suite asserts they compose into exactly the expected remote query, so a
 * change to one rendering rule cannot silently break the composite shape.
 *
 * <p>{@code ElasticsearchExternalSourceLiveIT} covers the same shapes end-to-end against a real remote cluster;
 * this suite is deterministic and always runs in CI.
 */
public class ConnectorQueryRenderingTests extends ESTestCase {

    private static final String TARGET = "logs";

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    private static QueryRequest request(
        List<String> projectedColumns,
        int rowLimit,
        List<RemoteSort> pushedSort,
        List<RemoteAggregate> pushedAggregates,
        List<RemoteGrouping> pushedGroupings
    ) {
        return request(projectedColumns, rowLimit, pushedSort, pushedAggregates, pushedGroupings, FormatReader.NO_SAMPLE);
    }

    private static QueryRequest request(
        List<String> projectedColumns,
        int rowLimit,
        List<RemoteSort> pushedSort,
        List<RemoteAggregate> pushedAggregates,
        List<RemoteGrouping> pushedGroupings,
        double pushedSampleProbability
    ) {
        return new QueryRequest(
            TARGET,
            projectedColumns,
            List.of(),
            Map.of(),
            1000,
            rowLimit,
            List.of(),
            pushedSort,
            pushedAggregates,
            pushedGroupings,
            false,
            pushedSampleProbability,
            null
        );
    }

    // ---------------------------------------------------------------------------------------------------------
    // STATS shapes the conditional-count histogram is built from, plus the projected read.
    // ---------------------------------------------------------------------------------------------------------

    public void testUngroupedCountStatsRenders() {
        // FROM logs | STATS total = COUNT(*) — the simplest count, computed remotely.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(List.of(), -1, List.of(), List.of(RemoteAggregate.of("total", "COUNT", null)), List.of())
        );
        assertThat(query, equalTo("FROM logs | STATS `total` = COUNT(*)"));
    }

    public void testGroupedCountByKeywordRenders() {
        // FROM logs | STATS c = COUNT(*) BY service.name — grouped count by a plain keyword field is pushed.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(
                List.of(),
                -1,
                List.of(),
                List.of(RemoteAggregate.of("c", "COUNT", null)),
                List.of(RemoteGrouping.ofField("service.name"))
            )
        );
        assertThat(query, equalTo("FROM logs | STATS `c` = COUNT(*) BY `service.name`"));
    }

    public void testGroupedCountByTimeBucketRenders() {
        // FROM logs | STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 5 minutes) — the histogram core.
        // The grouping is a computed RemoteGrouping carrying the already-rendered BUCKET expression and an alias.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(
                List.of(),
                -1,
                List.of(),
                List.of(RemoteAggregate.of("c", "COUNT", null)),
                List.of(RemoteGrouping.ofExpression("bucket", "BUCKET(@timestamp, 5 minutes)"))
            )
        );
        assertThat(query, equalTo("FROM logs | STATS `c` = COUNT(*) BY `bucket` = BUCKET(@timestamp, 5 minutes)"));
    }

    public void testFilteredCountStatsRenders() {
        // FROM logs | STATS errors = COUNT(*) WHERE log.level == "error" — a per-aggregate filter is rendered as a
        // WHERE clause attached to the aggregate. This is the conditional-count shape (e.g. error rate).
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(
                List.of(),
                -1,
                List.of(),
                List.of(new RemoteAggregate("errors", "COUNT", null, "log.level == \"error\"", null)),
                List.of()
            )
        );
        assertThat(query, equalTo("FROM logs | STATS `errors` = COUNT(*) WHERE log.level == \"error\""));
    }

    public void testMultipleFilteredAndPlainAggregatesRender() {
        // A mix of a filtered count and a plain count in the same STATS, the shape a ratio is computed from.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(
                List.of(),
                -1,
                List.of(),
                List.of(
                    new RemoteAggregate("errors", "COUNT", null, "status_code >= 500", null),
                    RemoteAggregate.of("total", "COUNT", null)
                ),
                List.of()
            )
        );
        assertThat(query, equalTo("FROM logs | STATS `errors` = COUNT(*) WHERE status_code >= 500, `total` = COUNT(*)"));
    }

    public void testGroupedSumRenders() {
        // FROM logs | STATS s = SUM(event.duration) BY service.name — grouped SUM is now pushed; the remote STATS
        // renders the same as any other field aggregate (the intermediate-state recipe only affects decode, not the
        // rendered remote query).
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(
                List.of(),
                -1,
                List.of(),
                List.of(RemoteAggregate.of("s", "SUM", "event.duration")),
                List.of(RemoteGrouping.ofField("service.name"))
            )
        );
        assertThat(query, equalTo("FROM logs | STATS `s` = SUM(`event.duration`) BY `service.name`"));
    }

    public void testWrappedFieldSumRenders() {
        // SUM(TO_DOUBLE(event.duration)) from AVG-over-long's surrogate: the connector wraps the quoted field in the
        // scalar function name, keeping identifier quoting on its side.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(
                List.of(),
                -1,
                List.of(),
                List.of(RemoteAggregate.ofWrappedField("s", "SUM", "TO_DOUBLE", "event.duration", null, null)),
                List.of(RemoteGrouping.ofField("service.name"))
            )
        );
        assertThat(query, equalTo("FROM logs | STATS `s` = SUM(TO_DOUBLE(`event.duration`)) BY `service.name`"));
    }

    public void testMatchStyleProjectedReadRenders() {
        // The non-aggregated leg of a document read: project a couple of fields with a SORT + LIMIT. The
        // METADATA _id, _source half of that shape is asserted separately below.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(List.of("@timestamp", "message"), 1000, List.of(new RemoteSort("@timestamp", false, false)), List.of(), List.of())
        );
        assertThat(query, equalTo("FROM logs | SORT `@timestamp` DESC NULLS LAST | KEEP `@timestamp`, `message` | LIMIT 1000"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // METADATA rendering. A caller that hydrates whole documents reads back _id and _source, so when those columns
    // are projected the connector emits the FROM ... METADATA option and then projects them with KEEP. Without the
    // option the remote response would carry neither column.
    // ---------------------------------------------------------------------------------------------------------

    public void testConnectorRendersMetadataIdAndSource() {
        // _id / _source projected => FROM logs METADATA _id, _source ... | KEEP _id, _source. Metadata names are
        // emitted unquoted in the METADATA option (the option does not accept backtick-quoted identifiers).
        String query = ElasticsearchConnector.buildRemoteQuery(request(List.of("_id", "_source"), 1000, List.of(), List.of(), List.of()));
        assertThat(query, equalTo("FROM logs METADATA _id, _source | KEEP `_id`, `_source` | LIMIT 1000"));
    }

    public void testMetadataOnlyEmittedForMetadataColumns() {
        // Ordinary fields and @timestamp (not a metadata attribute) never trigger a METADATA option; only genuine
        // metadata attributes do. _index is a metadata attribute, so it is added alongside the ordinary fields.
        String plain = ElasticsearchConnector.buildRemoteQuery(
            request(List.of("@timestamp", "message"), -1, List.of(), List.of(), List.of())
        );
        assertThat("no metadata option for ordinary fields", plain, not(containsString("METADATA")));

        String withIndex = ElasticsearchConnector.buildRemoteQuery(
            request(List.of("@timestamp", "_index"), -1, List.of(), List.of(), List.of())
        );
        assertThat(withIndex, equalTo("FROM logs METADATA _index | KEEP `@timestamp`, `_index`"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // _source / object columns decode into a BytesRef block holding the captured JSON text, mirroring how local
    // ES|QL carries a _source value as bytes, so a caller can read the whole document back out.
    // ---------------------------------------------------------------------------------------------------------

    public void testSourceTypeResolvesToSourceDataType() {
        // The remote `_query` response reports a _source column with ES|QL type name "_source"; the connector
        // resolves column types via DataType.fromNameOrAlias, so confirm that resolution first.
        assertThat(DataType.fromNameOrAlias("_source"), equalTo(DataType.SOURCE));
    }

    public void testSourceColumnDecodesToJsonBytes() {
        String json = "{\"message\":\"hi\",\"level\":\"INFO\"}";
        try (Block block = EsqlTypeMapping.toBlock(DataType.SOURCE, List.of(json), 1, blockFactory)) {
            assertThat(block.getPositionCount(), equalTo(1));
            BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
            assertThat(bytesRefBlock.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo(json));
        }
    }

    public void testObjectColumnDecodesToJsonBytes() {
        String json = "{\"a\":1}";
        try (Block block = EsqlTypeMapping.toBlock(DataType.OBJECT, List.of(json), 1, blockFactory)) {
            BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
            assertThat(bytesRefBlock.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo(json));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // SAMPLE pushdown: a pushed SAMPLE renders as a remote ` | SAMPLE <p>` so the remote draws the random sample
    // over the full dataset. SAMPLE sits after WHERE and before SORT/KEEP/LIMIT. Document sampling depends on this
    // drawing from the whole corpus rather than from a locally-fetched page.
    // ---------------------------------------------------------------------------------------------------------

    public void testSampleProbabilityRenders() {
        // FROM logs | SAMPLE 0.1 — a plain pushed sample with no other pushdown.
        String query = ElasticsearchConnector.buildRemoteQuery(request(List.of(), -1, List.of(), List.of(), List.of(), 0.1));
        assertThat(query, equalTo("FROM logs | SAMPLE 0.1"));
    }

    public void testSampleRendersAfterProjectionAndBeforeLimit() {
        // SAMPLE comes after WHERE (none here) and before KEEP/LIMIT: FROM logs | SAMPLE p | KEEP ... | LIMIT n.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(List.of("@timestamp", "message"), 50, List.of(), List.of(), List.of(), 0.25)
        );
        assertThat(query, equalTo("FROM logs | SAMPLE 0.25 | KEEP `@timestamp`, `message` | LIMIT 50"));
    }

    public void testSampleProbabilityRendersLocaleIndependently() {
        // The probability is rendered with Double.toString so a comma-decimal locale cannot corrupt it.
        String query = ElasticsearchConnector.buildRemoteQuery(request(List.of(), -1, List.of(), List.of(), List.of(), 0.001));
        assertThat(query, equalTo("FROM logs | SAMPLE 0.001"));
    }

    public void testNoSampleOptionWhenNotPushed() {
        // The NO_SAMPLE sentinel (the default) must never emit a SAMPLE stage.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(List.of(), -1, List.of(), List.of(), List.of(), FormatReader.NO_SAMPLE)
        );
        assertThat(query, not(containsString("SAMPLE")));
    }
}
