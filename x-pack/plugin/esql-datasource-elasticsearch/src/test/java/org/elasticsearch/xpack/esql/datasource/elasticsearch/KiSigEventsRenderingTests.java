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
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteGrouping;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteSort;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Characterisation tests for the remote ES|QL the connector renders for the Kibana Knowledge-Indicator (KI)
 * extraction / KI rule generation / Significant-Events (SigEvents) use cases (the {@code streams} plugin in the
 * Kibana repo, package {@code x-pack/platform/plugins/shared/streams/server/lib/sig_events}).
 *
 * <p>Those features run two canonical ES|QL query shapes against a stream's data:
 * <ul>
 *   <li><b>{@code match}</b> rules — {@code FROM <stream> METADATA _id, _source | WHERE ... | LIMIT N}, then read
 *       back the {@code _id} and {@code _source} columns (see Kibana {@code build_esql_search_request.ts} /
 *       {@code execute_esql_request.ts}).</li>
 *   <li><b>{@code stats}</b> histograms — {@code FROM <stream> | STATS errors = COUNT(*) WHERE <pred>,
 *       total = COUNT(*) BY bucket = BUCKET(@timestamp, 5 minutes) | EVAL rate = errors * 100.0 / total
 *       | WHERE total > N AND rate > M} (the count-over-time / error-rate shape, see Kibana
 *       {@code kbn-streams-schema/src/helpers/esql_helpers.ts}).</li>
 * </ul>
 *
 * <p>These tests assert what the connector renders/decodes <em>today</em> so the supported subset is pinned and
 * the current gaps for the KI/SigEvents use case are documented as executable specs. When a connector closes one
 * of these gaps, the corresponding assertion below should be updated (it will fail, pointing the implementer at
 * the spec to revisit). The companion {@code ElasticsearchExternalSourceLiveIT} covers end-to-end behaviour
 * against a real remote; this suite is deterministic and runs in CI.
 */
public class KiSigEventsRenderingTests extends ESTestCase {

    private static final String TARGET = "logs";

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    private static QueryRequest request(
        List<String> projectedColumns,
        int rowLimit,
        List<RemoteSort> pushedSort,
        List<RemoteAggregate> pushedAggregates,
        List<RemoteGrouping> pushedGroupings
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
            null
        );
    }

    // ---------------------------------------------------------------------------------------------------------
    // SUPPORTED today: the COUNT-based STATS shapes the SigEvents histogram is built from, plus a projected read.
    // ---------------------------------------------------------------------------------------------------------

    public void testUngroupedCountStatsRenders() {
        // FROM logs | STATS total = COUNT(*) — the simplest SigEvents/KI count, computed remotely.
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
        // FROM logs | STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 5 minutes) — the SigEvents histogram core.
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
        // WHERE clause attached to the aggregate. This is the SigEvents conditional-count shape (e.g. error rate).
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
        // A mix of a filtered count and a plain count in the same STATS, as SigEvents uses for ratios.
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
        // The non-aggregated leg of a match-style read: project a couple of fields with a SORT + LIMIT. Note this
        // is only what the connector CAN render; the KI match shape additionally needs METADATA _id, _source and
        // a readable _source value, both of which are gaps documented below.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(List.of("@timestamp", "message"), 1000, List.of(new RemoteSort("@timestamp", false, false)), List.of(), List.of())
        );
        assertThat(query, equalTo("FROM logs | SORT `@timestamp` DESC NULLS LAST | KEEP `@timestamp`, `message` | LIMIT 1000"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // F5 (was GAP 1): METADATA _id, _source IS now rendered. The KI `match` rule path reads back _id and _source,
    // so when those columns are projected the connector emits the FROM ... METADATA option, then projects them
    // with KEEP. Without the option the remote response would carry neither column.
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
    // F6 (was GAP 2): _source / object columns now decode into a BytesRef block holding the captured JSON text,
    // mirroring how local ES|QL carries a _source value as bytes. This makes the KI `match` read of _source work.
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
}
