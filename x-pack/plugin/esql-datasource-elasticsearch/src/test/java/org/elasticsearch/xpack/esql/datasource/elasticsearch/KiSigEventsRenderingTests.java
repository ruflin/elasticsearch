/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.RemoteAggregate;
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
        List<String> pushedGroupings
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
            request(List.of(), -1, List.of(), List.of(new RemoteAggregate("total", "COUNT", null)), List.of())
        );
        assertThat(query, equalTo("FROM logs | STATS `total` = COUNT(*)"));
    }

    public void testGroupedCountByKeywordRenders() {
        // FROM logs | STATS c = COUNT(*) BY service.name — grouped count by a plain keyword field is pushed.
        String query = ElasticsearchConnector.buildRemoteQuery(
            request(List.of(), -1, List.of(), List.of(new RemoteAggregate("c", "COUNT", null)), List.of("service.name"))
        );
        assertThat(query, equalTo("FROM logs | STATS `c` = COUNT(*) BY `service.name`"));
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
    // GAP 1: METADATA _id, _source is never rendered. The KI `match` rule path reads back _id and _source, but
    // the connector only ever issues `FROM <target> ...` with no METADATA option, so the remote response carries
    // neither column. This characterises that the rendered query is metadata-free regardless of projection.
    // ---------------------------------------------------------------------------------------------------------

    public void testConnectorNeverRendersMetadataIdOrSource() {
        // Even if _id / _source are asked for as projected columns, no METADATA option is emitted: the connector
        // renders `KEEP _id, _source` over a `FROM logs` that (without METADATA) never produced those columns.
        String query = ElasticsearchConnector.buildRemoteQuery(request(List.of("_id", "_source"), 1000, List.of(), List.of(), List.of()));
        assertThat("connector does not emit a METADATA option", query, not(containsString("METADATA")));
        assertThat(query, equalTo("FROM logs | KEEP `_id`, `_source` | LIMIT 1000"));
    }

    // ---------------------------------------------------------------------------------------------------------
    // GAP 2: _source / object columns cannot be decoded into a value block. A match-rule response column typed
    // `_source` (or `object`) hits the value-decoder's default branch and throws, rather than surfacing as a
    // (null or structured) block. So even if METADATA were rendered, reading _source back would fail at decode.
    // ---------------------------------------------------------------------------------------------------------

    public void testSourceTypeResolvesToSourceDataType() {
        // The remote `_query` response reports a _source column with ES|QL type name "_source"; the connector
        // resolves column types via DataType.fromNameOrAlias, so confirm that resolution first.
        assertThat(DataType.fromNameOrAlias("_source"), equalTo(DataType.SOURCE));
    }

    public void testSourceColumnDecodeIsUnsupported() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            try (Block ignored = EsqlTypeMapping.toBlock(DataType.SOURCE, List.of("{}"), 1, blockFactory)) {}
        });
        assertThat(e.getMessage(), containsString("Unsupported remote Elasticsearch column type for value decoding"));
    }

    public void testObjectColumnDecodeIsUnsupported() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            try (Block ignored = EsqlTypeMapping.toBlock(DataType.OBJECT, List.of("{}"), 1, blockFactory)) {}
        });
        assertThat(e.getMessage(), containsString("Unsupported remote Elasticsearch column type for value decoding"));
    }
}
