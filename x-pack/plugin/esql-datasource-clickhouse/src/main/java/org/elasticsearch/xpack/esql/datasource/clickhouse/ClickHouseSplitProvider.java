/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;

import java.util.List;
import java.util.Objects;

/**
 * Emits a single {@link ClickHouseSplit} per query.
 *
 * <p>The ClickHouse connector reads a table with one HTTP {@code SELECT} (no sharded/parallel reads
 * yet), so exactly one split is produced. This is required for correctness, not just parallelism: the
 * planner only schedules connector execution when at least one split exists, so the default
 * {@link SplitProvider#SINGLE} (which returns no splits) would make ungrouped aggregates such as
 * {@code STATS COUNT(*)} return zero rows. See {@link ClickHouseSplit}.
 */
class ClickHouseSplitProvider implements SplitProvider {

    @Override
    public SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context) {
        Object database = context.config().get("database");
        Object table = context.config().get("table");
        String target = Objects.toString(database, "default") + "." + Objects.toString(table, "");
        return SplitDiscoveryResult.of(List.of(new ClickHouseSplit(target)));
    }
}
