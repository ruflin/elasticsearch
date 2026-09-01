/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.io.IOException;
import java.util.Objects;

/**
 * An {@link ExternalSplit} representing a single ClickHouse table scan.
 *
 * <p>The connector is single-cursor (one HTTP {@code SELECT} per query), so the split provider emits
 * exactly one of these. Its sole purpose is to give the planner a concrete unit of work to schedule:
 * without at least one split, an ungrouped aggregate such as {@code STATS COUNT(*)} has nothing to
 * execute and returns zero rows. The split carries the {@code database.table} only for diagnostics;
 * the connection details travel in the query config, and {@link ClickHouseConnector#execute} reads the
 * whole projected table regardless of the split.
 */
public class ClickHouseSplit implements ExternalSplit {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        ExternalSplit.class,
        "ClickHouseSplit",
        ClickHouseSplit::new
    );

    private final String target;

    public ClickHouseSplit(String target) {
        this.target = Objects.requireNonNull(target, "target must not be null");
    }

    public ClickHouseSplit(StreamInput in) throws IOException {
        this.target = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(target);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String sourceType() {
        return "clickhouse";
    }

    public String target() {
        return target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClickHouseSplit that = (ClickHouseSplit) o;
        return target.equals(that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target);
    }

    @Override
    public String toString() {
        return "ClickHouseSplit[target=" + target + "]";
    }
}
