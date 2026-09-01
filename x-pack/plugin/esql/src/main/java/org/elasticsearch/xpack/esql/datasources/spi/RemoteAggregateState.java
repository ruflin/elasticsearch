/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.ElementType;

import java.util.List;

/**
 * The intermediate aggregator-state layout for a single pushed {@link RemoteAggregate}, used only when a connector
 * must emit <em>intermediate</em> state (the {@code INITIAL} position of a {@code FINAL(INITIAL(source))} plan) so a
 * surviving {@code FINAL} aggregate can merge the connector's single partial.
 *
 * <p>The ESQL aggregators expose their intermediate state as an ordered list of typed channels. The optimizer
 * captures that exact list here (derived from the function's intermediate-state descriptor) so the connector can
 * emit the right blocks without depending on aggregator internals. The connector relies on the conventions shared
 * by all currently pushed aggregates (COUNT, MIN, MAX, SUM):
 * <ul>
 *   <li>channel 0 ({@link Role#VALUE}) is the primary value (the per-group count / min / max / sum) — the connector
 *       fills it from the decoded remote result column, cast to {@link Channel#type()};</li>
 *   <li>a {@link Role#SEEN} channel (a boolean named {@code seen}) marks per-row presence — the connector fills it
 *       from the value's nullness, so a group whose value is null is skipped by the {@code FINAL} merge;</li>
 *   <li>every other channel ({@link Role#NEUTRAL}, e.g. SUM-double's Kahan {@code delta} or SUM-long's
 *       {@code failed} overflow flag) is emitted with its identity value (numeric {@code 0} / boolean
 *       {@code false}), which is correct for a single already-computed partial.</li>
 * </ul>
 *
 * <p>Never serialized: connectors execute on the coordinator only.
 *
 * @param channels the ordered intermediate-state channels for the aggregate; never empty
 */
public record RemoteAggregateState(List<Channel> channels) {

    public RemoteAggregateState {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("RemoteAggregateState channels must not be null or empty");
        }
        channels = List.copyOf(channels);
    }

    /** The role a channel plays when the connector materializes a single partial's intermediate state. */
    public enum Role {
        /** The primary aggregate value (channel 0): filled from the decoded remote result column. */
        VALUE,
        /** The {@code seen} presence marker: filled from the value's per-row nullness. */
        SEEN,
        /** An auxiliary channel (e.g. Kahan {@code delta}, overflow {@code failed}): filled with its identity value. */
        NEUTRAL
    }

    /**
     * A single intermediate-state channel.
     *
     * @param name the channel name from the aggregator's intermediate-state descriptor (e.g. {@code value},
     *        {@code sum}, {@code delta}, {@code seen}, {@code failed})
     * @param type the channel's block element type
     * @param role how the connector fills this channel for a single partial
     */
    public record Channel(String name, ElementType type, Role role) {}
}
