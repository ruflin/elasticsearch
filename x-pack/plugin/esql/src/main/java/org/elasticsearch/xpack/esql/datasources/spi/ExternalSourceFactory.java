/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Common interface for complete external data source factories.
 * Both API-based connectors (Flight, JDBC) and table-based catalogs (Iceberg)
 * implement this interface, enabling unified resolution and dispatch.
 *
 * Building-block factories (StorageProviderFactory, FormatReaderFactory) are NOT
 * part of this hierarchy — they are composed by the framework for file-based sources.
 */
public interface ExternalSourceFactory {

    String type();

    boolean canHandle(String location);

    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    /**
     * Reject configuration keys this factory doesn't recognize at the given location. Implementations
     * compose their claimed-key sets and call {@link ConfigKeyValidator#check}.
     * <p>
     * <b>Required override.</b> Every factory must explicitly state its validation contract — either
     * by composing claimed-key sets and delegating to {@link ConfigKeyValidator#check}, or, for
     * factories with no per-query config keys today, by calling
     * {@code ConfigKeyValidator.check(config, List.of())} to reject any non-empty config map. An
     * empty method body would silently accept typo'd configurations — exactly the footgun this
     * abstract contract exists to prevent — so do not write one.
     */
    void validateConfig(String location, Map<String, Object> config);

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    /**
     * Whether this factory's sources can compute a pushed {@code STATS} aggregation remotely and return the
     * <em>final</em> grouped result rows.
     * <p>
     * Only sources that natively understand ESQL aggregation and run it server-side — e.g. the elasticsearch
     * connector, which renders the aggregate into the remote {@code _query} — should return {@code true}. When
     * {@code true}, the optimizer may replace an {@code AggregateExec -> source} subtree with the source alone,
     * trusting it to return one final row per group (no local re-aggregation). File-based sources that answer
     * aggregates from footer statistics use {@link FormatReader#aggregatePushdownSupport()} instead and must
     * return {@code false} here (the default).
     */
    default boolean aggregatePushdownSupported() {
        return false;
    }

    /**
     * Whether this factory's sources can apply a pushed {@code SORT} (paired with the row limit) remotely.
     * <p>
     * Only sources that natively understand ESQL sorting and execute it server-side — e.g. the elasticsearch
     * connector, which renders the sort into the remote {@code _query} — should return {@code true}. File-based
     * sources and connectors that stream rows in arrival order must return {@code false} (the default); for them
     * the enclosing {@code TopNExec} does the sorting. A {@code true} here is a promise that, given the pushed
     * sort and limit, the source returns the correct global top-N rows.
     */
    default boolean sortPushdownSupported() {
        return false;
    }

    /**
     * Whether this factory resolves wildcard/multi-target patterns itself rather than via local glob
     * expansion over a {@link StorageProvider}.
     * <p>
     * File-based sources expand a glob such as {@code s3://bucket/logs-*.parquet} by listing storage and
     * matching names. API-based connectors to systems that resolve their own patterns (for example an
     * Elasticsearch index pattern like {@code logs*}, which the remote cluster expands) must return
     * {@code true} so the resolver passes the pattern through to {@link #resolveMetadata} as a single
     * source instead of attempting (unsupported) directory listing.
     */
    default boolean expandsPatternRemotely() {
        return false;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    default SplitProvider splitProvider() {
        return SplitProvider.SINGLE;
    }
}
