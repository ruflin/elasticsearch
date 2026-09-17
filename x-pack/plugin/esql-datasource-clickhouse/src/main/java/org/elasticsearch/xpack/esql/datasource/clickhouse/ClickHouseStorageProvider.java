/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Minimal {@link StorageProvider} for {@code clickhouse://} and {@code clickhouse+https://} locations.
 *
 * <p>A ClickHouse table is not a byte-addressable blob store; ES|QL reads from it through the
 * {@link org.elasticsearch.xpack.esql.datasources.spi.Connector} ({@link ClickHouseConnector}). This
 * provider exists only so {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver} can
 * register a concrete file-list entry and satisfy the storage SPI contract: the resolver always looks
 * up a storage provider for the location's scheme even when the actual read goes through the connector.
 * Mirrors {@code ElasticsearchStorageProvider} in the {@code esql-datasource-elasticsearch} plugin.
 */
public final class ClickHouseStorageProvider implements StorageProvider {

    @Override
    public StorageObject newObject(StoragePath path) {
        validateScheme(path);
        return new ClickHouseStorageObject(path, 0L, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateScheme(path);
        return new ClickHouseStorageObject(path, length, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new ClickHouseStorageObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
        throw new UnsupportedOperationException("ClickHouse external sources do not support directory listing");
    }

    @Override
    public boolean exists(StoragePath path) {
        validateScheme(path);
        // A ClickHouse table is not a blob whose presence can be cheaply probed without a network
        // round-trip, so we report "exists" and let the connector be the single source of truth:
        // schema resolution and query execution both hit ClickHouse over HTTP and fail with a clear
        // error if the database/table is absent.
        return true;
    }

    @Override
    public boolean supportsStableMetadata() {
        // No reliable mtime over HTTP; skip schema caching keyed on mtime.
        return false;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.copyOf(ClickHouseDataSourcePlugin.SCHEMES);
    }

    @Override
    public void close() {}

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (ClickHouseDataSourcePlugin.SCHEMES.contains(scheme) == false) {
            throw new IllegalArgumentException(
                "ClickHouseStorageProvider only supports " + ClickHouseDataSourcePlugin.SCHEMES + " schemes, got: " + scheme
            );
        }
    }

    private record ClickHouseStorageObject(StoragePath path, long knownLength, Instant knownLastModified) implements StorageObject {

        @Override
        public InputStream newStream() throws IOException {
            throw notByteAddressable();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            throw notByteAddressable();
        }

        private static IOException notByteAddressable() {
            return new IOException("ClickHouse external sources are read via the connector, not as byte streams");
        }

        @Override
        public long length() {
            return knownLength;
        }

        @Override
        public Instant lastModified() {
            return knownLastModified;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
