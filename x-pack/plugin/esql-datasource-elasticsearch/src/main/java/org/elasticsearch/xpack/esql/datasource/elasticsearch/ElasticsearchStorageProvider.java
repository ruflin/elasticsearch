/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

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
 * Minimal {@link StorageProvider} for {@code es://} and {@code elasticsearch://} locations.
 * <p>
 * A remote Elasticsearch cluster is not a byte-addressable blob store; ES|QL reads from it through the
 * {@link org.elasticsearch.xpack.esql.datasources.spi.Connector} ({@link ElasticsearchConnector}).
 * This provider exists only so {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver}
 * can register a concrete file-list entry and satisfy the storage SPI contract.
 */
public final class ElasticsearchStorageProvider implements StorageProvider {

    @Override
    public StorageObject newObject(StoragePath path) {
        validateScheme(path);
        return new ElasticsearchStorageObject(path, 0L, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateScheme(path);
        return new ElasticsearchStorageObject(path, length, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new ElasticsearchStorageObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
        throw new UnsupportedOperationException("Elasticsearch external sources do not support directory listing");
    }

    @Override
    public boolean exists(StoragePath path) {
        validateScheme(path);
        // Existence is verified by the connector when it resolves the schema / runs the query.
        return true;
    }

    @Override
    public boolean supportsStableMetadata() {
        // No reliable mtime over HTTP; skip schema caching keyed on mtime.
        return false;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.copyOf(ElasticsearchDataSourcePlugin.SCHEMES);
    }

    @Override
    public void close() {}

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (ElasticsearchDataSourcePlugin.SCHEMES.contains(scheme) == false) {
            throw new IllegalArgumentException(
                "ElasticsearchStorageProvider only supports " + ElasticsearchDataSourcePlugin.SCHEMES + " schemes, got: " + scheme
            );
        }
    }

    private record ElasticsearchStorageObject(StoragePath path, long knownLength, Instant knownLastModified) implements StorageObject {

        @Override
        public InputStream newStream() throws IOException {
            throw notByteAddressable();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            throw notByteAddressable();
        }

        private static IOException notByteAddressable() {
            return new IOException("Elasticsearch external sources are read via the connector, not as byte streams");
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
