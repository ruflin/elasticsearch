/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps remote Elasticsearch types to ES|QL types and converts remote {@code _query} column values
 * into ES|QL {@link Block}s.
 * <p>
 * v1 supports the common scalar types. Unsupported types resolve to {@link DataType#UNSUPPORTED};
 * referencing such a column in a query fails the same way it would for a local index, while leaving
 * other columns usable (mirrors local ES|QL behaviour).
 */
final class EsqlTypeMapping {

    private EsqlTypeMapping() {}

    /**
     * Builds an ES|QL attribute for a remote {@code _query} column. {@code esqlType} is the type
     * name as reported by the remote {@code _query} response (e.g. {@code keyword}, {@code long}).
     * Columns are marked nullable because a remote ES|QL result column can contain nulls.
     */
    static Attribute toAttribute(String name, String esqlType) {
        DataType dataType = DataType.fromNameOrAlias(esqlType);
        return new ReferenceAttribute(Source.EMPTY, null, name, dataType, Nullability.TRUE, null, false);
    }

    /**
     * Builds the schema for a remote {@code _query} response from its {@code columns} array.
     */
    static List<Attribute> toAttributes(List<RemoteColumn> columns) {
        List<Attribute> attributes = new ArrayList<>(columns.size());
        for (RemoteColumn column : columns) {
            attributes.add(toAttribute(column.name(), column.type()));
        }
        return attributes;
    }

    /** A column descriptor from a remote {@code _query} response. */
    record RemoteColumn(String name, String type) {}

    /**
     * Converts one column of a columnar {@code _query} response into an ES|QL {@link Block}.
     * {@code values} holds {@code rowCount} entries (one per row), each already decoded from JSON
     * ({@link String}, {@link Number}, {@link Boolean}, or {@code null}).
     */
    static Block toBlock(DataType dataType, List<Object> values, int rowCount, BlockFactory blockFactory) {
        return switch (dataType) {
            case KEYWORD, TEXT, IP, VERSION -> buildBytesRef(values, rowCount, blockFactory);
            case LONG -> buildLong(values, rowCount, blockFactory, false);
            case DATETIME -> buildLong(values, rowCount, blockFactory, true);
            case INTEGER -> buildInt(values, rowCount, blockFactory);
            case DOUBLE -> buildDouble(values, rowCount, blockFactory);
            case BOOLEAN -> buildBoolean(values, rowCount, blockFactory);
            default -> throw new IllegalArgumentException(
                "Unsupported remote Elasticsearch column type for value decoding: " + dataType.typeName()
            );
        };
    }

    /**
     * Reads the value for {@code row}, treating positions beyond the column length as {@code null}.
     * A columnar {@code _query} response is expected to be rectangular, but tolerating a ragged
     * column avoids an {@link IndexOutOfBoundsException} on a malformed response.
     */
    private static Object valueAt(List<Object> values, int row) {
        return row < values.size() ? values.get(row) : null;
    }

    private static Block buildBytesRef(List<Object> values, int rowCount, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                Object value = valueAt(values, i);
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(value.toString()));
                }
            }
            return builder.build();
        }
    }

    private static Block buildLong(List<Object> values, int rowCount, BlockFactory blockFactory, boolean datetime) {
        try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                Object value = valueAt(values, i);
                if (value == null) {
                    builder.appendNull();
                } else if (value instanceof Number n) {
                    builder.appendLong(n.longValue());
                } else if (datetime) {
                    // Remote ES|QL JSON renders datetimes as ISO-8601 strings; store epoch millis.
                    builder.appendLong(parseDateMillis(value.toString()));
                } else {
                    builder.appendLong(Long.parseLong(value.toString()));
                }
            }
            return builder.build();
        }
    }

    private static long parseDateMillis(String value) {
        try {
            // A numeric string is treated as epoch millis; anything else as an ISO-8601 datetime.
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return Instant.parse(value).toEpochMilli();
        }
    }

    private static Block buildInt(List<Object> values, int rowCount, BlockFactory blockFactory) {
        try (var builder = blockFactory.newIntBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                Object value = valueAt(values, i);
                if (value == null) {
                    builder.appendNull();
                } else if (value instanceof Number n) {
                    builder.appendInt(n.intValue());
                } else {
                    builder.appendInt(Integer.parseInt(value.toString()));
                }
            }
            return builder.build();
        }
    }

    private static Block buildDouble(List<Object> values, int rowCount, BlockFactory blockFactory) {
        try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                Object value = valueAt(values, i);
                if (value == null) {
                    builder.appendNull();
                } else if (value instanceof Number n) {
                    builder.appendDouble(n.doubleValue());
                } else {
                    builder.appendDouble(Double.parseDouble(value.toString()));
                }
            }
            return builder.build();
        }
    }

    private static Block buildBoolean(List<Object> values, int rowCount, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                Object value = valueAt(values, i);
                if (value == null) {
                    builder.appendNull();
                } else if (value instanceof Boolean b) {
                    builder.appendBoolean(b);
                } else {
                    builder.appendBoolean(Boolean.parseBoolean(value.toString()));
                }
            }
            return builder.build();
        }
    }
}
