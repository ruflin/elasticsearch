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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
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
     * Parses the {@code columns} array from a remote {@code _query} response, appending each column
     * descriptor to {@code out}. The parser must be positioned at the {@code START_ARRAY} token for
     * the columns array on entry; it will be positioned past the matching {@code END_ARRAY} on exit.
     * <p>
     * Shared between schema-resolution (which issues a {@code LIMIT 0} probe and only reads columns)
     * and response-parsing (which reads both columns and values in a single streaming pass).
     */
    static void parseColumns(XContentParser parser, List<RemoteColumn> out) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            String name = null;
            String type = null;
            while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String field = parser.currentName();
                parser.nextToken();
                if ("name".equals(field)) {
                    name = parser.text();
                } else if ("type".equals(field)) {
                    type = parser.text();
                } else {
                    parser.skipChildren();
                }
            }
            out.add(new RemoteColumn(name, type));
        }
    }

    /**
     * Converts one column of a columnar {@code _query} response into an ES|QL {@link Block}.
     * {@code values} holds {@code rowCount} entries (one per row), each already decoded from JSON
     * ({@link String}, {@link Number}, {@link Boolean}, or {@code null}).
     */
    static Block toBlock(DataType dataType, List<Object> values, int rowCount, BlockFactory blockFactory) {
        return switch (dataType) {
            case KEYWORD, TEXT, IP, VERSION -> buildBytesRef(values, rowCount, blockFactory);
            case LONG -> buildLong(values, rowCount, blockFactory, TemporalKind.NONE);
            case DATETIME -> buildLong(values, rowCount, blockFactory, TemporalKind.MILLIS);
            case DATE_NANOS -> buildLong(values, rowCount, blockFactory, TemporalKind.NANOS);
            case INTEGER -> buildInt(values, rowCount, blockFactory);
            case DOUBLE -> buildDouble(values, rowCount, blockFactory);
            case BOOLEAN -> buildBoolean(values, rowCount, blockFactory);
            // Columns the remote reports as unsupported (e.g. flattened) or null carry no decodable values;
            // surface them as an all-null column, mirroring local ES|QL where an unsupported field reads as null
            // unless operated on. This keeps the rest of the row usable instead of failing the whole query.
            case UNSUPPORTED, NULL -> blockFactory.newConstantNullBlock(rowCount);
            // Spatial types and unsigned_long are valid remote column types but not yet decoded here;
            // surface as null so queries that project these columns don't throw at runtime.
            case UNSIGNED_LONG, GEO_POINT, GEO_SHAPE, CARTESIAN_POINT, CARTESIAN_SHAPE,
                GEOHEX, GEOHASH, GEOTILE -> blockFactory.newConstantNullBlock(rowCount);
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

    /** How a long-backed column should interpret string values from the remote JSON. */
    private enum TemporalKind {
        NONE,
        MILLIS,
        NANOS
    }

    private static Block buildLong(List<Object> values, int rowCount, BlockFactory blockFactory, TemporalKind temporal) {
        try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                Object value = valueAt(values, i);
                if (value == null) {
                    builder.appendNull();
                } else if (value instanceof Number n) {
                    builder.appendLong(n.longValue());
                } else {
                    // Remote ES|QL JSON renders dates as ISO-8601 strings; plain longs as numeric strings.
                    builder.appendLong(switch (temporal) {
                        case NONE -> Long.parseLong(value.toString());
                        case MILLIS -> parseEpoch(value.toString(), false);
                        case NANOS -> parseEpoch(value.toString(), true);
                    });
                }
            }
            return builder.build();
        }
    }

    private static long parseEpoch(String value, boolean nanos) {
        // Remote ES|QL renders date / date_nanos columns as ISO-8601 strings, so that is the expected form
        // and is tried first: a malformed temporal value then surfaces as a DateTimeParseException rather
        // than being silently swallowed. A bare numeric string (epoch millis/nanos) is the fallback — these
        // never parse as ISO-8601, so the fallback is unambiguous.
        try {
            Instant instant = Instant.parse(value);
            return nanos ? instant.getEpochSecond() * 1_000_000_000L + instant.getNano() : instant.toEpochMilli();
        } catch (DateTimeParseException e) {
            return Long.parseLong(value);
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
