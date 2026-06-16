/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

/**
 * Maps ClickHouse type strings to ES|QL {@link DataType}s and parses JSON values
 * from a {@link JsonParser} into typed {@link Block}s.
 *
 * <p>Type coverage:
 * <ul>
 *   <li>Int8/16/32, UInt8/16 → INTEGER</li>
 *   <li>Int64, UInt32/64 → LONG</li>
 *   <li>Float32, Float64 → DOUBLE</li>
 *   <li>String, FixedString(N), LowCardinality(String) → KEYWORD</li>
 *   <li>Bool → BOOLEAN</li>
 *   <li>Nullable(T) → nullable variant of T</li>
 *   <li>Date, Date32, DateTime, DateTime64 → KEYWORD (returned as ISO strings; v1 limitation)</li>
 *   <li>All others → KEYWORD (as string fallback)</li>
 * </ul>
 */
final class ClickHouseTypeMapping {

    private ClickHouseTypeMapping() {}

    /**
     * Resolves a ClickHouse type string (as returned by DESCRIBE TABLE) to an ES|QL DataType.
     * Strips {@code Nullable(...)} and {@code LowCardinality(...)} wrappers before resolving.
     */
    static DataType dataTypeFor(String chType) {
        String t = unwrap(chType);
        return switch (t) {
            case "Int8", "Int16", "Int32", "UInt8", "UInt16" -> DataType.INTEGER;
            case "Int64", "UInt32", "UInt64" -> DataType.LONG;
            case "Float32", "Float64" -> DataType.DOUBLE;
            case "Bool" -> DataType.BOOLEAN;
            default -> {
                if (t.startsWith("FixedString(")) {
                    yield DataType.KEYWORD;
                }
                // Date/DateTime types are returned as ISO strings in JSONCompactColumns format.
                // Mapping them to KEYWORD preserves the value faithfully in v1.
                yield DataType.KEYWORD;
            }
        };
    }

    /**
     * Returns true if the raw ClickHouse type string declares nullability via {@code Nullable(...)}.
     */
    static boolean isNullable(String chType) {
        String t = chType.trim();
        return t.startsWith("Nullable(") && t.endsWith(")");
    }

    /**
     * Creates a typed block builder for the given {@link DataType}.
     * The {@code capacityHint} is a soft hint; builders grow dynamically.
     */
    static Block.Builder newBuilder(DataType dataType, int capacityHint, BlockFactory blockFactory) {
        return switch (dataType) {
            case INTEGER -> blockFactory.newIntBlockBuilder(capacityHint);
            case LONG -> blockFactory.newLongBlockBuilder(capacityHint);
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(capacityHint);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(capacityHint);
            case KEYWORD -> blockFactory.newBytesRefBlockBuilder(capacityHint);
            default -> blockFactory.newBytesRefBlockBuilder(capacityHint);
        };
    }

    /**
     * Reads the current JSON token from {@code parser} and appends the typed value to {@code builder}.
     * A JSON {@code null} token appends a null entry regardless of type.
     *
     * @param parser  positioned at a value token (not yet consumed for this row)
     * @param builder the typed block builder to append to
     * @param dataType the expected column type
     */
    static void appendValue(JsonParser parser, Block.Builder builder, DataType dataType) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NULL) {
            builder.appendNull();
            return;
        }
        switch (dataType) {
            case INTEGER -> ((IntBlock.Builder) builder).appendInt(parser.getIntValue());
            case LONG -> ((LongBlock.Builder) builder).appendLong(parser.getLongValue());
            case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(parser.getDoubleValue());
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(parser.getBooleanValue());
            default -> {
                // KEYWORD and fallback: store the JSON value as a UTF-8 string
                String text = parser.getValueAsString();
                BytesRef ref = new BytesRef(text);
                ((BytesRefBlock.Builder) builder).appendBytesRef(ref);
            }
        }
    }

    /**
     * Strips outer {@code Nullable(...)} and {@code LowCardinality(...)} wrappers from a CH type string.
     */
    private static String unwrap(String chType) {
        String t = chType.trim();
        if (t.startsWith("Nullable(") && t.endsWith(")")) {
            t = t.substring("Nullable(".length(), t.length() - 1).trim();
        }
        if (t.startsWith("LowCardinality(") && t.endsWith(")")) {
            t = t.substring("LowCardinality(".length(), t.length() - 1).trim();
        }
        return t;
    }
}
