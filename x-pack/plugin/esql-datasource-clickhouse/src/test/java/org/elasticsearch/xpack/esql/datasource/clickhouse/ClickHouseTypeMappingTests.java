/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

public class ClickHouseTypeMappingTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    // -------- dataTypeFor ----------

    public void testInt8MapsToInteger() {
        assertEquals(DataType.INTEGER, ClickHouseTypeMapping.dataTypeFor("Int8"));
    }

    public void testInt16MapsToInteger() {
        assertEquals(DataType.INTEGER, ClickHouseTypeMapping.dataTypeFor("Int16"));
    }

    public void testInt32MapsToInteger() {
        assertEquals(DataType.INTEGER, ClickHouseTypeMapping.dataTypeFor("Int32"));
    }

    public void testUInt8MapsToInteger() {
        assertEquals(DataType.INTEGER, ClickHouseTypeMapping.dataTypeFor("UInt8"));
    }

    public void testUInt16MapsToInteger() {
        assertEquals(DataType.INTEGER, ClickHouseTypeMapping.dataTypeFor("UInt16"));
    }

    public void testInt64MapsToLong() {
        assertEquals(DataType.LONG, ClickHouseTypeMapping.dataTypeFor("Int64"));
    }

    public void testUInt32MapsToLong() {
        assertEquals(DataType.LONG, ClickHouseTypeMapping.dataTypeFor("UInt32"));
    }

    public void testUInt64MapsToLong() {
        assertEquals(DataType.LONG, ClickHouseTypeMapping.dataTypeFor("UInt64"));
    }

    public void testFloat32MapsToDouble() {
        assertEquals(DataType.DOUBLE, ClickHouseTypeMapping.dataTypeFor("Float32"));
    }

    public void testFloat64MapsToDouble() {
        assertEquals(DataType.DOUBLE, ClickHouseTypeMapping.dataTypeFor("Float64"));
    }

    public void testStringMapsToKeyword() {
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("String"));
    }

    public void testFixedStringMapsToKeyword() {
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("FixedString(16)"));
    }

    public void testBoolMapsToBoolean() {
        assertEquals(DataType.BOOLEAN, ClickHouseTypeMapping.dataTypeFor("Bool"));
    }

    public void testNullableStrippedBeforeMapping() {
        assertEquals(DataType.INTEGER, ClickHouseTypeMapping.dataTypeFor("Nullable(Int32)"));
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("Nullable(String)"));
        assertEquals(DataType.BOOLEAN, ClickHouseTypeMapping.dataTypeFor("Nullable(Bool)"));
    }

    public void testLowCardinalityStrippedBeforeMapping() {
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("LowCardinality(String)"));
    }

    public void testDateTypeMapsToKeyword() {
        // v1: Date/DateTime returned as ISO strings
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("Date"));
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("DateTime"));
        assertEquals(DataType.KEYWORD, ClickHouseTypeMapping.dataTypeFor("DateTime64(3)"));
    }

    // -------- isNullable ----------

    public void testIsNullableTrue() {
        assertTrue(ClickHouseTypeMapping.isNullable("Nullable(Int32)"));
        assertTrue(ClickHouseTypeMapping.isNullable("Nullable(String)"));
    }

    public void testIsNullableFalse() {
        assertFalse(ClickHouseTypeMapping.isNullable("Int32"));
        assertFalse(ClickHouseTypeMapping.isNullable("String"));
        assertFalse(ClickHouseTypeMapping.isNullable("LowCardinality(String)"));
    }

    // -------- appendValue ----------

    public void testAppendIntegerValue() throws IOException {
        try (Block.Builder builder = ClickHouseTypeMapping.newBuilder(DataType.INTEGER, 4, blockFactory)) {
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.INTEGER), "42");
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.INTEGER), "-7");
            try (IntBlock block = (IntBlock) builder.build()) {
                assertEquals(2, block.getPositionCount());
                assertEquals(42, block.getInt(0));
                assertEquals(-7, block.getInt(1));
            }
        }
    }

    public void testAppendLongValue() throws IOException {
        try (Block.Builder builder = ClickHouseTypeMapping.newBuilder(DataType.LONG, 2, blockFactory)) {
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.LONG), "9999999999");
            try (LongBlock block = (LongBlock) builder.build()) {
                assertEquals(1, block.getPositionCount());
                assertEquals(9999999999L, block.getLong(0));
            }
        }
    }

    public void testAppendDoubleValue() throws IOException {
        try (Block.Builder builder = ClickHouseTypeMapping.newBuilder(DataType.DOUBLE, 2, blockFactory)) {
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.DOUBLE), "3.14");
            try (DoubleBlock block = (DoubleBlock) builder.build()) {
                assertEquals(1, block.getPositionCount());
                assertEquals(3.14, block.getDouble(0), 1e-9);
            }
        }
    }

    public void testAppendBooleanValues() throws IOException {
        try (Block.Builder builder = ClickHouseTypeMapping.newBuilder(DataType.BOOLEAN, 2, blockFactory)) {
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.BOOLEAN), "true");
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.BOOLEAN), "false");
            try (BooleanBlock block = (BooleanBlock) builder.build()) {
                assertEquals(2, block.getPositionCount());
                assertTrue(block.getBoolean(0));
                assertFalse(block.getBoolean(1));
            }
        }
    }

    public void testAppendKeywordValue() throws IOException {
        try (Block.Builder builder = ClickHouseTypeMapping.newBuilder(DataType.KEYWORD, 2, blockFactory)) {
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.KEYWORD), "\"hello\"");
            try (BytesRefBlock block = (BytesRefBlock) builder.build()) {
                assertEquals(1, block.getPositionCount());
                assertEquals(new BytesRef("hello"), block.getBytesRef(0, new BytesRef()));
            }
        }
    }

    public void testAppendNullValue() throws IOException {
        try (Block.Builder builder = ClickHouseTypeMapping.newBuilder(DataType.INTEGER, 2, blockFactory)) {
            appendJson(parser -> ClickHouseTypeMapping.appendValue(parser, builder, DataType.INTEGER), "null");
            try (Block block = builder.build()) {
                assertEquals(1, block.getPositionCount());
                assertTrue(block.isNull(0));
            }
        }
    }

    // -------- helpers ----------

    @FunctionalInterface
    interface ParserAction {
        void apply(JsonParser parser) throws IOException;
    }

    private static void appendJson(ParserAction action, String json) throws IOException {
        try (JsonParser parser = new JsonFactory().createParser(json)) {
            parser.nextToken(); // position at first token
            action.apply(parser);
        }
    }
}
