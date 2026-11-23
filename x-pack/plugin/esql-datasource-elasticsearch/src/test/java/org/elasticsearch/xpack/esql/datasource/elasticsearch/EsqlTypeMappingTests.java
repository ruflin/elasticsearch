/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class EsqlTypeMappingTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testToAttributeMapsCommonTypes() {
        assertEquals(DataType.KEYWORD, EsqlTypeMapping.toAttribute("f", "keyword").dataType());
        assertEquals(DataType.LONG, EsqlTypeMapping.toAttribute("f", "long").dataType());
        assertEquals(DataType.INTEGER, EsqlTypeMapping.toAttribute("f", "integer").dataType());
        assertEquals(DataType.DOUBLE, EsqlTypeMapping.toAttribute("f", "double").dataType());
        assertEquals(DataType.BOOLEAN, EsqlTypeMapping.toAttribute("f", "boolean").dataType());
    }

    public void testToAttributesPreservesOrder() {
        List<Attribute> attributes = EsqlTypeMapping.toAttributes(
            List.of(new EsqlTypeMapping.RemoteColumn("name", "keyword"), new EsqlTypeMapping.RemoteColumn("age", "long"))
        );
        assertEquals(2, attributes.size());
        assertEquals("name", attributes.get(0).name());
        assertEquals(DataType.KEYWORD, attributes.get(0).dataType());
        assertEquals("age", attributes.get(1).name());
        assertEquals(DataType.LONG, attributes.get(1).dataType());
    }

    public void testKeywordBlock() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.KEYWORD, Arrays.asList("a", null, "c"), 3, blockFactory)) {
            assertThat(block, instanceOf(BytesRefBlock.class));
            BytesRefBlock b = (BytesRefBlock) block;
            assertEquals(new BytesRef("a"), b.getBytesRef(0, new BytesRef()));
            assertTrue(b.isNull(1));
            assertEquals(new BytesRef("c"), b.getBytesRef(2, new BytesRef()));
        }
    }

    public void testLongBlock() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.LONG, Arrays.asList(1L, 2, null), 3, blockFactory)) {
            assertThat(block, instanceOf(LongBlock.class));
            LongBlock b = (LongBlock) block;
            assertEquals(1L, b.getLong(0));
            assertEquals(2L, b.getLong(1));
            assertTrue(b.isNull(2));
        }
    }

    public void testIntBlock() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.INTEGER, Arrays.asList(10, 20, 30), 3, blockFactory)) {
            assertThat(block, instanceOf(IntBlock.class));
            IntBlock b = (IntBlock) block;
            assertEquals(10, b.getInt(0));
            assertEquals(20, b.getInt(1));
            assertEquals(30, b.getInt(2));
        }
    }

    public void testDoubleBlock() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.DOUBLE, Arrays.asList(1.5, 2.5), 2, blockFactory)) {
            assertThat(block, instanceOf(DoubleBlock.class));
            DoubleBlock b = (DoubleBlock) block;
            assertEquals(1.5, b.getDouble(0), 0.0001);
            assertEquals(2.5, b.getDouble(1), 0.0001);
        }
    }

    public void testBooleanBlock() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.BOOLEAN, Arrays.asList(true, false, null), 3, blockFactory)) {
            assertThat(block, instanceOf(BooleanBlock.class));
            BooleanBlock b = (BooleanBlock) block;
            assertTrue(b.getBoolean(0));
            assertFalse(b.getBoolean(1));
            assertTrue(b.isNull(2));
        }
    }

    public void testDatetimeBlockFromEpochMillis() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.DATETIME, Arrays.asList(1700000000000L, null), 2, blockFactory)) {
            assertThat(block, instanceOf(LongBlock.class));
            LongBlock b = (LongBlock) block;
            assertEquals(1700000000000L, b.getLong(0));
            assertTrue(b.isNull(1));
        }
    }

    public void testDatetimeBlockFromIso8601String() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.DATETIME, List.of("2023-11-14T22:13:20.000Z"), 1, blockFactory)) {
            assertThat(block, instanceOf(LongBlock.class));
            assertEquals(1700000000000L, ((LongBlock) block).getLong(0));
        }
    }

    public void testDatetimeBlockFromNumericEpochString() {
        // A bare numeric string is epoch millis and must still decode after ISO-8601 parsing fails.
        try (Block block = EsqlTypeMapping.toBlock(DataType.DATETIME, List.of("1700000000000"), 1, blockFactory)) {
            assertThat(block, instanceOf(LongBlock.class));
            assertEquals(1700000000000L, ((LongBlock) block).getLong(0));
        }
    }

    public void testDateNanosBlockFromIso8601String() {
        try (Block block = EsqlTypeMapping.toBlock(DataType.DATE_NANOS, List.of("2023-11-14T22:13:20.000000123Z"), 1, blockFactory)) {
            assertThat(block, instanceOf(LongBlock.class));
            assertEquals(1700000000000000123L, ((LongBlock) block).getLong(0));
        }
    }

    public void testRaggedColumnFillsMissingPositionsWithNull() {
        // rowCount (3) exceeds this column's length (1); missing positions must decode to null, not throw.
        try (Block block = EsqlTypeMapping.toBlock(DataType.KEYWORD, List.of("a"), 3, blockFactory)) {
            assertThat(block, instanceOf(BytesRefBlock.class));
            BytesRefBlock b = (BytesRefBlock) block;
            assertEquals(new BytesRef("a"), b.getBytesRef(0, new BytesRef()));
            assertTrue(b.isNull(1));
            assertTrue(b.isNull(2));
        }
    }

    public void testUnsupportedColumnTypeDecodesAsNullBlock() {
        // A remote column the cluster reports as `unsupported` (e.g. flattened) must not fail the query;
        // it surfaces as an all-null column so the rest of the row stays usable.
        try (Block block = EsqlTypeMapping.toBlock(DataType.UNSUPPORTED, List.of(), 3, blockFactory)) {
            assertTrue(block.areAllValuesNull());
            assertEquals(3, block.getPositionCount());
        }
    }

    public void testSpatialAndUnsignedLongDecodeAsNullBlock() {
        // Spatial types and unsigned_long are valid remote column types not yet decoded;
        // they must surface as null rather than crashing queries that project these columns.
        for (DataType type : new DataType[] {
            DataType.GEO_POINT,
            DataType.GEO_SHAPE,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.GEOHEX,
            DataType.GEOHASH,
            DataType.GEOTILE,
            DataType.UNSIGNED_LONG }) {
            try (Block block = EsqlTypeMapping.toBlock(type, List.of("ignored"), 2, blockFactory)) {
                assertTrue("expected all-null block for " + type, block.areAllValuesNull());
                assertEquals(2, block.getPositionCount());
            }
        }
    }

    public void testTrulyUndecodableTypeThrows() {
        // FLOAT is never emitted by the remote ES|QL server, so an IllegalArgumentException is correct.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlTypeMapping.toBlock(DataType.FLOAT, List.of(), 0, blockFactory)
        );
        assertTrue(e.getMessage().contains("Unsupported remote Elasticsearch column type"));
    }
}
