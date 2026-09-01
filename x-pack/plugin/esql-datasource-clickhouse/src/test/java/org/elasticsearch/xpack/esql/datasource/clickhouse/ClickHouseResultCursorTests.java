/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests parsing of ClickHouse {@code FORMAT JSONCompactColumns} responses into ES|QL Pages.
 */
public class ClickHouseResultCursorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testParseSingleIntColumn() throws IOException {
        // JSONCompactColumns: [[1, 2, 3]]
        String json = "[[1,2,3]]";
        List<Attribute> attrs = List.of(attr("val", DataType.INTEGER));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            assertTrue(cursor.hasNext());
            Page page = cursor.next();
            try {
                assertEquals(3, page.getPositionCount());
                IntBlock block = (IntBlock) page.getBlock(0);
                assertEquals(1, block.getInt(0));
                assertEquals(2, block.getInt(1));
                assertEquals(3, block.getInt(2));
            } finally {
                page.releaseBlocks();
            }
            assertFalse(cursor.hasNext());
        }
    }

    public void testParseMultipleColumns() throws IOException {
        // Two columns: int and keyword
        String json = "[[10,20],[\"alice\",\"bob\"]]";
        List<Attribute> attrs = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            assertTrue(cursor.hasNext());
            Page page = cursor.next();
            try {
                assertEquals(2, page.getPositionCount());
                IntBlock ids = (IntBlock) page.getBlock(0);
                BytesRefBlock names = (BytesRefBlock) page.getBlock(1);
                assertEquals(10, ids.getInt(0));
                assertEquals(20, ids.getInt(1));
                assertEquals(new BytesRef("alice"), names.getBytesRef(0, new BytesRef()));
                assertEquals(new BytesRef("bob"), names.getBytesRef(1, new BytesRef()));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testParseLongColumn() throws IOException {
        String json = "[[9999999999,-1]]";
        List<Attribute> attrs = List.of(attr("big", DataType.LONG));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            Page page = cursor.next();
            try {
                LongBlock block = (LongBlock) page.getBlock(0);
                assertEquals(9999999999L, block.getLong(0));
                assertEquals(-1L, block.getLong(1));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testParseDoubleColumn() throws IOException {
        String json = "[[1.5,2.7]]";
        List<Attribute> attrs = List.of(attr("score", DataType.DOUBLE));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            Page page = cursor.next();
            try {
                DoubleBlock block = (DoubleBlock) page.getBlock(0);
                assertEquals(1.5, block.getDouble(0), 1e-9);
                assertEquals(2.7, block.getDouble(1), 1e-9);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testParseBooleanColumn() throws IOException {
        String json = "[[true,false,true]]";
        List<Attribute> attrs = List.of(attr("flag", DataType.BOOLEAN));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            Page page = cursor.next();
            try {
                BooleanBlock block = (BooleanBlock) page.getBlock(0);
                assertTrue(block.getBoolean(0));
                assertFalse(block.getBoolean(1));
                assertTrue(block.getBoolean(2));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testParseNullableColumn() throws IOException {
        // Nullable(Int32): some values are null
        String json = "[[1,null,3]]";
        List<Attribute> attrs = List.of(attr("val", DataType.INTEGER));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            Page page = cursor.next();
            try {
                IntBlock block = (IntBlock) page.getBlock(0);
                assertEquals(3, block.getPositionCount());
                assertEquals(1, block.getInt(0));
                assertTrue(block.isNull(1));
                assertEquals(3, block.getInt(2));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testEmptyResultReturnsNoPage() throws IOException {
        // Empty result: all columns are empty arrays
        String json = "[[],[]]";
        List<Attribute> attrs = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            assertFalse(cursor.hasNext());
        }
    }

    public void testKeywordWithSpecialCharacters() throws IOException {
        String json = "[[\"hello world\",\"caf\\u00e9\"]]";
        List<Attribute> attrs = List.of(attr("name", DataType.KEYWORD));
        try (ClickHouseResultCursor cursor = parse(json, attrs)) {
            Page page = cursor.next();
            try {
                BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
                assertEquals(new BytesRef("hello world"), block.getBytesRef(0, new BytesRef()));
                assertEquals(new BytesRef("café"), block.getBytesRef(1, new BytesRef()));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    // -------- helpers ----------

    private ClickHouseResultCursor parse(String json, List<Attribute> attrs) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new ClickHouseResultCursor(new ByteArrayInputStream(bytes), attrs, blockFactory);
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, false);
    }
}
