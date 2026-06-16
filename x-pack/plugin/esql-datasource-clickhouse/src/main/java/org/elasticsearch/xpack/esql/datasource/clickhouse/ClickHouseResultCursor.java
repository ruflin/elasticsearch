/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Parses a ClickHouse {@code FORMAT JSONCompactColumns} HTTP response into ES|QL {@link Page}s.
 *
 * <p>The response format is a JSON array of column arrays:
 * {@code [[col1_row1, col1_row2, ...], [col2_row1, col2_row2, ...], ...]}
 *
 * <p>All data is read eagerly in a single pass and returned as one {@link Page} containing
 * one {@link Block} per column. Nulls in the JSON are mapped to null entries in the block.
 */
class ClickHouseResultCursor implements ResultCursor {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private Page page;

    ClickHouseResultCursor(InputStream responseBody, List<Attribute> attributes, BlockFactory blockFactory) throws IOException {
        this.page = parseResponse(responseBody, attributes, blockFactory);
    }

    @Override
    public boolean hasNext() {
        return page != null;
    }

    @Override
    public Page next() {
        Page result = page;
        page = null; // transfer ownership to caller; caller must call page.releaseBlocks()
        return result;
    }

    @Override
    public void close() throws IOException {
        if (page != null) {
            page.releaseBlocks();
            page = null;
        }
    }

    private static Page parseResponse(InputStream responseBody, List<Attribute> attributes, BlockFactory blockFactory) throws IOException {
        int colCount = attributes.size();
        if (colCount == 0) {
            return null;
        }

        Block.Builder[] builders = new Block.Builder[colCount];
        try {
            for (int i = 0; i < colCount; i++) {
                DataType dt = attributes.get(i).dataType();
                builders[i] = ClickHouseTypeMapping.newBuilder(dt, 64, blockFactory);
            }

            try (JsonParser parser = JSON_FACTORY.createParser(responseBody)) {
                JsonToken token = parser.nextToken();
                if (token != JsonToken.START_ARRAY) {
                    throw new IOException("Expected JSON array from ClickHouse, got: " + token);
                }

                int colIdx = 0;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    // We are at START_ARRAY for column colIdx
                    if (colIdx >= colCount) {
                        // More columns in response than expected; skip them
                        parser.skipChildren();
                        colIdx++;
                        continue;
                    }

                    DataType dt = attributes.get(colIdx).dataType();
                    Block.Builder builder = builders[colIdx];

                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        ClickHouseTypeMapping.appendValue(parser, builder, dt);
                    }
                    colIdx++;
                }
            }

            Block[] blocks = new Block[colCount];
            int rowCount = -1;
            for (int i = 0; i < colCount; i++) {
                blocks[i] = builders[i].build();
                builders[i] = null;
                int thisCount = blocks[i].getPositionCount();
                if (rowCount == -1) {
                    rowCount = thisCount;
                } else if (thisCount != rowCount) {
                    throw new IOException(
                        "Column " + i + " has " + thisCount + " rows but expected " + rowCount + " (ClickHouse response inconsistency)"
                    );
                }
            }
            if (rowCount <= 0) {
                for (Block b : blocks) {
                    b.close();
                }
                return null;
            }
            return new Page(rowCount, blocks);
        } catch (Exception e) {
            // Release all builders and any built blocks on failure
            for (Block.Builder b : builders) {
                if (b != null) {
                    b.close();
                }
            }
            throw e;
        }
    }
}
