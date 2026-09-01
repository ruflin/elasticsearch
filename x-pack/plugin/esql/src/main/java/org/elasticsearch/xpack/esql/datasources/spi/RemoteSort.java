/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * A single sort key the optimizer asked a connector to apply remotely, alongside
 * {@link QueryRequest#rowLimit()}. This is the SPI-level projection of an ESQL {@code Order}: it carries only
 * the field name, direction, and null placement a connector needs to render a remote {@code SORT}, so the
 * {@code datasources.spi} leaf module and connectors do not depend on the full ESQL expression tree.
 *
 * <p>Never serialized: connectors execute on the coordinator only, so the sort is produced and consumed in the
 * same JVM.
 *
 * @param field the column name to sort on (the source field name, already resolved to a single attribute)
 * @param ascending {@code true} for ascending order, {@code false} for descending
 * @param nullsFirst {@code true} to place nulls first, {@code false} to place them last
 */
public record RemoteSort(String field, boolean ascending, boolean nullsFirst) {

    public RemoteSort {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("RemoteSort field must not be null or empty");
        }
    }
}
