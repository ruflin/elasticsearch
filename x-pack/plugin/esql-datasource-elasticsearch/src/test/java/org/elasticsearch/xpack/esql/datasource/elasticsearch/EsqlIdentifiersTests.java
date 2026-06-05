/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.test.ESTestCase;

public class EsqlIdentifiersTests extends ESTestCase {

    public void testQuoteWrapsInBackticks() {
        assertEquals("`message`", EsqlIdentifiers.quote("message"));
        assertEquals("`@timestamp`", EsqlIdentifiers.quote("@timestamp"));
        assertEquals("`field.with.dots`", EsqlIdentifiers.quote("field.with.dots"));
    }

    public void testQuoteEscapesEmbeddedBacktick() {
        assertEquals("`a``b`", EsqlIdentifiers.quote("a`b"));
    }

    public void testValidateTargetAcceptsIndexPatterns() {
        assertEquals("logs-*", EsqlIdentifiers.validateTarget("logs-*"));
        assertEquals("a,b,c", EsqlIdentifiers.validateTarget("a,b,c"));
        assertEquals("metrics.system", EsqlIdentifiers.validateTarget("metrics.system"));
    }

    public void testValidateTargetRejectsInjection() {
        assertThrows(IllegalArgumentException.class, () -> EsqlIdentifiers.validateTarget("logs | DROP age"));
        assertThrows(IllegalArgumentException.class, () -> EsqlIdentifiers.validateTarget("logs\" | LIMIT 0 | EVAL x=\""));
        assertThrows(IllegalArgumentException.class, () -> EsqlIdentifiers.validateTarget("logs`evil`"));
        assertThrows(IllegalArgumentException.class, () -> EsqlIdentifiers.validateTarget("logs\nmore"));
    }

    public void testValidateTargetRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> EsqlIdentifiers.validateTarget(""));
        assertThrows(IllegalArgumentException.class, () -> EsqlIdentifiers.validateTarget(null));
    }
}
