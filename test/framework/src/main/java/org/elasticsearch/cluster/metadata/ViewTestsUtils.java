/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.core.Tuple;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomMap;

public class ViewTestsUtils {
    public static String randomName() {
        return randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    public static View randomView(String name) {
        String query = "FROM " + randomAlphaOfLength(10);
        String description = randomBoolean() ? randomAlphaOfLengthBetween(1, 40) : null;
        boolean managed = randomBoolean();
        Map<String, Object> metadata = randomBoolean()
            ? randomMap(1, 3, () -> Tuple.tuple(randomAlphaOfLength(5), randomAlphaOfLength(8)))
            : null;
        return new View(name, query, description, managed, metadata);
    }
}
