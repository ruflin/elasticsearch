/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.elasticsearch.ElasticsearchDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Shared infrastructure for live end-to-end tests that talk to a real remote Elasticsearch cluster.
 * <p>
 * The remote endpoint and API key are supplied via system properties so no secret is committed:
 * <pre>
 *   -Dtests.esql.remote.url=https://&lt;host&gt;
 *   -Dtests.esql.remote.apikey=&lt;base64 api key&gt;
 *   [-Dtests.esql.remote.target=logs*]
 * </pre>
 * Tests self-skip when {@code tests.esql.remote.url} is not set.
 */
abstract class AbstractElasticsearchLiveIT extends AbstractEsqlIntegTestCase {

    protected static final String URL = System.getProperty("tests.esql.remote.url");
    protected static final String API_KEY = System.getProperty("tests.esql.remote.apikey");
    protected static final String TARGET = System.getProperty("tests.esql.remote.target", "logs*");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ElasticsearchDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    /**
     * Runs {@code esql} straight against the remote {@code _query} API with the configured API key and
     * returns the result rows. The caller owns decoding of cell values.
     */
    @SuppressWarnings("unchecked")
    protected List<List<Object>> directValues(String esql) throws IOException {
        var builder = RestClient.builder(HttpHost.create(URL));
        if (API_KEY != null) {
            builder.setDefaultHeaders(new org.apache.http.Header[] { new BasicHeader("Authorization", "ApiKey " + API_KEY) });
        }
        try (RestClient client = builder.build()) {
            Request request = new Request("POST", "/_query");
            request.addParameter("format", "json");
            request.setJsonEntity(Strings.format("{\"query\":%s}", quote(esql)));
            Response response = client.performRequest(request);
            try (
                InputStream content = response.getEntity().getContent();
                XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
            ) {
                Map<String, Object> body = parser.map();
                List<List<Object>> rows = new ArrayList<>();
                for (Object rowObj : (List<Object>) body.getOrDefault("values", List.of())) {
                    rows.add((List<Object>) rowObj);
                }
                return rows;
            }
        }
    }

    /** Collects all rows from a local {@link EsqlQueryResponse} into a list. */
    protected static List<List<Object>> collectRows(EsqlQueryResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        for (Iterator<Iterator<Object>> it = response.values(); it.hasNext();) {
            List<Object> row = new ArrayList<>();
            it.next().forEachRemaining(row::add);
            rows.add(row);
        }
        return rows;
    }

    /** JSON-escapes and double-quotes a string so it can be embedded in a JSON body. */
    protected static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
