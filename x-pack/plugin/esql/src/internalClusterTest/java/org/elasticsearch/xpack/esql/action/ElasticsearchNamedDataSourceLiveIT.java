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
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.elasticsearch.ElasticsearchDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Live end-to-end test for the <em>named data source</em> path of the {@code elasticsearch} connector against a
 * <em>real</em> remote cluster. It registers an {@code elasticsearch} data source that stores the API key once
 * (encrypted into cluster state), binds it to a dataset whose {@code resource} is the remote
 * {@code es+https://host/target} URI, then runs {@code FROM <dataset>} without repeating the credential and
 * compares the result with the same query run directly against the remote {@code _query} API.
 * <p>
 * Single-node ({@code numDataNodes = 1}) like {@code FromDatasetIT}: registering a dataset mutates project
 * metadata, and the test harness's cross-node cluster-state diff-apply assertion is incompatible with adding a
 * dataset on a multi-node cluster, so the named-data-source path is validated on one node.
 * <p>
 * The remote endpoint and API key are supplied via system properties so no secret is committed:
 * <pre>
 *   -Dtests.esql.remote.url=https://&lt;host&gt;
 *   -Dtests.esql.remote.apikey=&lt;base64 api key&gt;
 *   [-Dtests.esql.remote.target=logs*]
 * </pre>
 * The test self-skips when {@code tests.esql.remote.url} is not set, so it never runs (or fails) in CI.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ElasticsearchNamedDataSourceLiveIT extends AbstractEsqlIntegTestCase {

    private static final String URL = System.getProperty("tests.esql.remote.url");
    private static final String API_KEY = System.getProperty("tests.esql.remote.apikey");
    private static final String TARGET = System.getProperty("tests.esql.remote.target", "logs*");
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ElasticsearchDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    public void testNamedDataSourceMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());

        String dataSourceName = "live_remote_ds";
        String datasetName = "live_remote_logs";
        String hostPort = URL.replaceFirst("^https?://", "");
        String resource = "es+https://" + hostPort + "/" + TARGET;

        Map<String, Object> settings = new HashMap<>();
        if (API_KEY != null) {
            settings.put("api_key", API_KEY);
        }
        try {
            assertTrue(
                "put data source acknowledged",
                client().execute(
                    PutDataSourceAction.INSTANCE,
                    new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, dataSourceName, "elasticsearch", null, settings)
                ).actionGet(TIMEOUT).isAcknowledged()
            );
            assertTrue(
                "put dataset acknowledged",
                client().execute(
                    PutDatasetAction.INSTANCE,
                    new PutDatasetAction.Request(TIMEOUT, TIMEOUT, datasetName, dataSourceName, resource, null, new HashMap<>())
                ).actionGet(TIMEOUT).isAcknowledged()
            );

            // COUNT(*) is pushed to the remote, so the dataset-backed query computes the count server-side over the
            // full data set and matches a direct aggregate exactly — proving the stored, decrypted API key reaches
            // the connector via the _datasource carrier and the dataset resource supplies endpoint + target.
            String tail = " | STATS c = COUNT(*)";
            long direct = ((Number) directValues("FROM " + TARGET + tail).get(0).get(0)).longValue();
            long viaDataset = ((Number) runDataset("FROM " + datasetName + tail).get(0).get(0)).longValue();
            assertThat("remote has a substantial number of rows", direct, greaterThan(10_000L));
            assertThat("named-data-source count matches the direct full-dataset count", viaDataset, equalTo(direct));
        } finally {
            safeDelete(
                () -> client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { datasetName }))
                    .actionGet(TIMEOUT)
            );
            safeDelete(
                () -> client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { dataSourceName })
                ).actionGet(TIMEOUT)
            );
        }
    }

    private static void safeDelete(Runnable delete) {
        try {
            delete.run();
        } catch (Exception ignored) {
            // best-effort cleanup; a missing source/dataset is fine
        }
    }

    private List<List<Object>> runDataset(String query) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = new ArrayList<>();
            for (Iterator<Iterator<Object>> it = response.values(); it.hasNext();) {
                List<Object> row = new ArrayList<>();
                it.next().forEachRemaining(row::add);
                rows.add(row);
            }
            return rows;
        }
    }

    /** Runs the query straight against the remote {@code _query} API with the configured API key. */
    @SuppressWarnings("unchecked")
    private List<List<Object>> directValues(String esql) throws IOException {
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

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
