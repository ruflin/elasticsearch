/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND;
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
public class ElasticsearchNamedDataSourceLiveIT extends AbstractElasticsearchLiveIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    public void testNamedDataSourceMatchesDirect() throws Exception {
        assumeTrue("requires a configured remote (tests.esql.remote.url)", URL != null);
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires dataset-in-from-command capability", DATASET_IN_FROM_COMMAND.isEnabled());

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
                () -> client().execute(
                    DeleteDatasetAction.INSTANCE,
                    new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { datasetName })
                ).actionGet(TIMEOUT)
            );
            safeDelete(
                () -> client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { dataSourceName })
                ).actionGet(TIMEOUT)
            );
        }
    }

    private void safeDelete(Runnable delete) {
        try {
            delete.run();
        } catch (Exception e) {
            logger.warn("Failed to delete test resource during cleanup (ignored)", e);
        }
    }

    private List<List<Object>> runDataset(String query) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            return collectRows(response);
        }
    }
}
