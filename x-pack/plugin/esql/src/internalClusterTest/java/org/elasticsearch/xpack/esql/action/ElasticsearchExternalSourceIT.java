/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.datasource.elasticsearch.ElasticsearchDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.GetDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * End-to-end test that proves ES|QL can pull data from a <em>separate</em> Elasticsearch cluster over
 * HTTP via the {@code elasticsearch} external data source connector.
 * <p>
 * The local cluster (the one running the ES|QL coordinator) and a second "remote" cluster are both
 * started with real HTTP transports. Data is ingested into the remote cluster, then queried from the
 * local cluster with {@code EXTERNAL "es://<remote-http>/<index>"}.
 */
public class ElasticsearchExternalSourceIT extends AbstractEsqlIntegTestCase {

    private InternalTestCluster remoteCluster;

    @Override
    protected boolean addMockHttpTransport() {
        // Local cluster does not strictly need HTTP, but keep both clusters consistent and exercise
        // the real transport stack.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ElasticsearchDataSourcePlugin.class);
        plugins.add(TestEncryptionServicePlugin.class);
        return plugins;
    }

    @Before
    public void startRemoteCluster() throws Exception {
        remoteCluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
            true,
            true,
            1,
            1,
            "remote_cluster",
            new NodeConfigurationSource() {
                @Override
                public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                    return Settings.builder()
                        .put(ElasticsearchExternalSourceIT.this.nodeSettings(nodeOrdinal, otherSettings))
                        // Real HTTP so the connector can reach the remote cluster's _query endpoint.
                        .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                        .build();
                }

                @Override
                public Path nodeConfigPath(int nodeOrdinal) {
                    return null;
                }
            },
            0,
            "remote",
            // No MockHttpTransport here: the remote cluster must expose a real HTTP endpoint.
            List.of(
                ESIntegTestCase.TestSeedPlugin.class,
                MockTransportService.TestPlugin.class,
                getTestTransportPlugin(),
                TestEncryptionServicePlugin.class,
                EsqlPluginWithEnterpriseOrTrialLicense.class
            ),
            Function.identity(),
            TEST_ENTITLEMENTS::addEntitledNodePaths
        );
        remoteCluster.beforeTest(random());
        remoteCluster.ensureAtLeastNumDataNodes(1);
    }

    @After
    public void stopRemoteCluster() throws IOException {
        IOUtils.close(remoteCluster::close);
    }

    public void testQueryRemoteClusterOverHttp() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String remoteIndex = "remote-logs";
        indexRemoteDocs(remoteIndex);

        String location = "es://" + remoteHttpAddress() + "/" + remoteIndex;
        String query = "EXTERNAL \"" + location + "\" | KEEP name, age | SORT age";

        List<List<Object>> rows = runExternal(query);
        assertThat(rows.size(), equalTo(3));
        assertThat(rows, hasItem(List.of("alice", 30L)));
        assertThat(rows, hasItem(List.of("bob", 35L)));
        assertThat(rows, hasItem(List.of("carol", 40L)));
    }

    public void testPutDataSourceAcceptsElasticsearchType() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires dataset-in-from-command capability", DATASET_IN_FROM_COMMAND.isEnabled());

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        String name = "test_es_connector_ds";
        assertTrue(
            "put data source acknowledged",
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(timeout, timeout, name, "elasticsearch", null, Map.of("api_key", "test-api-key"))
            ).actionGet(timeout).isAcknowledged()
        );
        try {
            GetDataSourceAction.Response response = client().execute(
                GetDataSourceAction.INSTANCE,
                new GetDataSourceAction.Request(timeout, new String[] { name })
            ).actionGet(timeout);
            assertThat(response.getDataSources().size(), equalTo(1));
            assertThat(response.getDataSources().iterator().next().type(), equalTo("elasticsearch"));
        } finally {
            client().execute(DeleteDataSourceAction.INSTANCE, new DeleteDataSourceAction.Request(timeout, timeout, new String[] { name }))
                .actionGet(timeout);
        }
    }

    // The filter/limit assertions below verify end-to-end correctness with pushdown enabled. They do not by
    // themselves prove the WHERE/LIMIT was executed remotely: the local LimitExec safety net (and, when a conjunct
    // is left unpushed, the FilterExec) would make the same assertions pass anyway. That the pushed clauses reach
    // the remote HTTP request is asserted separately by the connector module's RemoteRequestCaptureTests.

    public void testFilterWithPushdownEnabledReturnsSubset() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String remoteIndex = "remote-logs-filter";
        indexRemoteDocs(remoteIndex);

        String location = "es://" + remoteHttpAddress() + "/" + remoteIndex;
        String query = "EXTERNAL \"" + location + "\" | WHERE age > 30 | KEEP name, age | SORT age";

        List<List<Object>> rows = runExternal(query);
        assertThat(rows.size(), equalTo(2));
        assertThat(rows.get(0), equalTo(List.of("bob", 35L)));
        assertThat(rows.get(1), equalTo(List.of("carol", 40L)));
    }

    public void testKeywordFilterWithPushdownEnabled() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String remoteIndex = "remote-logs-keyword";
        indexRemoteDocs(remoteIndex);

        String location = "es://" + remoteHttpAddress() + "/" + remoteIndex;
        String query = "EXTERNAL \"" + location + "\" | WHERE name == \"alice\" | KEEP name, age";

        List<List<Object>> rows = runExternal(query);
        assertThat(rows.size(), equalTo(1));
        assertThat(rows.get(0), equalTo(List.of("alice", 30L)));
    }

    public void testLimitWithPushdownEnabledCapsRows() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String remoteIndex = "remote-logs-limit";
        indexRemoteDocs(remoteIndex);

        String location = "es://" + remoteHttpAddress() + "/" + remoteIndex;
        String query = "EXTERNAL \"" + location + "\" | KEEP name, age | LIMIT 2";

        List<List<Object>> rows = runExternal(query);
        assertThat(rows.size(), equalTo(2));
    }

    public void testFilterAndLimitWithPushdownEnabled() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        String remoteIndex = "remote-logs-filter-limit";
        indexRemoteDocs(remoteIndex);

        String location = "es://" + remoteHttpAddress() + "/" + remoteIndex;
        String query = "EXTERNAL \"" + location + "\" | WHERE age >= 35 | KEEP name, age | SORT age | LIMIT 1";

        List<List<Object>> rows = runExternal(query);
        assertThat(rows.size(), equalTo(1));
        assertThat(rows.get(0), equalTo(List.of("bob", 35L)));
    }

    /**
     * Manual verification hook: start both clusters, load remote data, register a named data source,
     * prove {@code EXTERNAL} and {@code FROM <dataset>} work, then keep the clusters up.
     * Skipped unless {@code -Dtests.esql.keep_clusters=true}.
     */
    public void testKeepClustersRunningForManualQuery() throws Exception {
        assumeTrue("opt-in keep-alive for two running clusters", Booleans.parseBoolean(System.getProperty("tests.esql.keep_clusters")));
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires dataset-in-from-command capability", DATASET_IN_FROM_COMMAND.isEnabled());

        String remoteIndex = "remote-logs";
        indexRemoteDocs(remoteIndex);

        String location = "es://" + remoteHttpAddress() + "/" + remoteIndex;
        List<List<Object>> externalRows = runExternal("EXTERNAL \"" + location + "\" | KEEP name, age | SORT age");
        assertThat(externalRows.size(), equalTo(3));
        assertThat(externalRows, hasItem(List.of("alice", 30L)));

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        String dataSourceName = "remote_prod";
        String datasetName = "remote_logs";
        assertTrue(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(timeout, timeout, dataSourceName, "elasticsearch", null, Map.of())
            ).actionGet(timeout).isAcknowledged()
        );
        assertTrue(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(timeout, timeout, datasetName, dataSourceName, location, null, Map.of())
            ).actionGet(timeout).isAcknowledged()
        );
        List<List<Object>> datasetRows = runExternal("FROM " + datasetName + " | KEEP name, age | SORT age");
        assertThat(datasetRows, equalTo(externalRows));

        Path portsFile = Path.of("/tmp/esql-two-cluster-ports.txt");
        Files.writeString(
            portsFile,
            "local_http="
                + localHttpAddress()
                + "\nremote_http="
                + remoteHttpAddress()
                + "\nexternal="
                + location
                + "\ndataset="
                + datasetName
                + "\n"
        );
        logger.info("Two clusters are running; details in {}", portsFile);
        // Keep both clusters up for manual curl / ES|QL verification.
        Thread.sleep(TimeUnit.HOURS.toMillis(6));
    }

    private List<List<Object>> runExternal(String query) {
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

    private void indexRemoteDocs(String index) {
        remoteCluster.client().admin().indices().prepareCreate(index).setMapping("name", "type=keyword", "age", "type=long").get();
        indexRemoteDoc(index, "1", "{\"name\":\"alice\",\"age\":30}");
        indexRemoteDoc(index, "2", "{\"name\":\"bob\",\"age\":35}");
        indexRemoteDoc(index, "3", "{\"name\":\"carol\",\"age\":40}");
        remoteCluster.client().admin().indices().prepareRefresh(index).get();
    }

    private void indexRemoteDoc(String index, String id, String source) {
        remoteCluster.client().index(new IndexRequest(index).id(id).source(source, XContentType.JSON)).actionGet();
    }

    private String remoteHttpAddress() {
        return httpAddress(remoteCluster);
    }

    private String localHttpAddress() {
        return httpAddress(internalCluster());
    }

    private static String httpAddress(InternalTestCluster cluster) {
        HttpServerTransport http = cluster.getInstance(HttpServerTransport.class);
        InetSocketAddress address = http.boundAddress().publishAddress().address();
        return address.getHostString() + ":" + address.getPort();
    }
}
