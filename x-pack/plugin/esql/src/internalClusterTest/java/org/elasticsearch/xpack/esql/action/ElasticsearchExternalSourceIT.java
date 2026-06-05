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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.datasource.elasticsearch.ElasticsearchDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

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
                EsqlPluginWithEnterpriseOrTrialLicense.class,
                TestEncryptionServicePlugin.class
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

    // The filter/limit assertions below verify end-to-end correctness with pushdown enabled. They do
    // not by themselves prove the WHERE/LIMIT was executed remotely: the local FilterExec/LimitExec
    // safety nets would make the same assertions pass even without pushdown. buildRemoteQuery unit
    // tests cover the remote-query string; proving remote execution (request capture) is a follow-up.

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
        remoteCluster.client()
            .index(new IndexRequest(index).id(id).source(source, org.elasticsearch.xcontent.XContentType.JSON))
            .actionGet();
    }

    private String remoteHttpAddress() {
        HttpServerTransport http = remoteCluster.getInstance(HttpServerTransport.class);
        InetSocketAddress address = http.boundAddress().publishAddress().address();
        return address.getHostString() + ":" + address.getPort();
    }
}
