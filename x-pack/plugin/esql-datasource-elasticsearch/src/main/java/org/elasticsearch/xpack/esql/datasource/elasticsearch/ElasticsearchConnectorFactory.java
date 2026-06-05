/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Factory for the {@code elasticsearch} connector. Handles {@code es://} and {@code elasticsearch://}
 * location URIs of the form {@code es://host:port/index}.
 * <p>
 * Schema resolution runs {@code FROM <index> | LIMIT 0} against the remote {@code _query} API so the
 * resolved schema matches exactly what a real query returns. The remote endpoint and target index are stored in
 * the resolved config and handed back to {@link #open} at execution time; credentials stay in the original query
 * config so encrypted data-source secrets are not copied into resolved metadata as plaintext.
 */
class ElasticsearchConnectorFactory implements ConnectorFactory {

    static final String CONFIG_ENDPOINT = "endpoint";
    static final String CONFIG_TARGET = "target";
    static final String CONFIG_API_KEY = "api_key";

    static final int DEFAULT_PORT = 9200;
    private static final int CONNECT_TIMEOUT_MILLIS = 10_000;
    private static final int SOCKET_TIMEOUT_MILLIS = 60_000;
    private static final int CONNECTION_REQUEST_TIMEOUT_MILLIS = 10_000;

    @Override
    public String type() {
        return ElasticsearchDataSourcePlugin.TYPE;
    }

    @Override
    public boolean canHandle(String location) {
        return canHandleLocation(location);
    }

    /** True if {@code location} uses one of this connector's URI schemes. Shared with CRUD-time validation. */
    static boolean canHandleLocation(String location) {
        return location != null
            && (location.startsWith("es://")
                || location.startsWith("elasticsearch://")
                || location.startsWith("es+https://")
                || location.startsWith("elasticsearch+https://"));
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(config, List.of(Set.of(CONFIG_API_KEY)));
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        // The remote source speaks ES|QL, so filters are pushed by re-rendering them into a remote WHERE clause.
        return EsqlFilterTranslator.INSTANCE;
    }

    @Override
    public boolean expandsPatternRemotely() {
        // The target is a remote index/data-stream pattern (e.g. logs*); the remote cluster expands it.
        return true;
    }

    @Override
    public boolean sortPushdownSupported() {
        // The remote source speaks ES|QL, so a pushed SORT is re-rendered into a remote SORT and applied
        // server-side, returning the correct global top-N when paired with the pushed LIMIT.
        return true;
    }

    @Override
    public boolean aggregatePushdownSupported() {
        // The remote source speaks ES|QL, so a pushed STATS is re-rendered into a remote STATS and computed
        // server-side. The connector can return either final rows or intermediate state, depending on the
        // aggregate mode the optimizer pushed.
        return true;
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        validateConfig(location, config);
        Endpoint endpoint = parseLocation(location);
        String apiKey = Objects.toString(config.get(CONFIG_API_KEY), null);
        try (RestClient client = buildClient(endpoint.baseUrl(), apiKey)) {
            List<Attribute> attributes = resolveSchema(client, endpoint.target());
            Map<String, Object> resolvedConfig = new HashMap<>();
            resolvedConfig.put(CONFIG_ENDPOINT, endpoint.baseUrl());
            resolvedConfig.put(CONFIG_TARGET, endpoint.target());
            return new SimpleSourceMetadata(attributes, type(), location, null, null, null, resolvedConfig);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve schema for remote Elasticsearch [" + location + "]", e);
        }
    }

    @Override
    public Connector open(Map<String, Object> config) {
        String baseUrl = Objects.toString(config.get(CONFIG_ENDPOINT), null);
        if (baseUrl == null) {
            throw new IllegalArgumentException("Elasticsearch connector requires '" + CONFIG_ENDPOINT + "' in config");
        }
        String apiKey = Objects.toString(config.get(CONFIG_API_KEY), null);
        return new ElasticsearchConnector(buildClient(baseUrl, apiKey));
    }

    private static RestClient buildClient(String baseUrl, String apiKey) {
        var builder = RestClient.builder(HttpHost.create(baseUrl));
        builder.setRequestConfigCallback(
            requestConfig -> requestConfig.setConnectTimeout(CONNECT_TIMEOUT_MILLIS)
                .setSocketTimeout(SOCKET_TIMEOUT_MILLIS)
                .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MILLIS)
        );
        if (apiKey != null) {
            builder.setDefaultHeaders(new Header[] { new BasicHeader("Authorization", "ApiKey " + apiKey) });
        }
        return builder.build();
    }

    private static List<Attribute> resolveSchema(RestClient client, String target) throws IOException {
        // Same validation/quoting as a real query so a crafted target can't inject into the schema probe.
        Request request = RemoteQuery.request("FROM " + EsqlIdentifiers.validateTarget(target) + " | LIMIT 0");
        Response response = client.performRequest(request);
        List<EsqlTypeMapping.RemoteColumn> columns = new ArrayList<>();
        try (
            InputStream content = response.getEntity().getContent();
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content)
        ) {
            parser.nextToken(); // START_OBJECT
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.FIELD_NAME && "columns".equals(parser.currentName())) {
                    parser.nextToken(); // START_ARRAY
                    EsqlTypeMapping.parseColumns(parser, columns);
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        return EsqlTypeMapping.toAttributes(columns);
    }

    /**
     * Parses {@code es://host:port/index} into a base URL and target index.
     * <p>
     * The transport defaults to plaintext {@code http}. TLS is selected with a {@code +https} scheme
     * suffix ({@code es+https://...} or {@code elasticsearch+https://...}); when secure and no explicit
     * port is given, the URL default ({@code 443}) is used so managed endpoints such as Elastic Cloud
     * work without spelling out a port. Plaintext with no explicit port defaults to {@value #DEFAULT_PORT}.
     */
    static Endpoint parseLocation(String location) {
        URI uri = URI.create(location);
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException("Invalid Elasticsearch location [" + location + "]: missing scheme");
        }
        boolean secure = scheme.endsWith("+https");
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid Elasticsearch location [" + location + "]: missing host");
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException(
                "Invalid Elasticsearch location [" + location + "]: user info is not supported; use api_key config instead"
            );
        }
        rejectPrivateHost(host, location);
        String path = uri.getPath();
        if (path == null || path.length() <= 1) {
            throw new IllegalArgumentException("Invalid Elasticsearch location [" + location + "]: missing index in path");
        }
        String target = path.substring(1);
        StringBuilder baseUrl = new StringBuilder(secure ? "https://" : "http://").append(host);
        if (uri.getPort() > 0) {
            baseUrl.append(':').append(uri.getPort());
        } else if (secure == false) {
            // Plaintext keeps the historical 9200 default; secure relies on the URL's implicit 443.
            baseUrl.append(':').append(DEFAULT_PORT);
        }
        return new Endpoint(baseUrl.toString(), target);
    }

    /**
     * Rejects IP-literal hosts in the link-local range (169.254.0.0/16, fe80::/10), which is used
     * by cloud-provider instance metadata services (AWS, GCP, and Azure all serve credentials at
     * 169.254.169.254). Allowing those addresses would let an admin-registered data source silently
     * exfiltrate instance credentials at query time.
     * <p>
     * Loopback and RFC-1918 private ranges are intentionally allowed: connecting to a local or
     * network-internal Elasticsearch cluster is a legitimate use case. Link-local is the one range
     * that has no legitimate ES endpoint but is universally reachable on cloud hosts.
     * <p>
     * Only literal IP addresses are checked; hostnames are allowed through because resolving them here
     * would require a DNS round-trip at dataset-registration time.
     */
    private static void rejectPrivateHost(String host, String location) {
        // Only check literal IP addresses (IPv4: digits-and-dots; IPv6: contains a colon).
        boolean isIpLiteral = host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+.*") || host.contains(":");
        if (isIpLiteral == false) {
            return;
        }
        try {
            InetAddress addr = InetAddress.getByName(host);
            if (addr.isLinkLocalAddress()) {
                throw new IllegalArgumentException(
                    "Invalid Elasticsearch location ["
                        + location
                        + "]: link-local addresses are not allowed (they map to cloud metadata services)"
                );
            }
        } catch (UnknownHostException e) {
            // Malformed IP literal — connection will fail at query time with a clear error.
        }
    }

    record Endpoint(String baseUrl, String target) {}
}
