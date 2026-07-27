/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.InetAddresses;
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
    static final String CONFIG_CONNECT_TIMEOUT_MILLIS = "connect_timeout_millis";
    static final String CONFIG_SOCKET_TIMEOUT_MILLIS = "socket_timeout_millis";

    static final int DEFAULT_PORT = 9200;
    static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 60_000;
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

    /**
     * Validates a query-time config map: first the key names, then the values {@link ElasticsearchConfiguration}
     * owns. The value check matters for the inline {@code EXTERNAL "es://..." WITH {"api_key": ...}} path, which
     * does not go through the named data source's CRUD validator: without it an {@code api_key} containing CRLF
     * would reach the {@code Authorization} header verbatim (Apache HttpClient 4.5.x does not strip control
     * characters from header values), which is an HTTP request-splitting vector.
     */
    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(config, List.of(Set.of(CONFIG_API_KEY, CONFIG_CONNECT_TIMEOUT_MILLIS, CONFIG_SOCKET_TIMEOUT_MILLIS)));
        ElasticsearchConfiguration.fromQueryConfig(config);
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
    public boolean samplePushdownSupported() {
        // The remote source speaks ES|QL, so a pushed SAMPLE is re-rendered into a remote SAMPLE and applied
        // server-side, drawing the random sample over the full remote dataset instead of over the rows the
        // connector would otherwise materialize locally.
        return true;
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        validateConfig(location, config);
        Endpoint endpoint = parseLocation(location);
        try (RestClient client = buildClient(endpoint.baseUrl(), apiKey(config), config)) {
            List<Attribute> attributes = resolveSchema(client, endpoint.target());
            Map<String, Object> resolvedConfig = new HashMap<>();
            resolvedConfig.put(CONFIG_ENDPOINT, endpoint.baseUrl());
            resolvedConfig.put(CONFIG_TARGET, endpoint.target());
            return new SimpleSourceMetadata(attributes, type(), location, null, null, null, resolvedConfig);
        } catch (ResponseException e) {
            // The remote answered with a non-2xx (e.g. a missing index, a bad target pattern, or a permissions
            // problem): surface the remote status and a snippet of its error body so registration / first-query
            // schema resolution fails with an actionable reason rather than an opaque wrapper.
            throw new IllegalArgumentException(
                "Failed to resolve schema for remote Elasticsearch ["
                    + location
                    + "]: remote returned ["
                    + e.getResponse().getStatusLine()
                    + "]: "
                    + RemoteErrorSnippets.snippet(e.getResponse()),
                e
            );
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
        return new ElasticsearchConnector(buildClient(baseUrl, apiKey(config), config));
    }

    /**
     * Reads the API key out of a config map, running {@link ElasticsearchConfiguration}'s value validation on the
     * way. Both entry points that build a client ({@link #resolveMetadata} and {@link #open}) go through here, so
     * a rejected value (notably a CRLF-bearing key) can never reach the {@code Authorization} header, whether it
     * arrived from a named data source's decrypted secret or from an inline {@code WITH {"api_key": ...}}.
     */
    private static String apiKey(Map<String, Object> config) {
        ElasticsearchConfiguration configuration = ElasticsearchConfiguration.fromQueryConfig(config).value();
        return configuration == null ? null : configuration.apiKey();
    }

    /**
     * Builds the low-level {@link RestClient} used to talk to the remote cluster.
     * <p>
     * A custom {@link DnsResolver} runs the {@link #rejectLinkLocal link-local guard} against the addresses the
     * host actually resolves to <em>each time a connection is opened</em>. This closes the DNS-rebinding window that
     * the literal-IP check in {@link #rejectPrivateHost} cannot cover: a hostname that passed registration can still
     * resolve to {@code 169.254.169.254} (the cloud instance-metadata service) later, and without this guard the API
     * key would be sent there. Validating the resolved address — not just the spelled-out host — is the only reliable
     * defence. The resolver is wired in through a {@link PoolingNHttpClientConnectionManager} because the async client
     * builder (unlike the synchronous one) has no {@code setDnsResolver} shortcut.
     * <p>
     * Connect and socket timeouts default to {@value #DEFAULT_CONNECT_TIMEOUT_MILLIS}ms /
     * {@value #DEFAULT_SOCKET_TIMEOUT_MILLIS}ms but can be overridden per data source / query via the
     * {@value #CONFIG_CONNECT_TIMEOUT_MILLIS} and {@value #CONFIG_SOCKET_TIMEOUT_MILLIS} config keys, so a slow remote
     * cluster or an expensive aggregation does not time out under the defaults.
     * <p>
     * <b>v1 limitations.</b> A fresh client (and thus a fresh HTTP connection pool) is built per schema resolution and
     * per query execution; clients are not pooled or reused across queries to the same endpoint, so each query pays a
     * TCP+TLS setup cost. HTTPS uses the default JVM trust store only — there is no hook yet for a custom CA, disabling
     * verification, or client-certificate auth, so self-hosted clusters behind a private CA are not reachable. Both are
     * tracked for a follow-up.
     */
    private static RestClient buildClient(String baseUrl, String apiKey, Map<String, Object> config) {
        int connectTimeout = intConfig(config, CONFIG_CONNECT_TIMEOUT_MILLIS, DEFAULT_CONNECT_TIMEOUT_MILLIS);
        int socketTimeout = intConfig(config, CONFIG_SOCKET_TIMEOUT_MILLIS, DEFAULT_SOCKET_TIMEOUT_MILLIS);
        var builder = RestClient.builder(HttpHost.create(baseUrl));
        builder.setRequestConfigCallback(
            requestConfig -> requestConfig.setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MILLIS)
        );
        builder.setHttpClientConfigCallback(ElasticsearchConnectorFactory::installLinkLocalSafeDnsResolver);
        if (apiKey != null) {
            builder.setDefaultHeaders(new Header[] { new BasicHeader("Authorization", "ApiKey " + apiKey) });
        }
        return builder.build();
    }

    /**
     * Replaces the async client's connection manager with one whose {@link DnsResolver} rejects link-local
     * resolved addresses, so the SSRF guard runs against the address actually dialled (see {@link #buildClient}).
     * The default connection factory and scheme-strategy registry are used (the {@code null} factory argument),
     * so the standard HTTP/HTTPS session strategies — including the default JVM TLS strategy — stay in place.
     */
    private static HttpAsyncClientBuilder installLinkLocalSafeDnsResolver(HttpAsyncClientBuilder httpClientBuilder) {
        try {
            PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(
                new DefaultConnectingIOReactor(IOReactorConfig.DEFAULT),
                null,
                LINK_LOCAL_SAFE_DNS_RESOLVER
            );
            httpClientBuilder.setConnectionManager(connectionManager);
            return httpClientBuilder;
        } catch (IOReactorException e) {
            throw new UncheckedIOException("Failed to set up Elasticsearch connector HTTP client", e);
        }
    }

    private static int intConfig(Map<String, Object> config, String key, int defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Elasticsearch connector option [" + key + "] must be an integer, got [" + value + "]");
        }
    }

    /**
     * DNS resolver that delegates to the system resolver and then rejects the lookup if any resolved address is
     * link-local. Installed on every client so the SSRF guard runs against the address actually dialled, defeating
     * DNS-rebinding where a benign-looking hostname resolves to a metadata-service address at connection time.
     */
    private static final DnsResolver LINK_LOCAL_SAFE_DNS_RESOLVER = new DnsResolver() {
        private final SystemDefaultDnsResolver delegate = SystemDefaultDnsResolver.INSTANCE;

        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            InetAddress[] addresses = delegate.resolve(host);
            for (InetAddress address : addresses) {
                rejectLinkLocal(address, host);
            }
            return addresses;
        }
    };

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
     * This is a fail-fast pre-check for literal IPs only. Hostnames are intentionally not resolved here
     * (that would add a DNS round-trip at registration time, and the answer could change before the query
     * runs anyway). The authoritative guard against both hostnames and DNS rebinding is the connection-time
     * {@link #LINK_LOCAL_SAFE_DNS_RESOLVER}, which checks the address actually dialled.
     */
    private static void rejectPrivateHost(String host, String location) {
        // Strip surrounding IPv6 brackets before the literal check. URI.getHost() is not consistent across JDKs:
        // older versions return a bare "fe80::1" for a bracketed authority, but JDK 25+ returns "[fe80::1]" with
        // the brackets attached. InetAddresses.isInetAddress() rejects the bracketed form, which would silently let
        // a link-local IPv6 literal through the SSRF pre-check. Normalising here keeps the guard JDK-independent.
        // It returns false for hostnames without triggering a DNS lookup, so genuine hostnames are left for the
        // connection-time resolver.
        String literal = stripIpv6Brackets(host);
        if (InetAddresses.isInetAddress(literal) == false) {
            return;
        }
        rejectLinkLocal(InetAddresses.forString(literal), location);
    }

    /** Removes the surrounding {@code [ ]} from a bracketed IPv6 literal host; returns other hosts unchanged. */
    private static String stripIpv6Brackets(String host) {
        if (host.length() >= 2 && host.charAt(0) == '[' && host.charAt(host.length() - 1) == ']') {
            return host.substring(1, host.length() - 1);
        }
        return host;
    }

    /**
     * Throws if {@code address} is link-local (169.254.0.0/16, fe80::/10), the range used by cloud
     * instance-metadata services. Shared by the registration-time literal-IP pre-check and the
     * connection-time DNS resolver so both apply identical policy.
     */
    private static void rejectLinkLocal(InetAddress address, String origin) {
        if (address.isLinkLocalAddress()) {
            throw new IllegalArgumentException(
                "Invalid Elasticsearch location ["
                    + origin
                    + "]: link-local addresses are not allowed (they map to cloud metadata services)"
            );
        }
    }

    record Endpoint(String baseUrl, String target) {}
}
