/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.opensearch;

import static org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder.DEFAULT_RESPONSE_TIMEOUT_MILLIS;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to instantiate an OpenSearch client and bulkprocessor based on the configuration.
 */
public final class OpenSearchConnection {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConnection.class);

    @NotNull private final OpenSearchClient client;

    @NotNull private final AsyncBulkProcessor processor;

    @NotNull private final OpenSearchTransport transport;

    private OpenSearchConnection(
            @NotNull OpenSearchClient c,
            @NotNull AsyncBulkProcessor p,
            @NotNull OpenSearchTransport t) {
        client = c;
        processor = p;
        transport = t;
    }

    public OpenSearchClient getClient() {
        return client;
    }

    /**
     * Creates a standalone {@link OpenSearchClient}. Used by classes that need a client without a
     * bulk processor (e.g. spouts, filters). Callers are responsible for closing the returned
     * client's transport via {@code client._transport().close()}.
     */
    public static OpenSearchClient getClient(Map<String, Object> stormConf, String boltType) {
        return buildClientResources(stormConf, boltType).client();
    }

    /** Adds a single bulk operation to the internal processor. */
    public void addToProcessor(final BulkOperation operation) {
        processor.add(operation);
    }

    /**
     * Creates a connection with a default (no-op) listener. The values for bolt type are
     * [indexer,status,metrics].
     */
    public static OpenSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType) {
        AsyncBulkProcessor.Listener listener =
                new AsyncBulkProcessor.Listener() {
                    @Override
                    public void afterBulk(
                            long arg0,
                            org.opensearch.client.opensearch.core.BulkRequest arg1,
                            org.opensearch.client.opensearch.core.BulkResponse arg2) {}

                    @Override
                    public void afterBulk(
                            long arg0,
                            org.opensearch.client.opensearch.core.BulkRequest arg1,
                            Throwable arg2) {}

                    @Override
                    public void beforeBulk(
                            long arg0, org.opensearch.client.opensearch.core.BulkRequest arg1) {}
                };
        return getConnection(stormConf, boltType, listener);
    }

    public static OpenSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType, AsyncBulkProcessor.Listener listener) {

        final String dottedType = boltType + ".";

        warnOnRemovedKeys(stormConf, dottedType);

        ClientResources cr = buildClientResources(stormConf, boltType);

        final String flushIntervalString =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX, dottedType, "flushInterval", "5s");

        final long flushIntervalMillis = parseTimeValueToMillis(flushIntervalString, 5000);

        final int bulkActions =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX, dottedType, "bulkActions", 50);

        final int concurrentRequests =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX, dottedType, "concurrentRequests", 1);

        AsyncBulkProcessor bulkProcessor = null;
        try {
            bulkProcessor =
                    new AsyncBulkProcessor.Builder(cr.client(), listener)
                            .setBulkActions(bulkActions)
                            .setFlushIntervalMillis(flushIntervalMillis)
                            .setConcurrentRequests(concurrentRequests)
                            .build();

            return new OpenSearchConnection(cr.client(), bulkProcessor, cr.transport());
        } catch (Exception e) {
            if (bulkProcessor != null) {
                try {
                    bulkProcessor.close();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            try {
                cr.transport().close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public void close() {

        if (!isClosed.compareAndSet(false, true)) {
            LOG.warn("Tried to close an already closed connection!");
            return;
        }

        LOG.debug("Start closing the OpenSearch connection");

        // First, close the BulkProcessor ensuring pending actions are flushed
        try {
            boolean success = processor.awaitClose(60, TimeUnit.SECONDS);
            if (!success) {
                throw new RuntimeException(
                        "Failed to flush pending actions when closing BulkProcessor");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Now close the transport (also shuts down the underlying HTTP client)
        try {
            transport.close();
        } catch (IOException e) {
            LOG.trace("Transport threw IO exception on close.");
        }
    }

    /**
     * Extracts the document ID from a {@link BulkOperation} regardless of its type (index, create,
     * delete, update).
     */
    public static String getBulkOperationId(BulkOperation op) {
        if (op.isIndex()) {
            return op.index().id();
        }
        if (op.isCreate()) {
            return op.create().id();
        }
        if (op.isDelete()) {
            return op.delete().id();
        }
        if (op.isUpdate()) {
            return op.update().id();
        }
        return null;
    }

    // internal helpers
    private record ClientResources(OpenSearchClient client, OpenSearchTransport transport) {}

    /**
     * Logs a WARN for legacy configuration keys that are no longer honoured by this module, so that
     * users migrating from {@code external/opensearch} notice silently-dropped tuning. See the
     * module README for the full list of differences.
     */
    private static void warnOnRemovedKeys(Map<String, Object> stormConf, String dottedType) {
        final String responseBufferKey = Constants.PARAMPREFIX + dottedType + "responseBufferSize";
        if (stormConf.containsKey(responseBufferKey)) {
            LOG.warn(
                    "Configuration key '{}' is set but no longer supported by the opensearch-java module. "
                            + "The HC5-based async transport does not expose an equivalent per-request "
                            + "heap-buffer override. The setting is ignored — remove it from your "
                            + "configuration. See external/opensearch-java/README.md for details.",
                    responseBufferKey);
        }

        final String sniffKey = Constants.PARAMPREFIX + dottedType + "sniff";
        if (stormConf.containsKey(sniffKey)) {
            LOG.warn(
                    "Configuration key '{}' is set but no longer supported by the opensearch-java module. "
                            + "The OpenSearch Java Client 3.x does not ship a Sniffer equivalent, so "
                            + "automatic node discovery is not available. Keep the 'addresses' list up to "
                            + "date manually or put a load balancer in front of the cluster. "
                            + "See external/opensearch-java/README.md for details.",
                    sniffKey);
        }
    }

    private static ClientResources buildClientResources(
            Map<String, Object> stormConf, String boltType) {

        final String dottedType = boltType + ".";

        final List<HttpHost> hosts = new ArrayList<>();

        final List<String> confighosts =
                ConfUtils.loadListFromConf(
                        Constants.PARAMPREFIX, dottedType, "addresses", stormConf);

        // find ; separated values and tokenise as multiple addresses
        // e.g. opensearch1:9200; opensearch2:9200
        if (confighosts.size() == 1) {
            String input = confighosts.get(0);
            confighosts.clear();
            confighosts.addAll(Arrays.asList(input.split(" *; *")));
        }

        for (String host : confighosts) {
            // no port specified? use default one
            int port = 9200;
            String scheme = "http";
            // no scheme specified? use http
            if (!host.startsWith(scheme)) {
                host = "http://" + host;
            }
            URI uri = URI.create(host);
            if (uri.getHost() == null) {
                throw new RuntimeException("host undefined " + host);
            }
            if (uri.getPort() != -1) {
                port = uri.getPort();
            }
            if (uri.getScheme() != null) {
                scheme = uri.getScheme();
            }
            // HC5: constructor is (scheme, hostname, port) — not (hostname, port, scheme)
            hosts.add(new HttpHost(scheme, uri.getHost(), port));
        }

        LOG.info(
                "OpenSearch {} transport configured with {} host(s): {}",
                boltType,
                hosts.size(),
                hosts);

        // authentication via user / password
        final String user =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX, dottedType, "user");
        final String password =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX, dottedType, "password");

        final String proxyhost =
                ConfUtils.getString(stormConf, Constants.PARAMPREFIX, dottedType, "proxy.host");

        final int proxyport =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX, dottedType, "proxy.port", -1);

        final String proxyscheme =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX, dottedType, "proxy.scheme", "http");

        final boolean disableTlsValidation =
                ConfUtils.getBoolean(
                        stormConf, Constants.PARAMPREFIX, "", "disable.tls.validation", false);

        final boolean needsUser = StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password);
        final boolean needsProxy = StringUtils.isNotBlank(proxyhost) && proxyport != -1;

        // Defaults from ApacheHttpClient5TransportBuilder (same as the former RestClientBuilder)
        final int connectTimeout =
                ConfUtils.getInt(
                        stormConf,
                        Constants.PARAMPREFIX,
                        dottedType,
                        "connect.timeout",
                        DEFAULT_CONNECT_TIMEOUT_MILLIS);
        final int socketTimeout =
                ConfUtils.getInt(
                        stormConf,
                        Constants.PARAMPREFIX,
                        dottedType,
                        "socket.timeout",
                        DEFAULT_RESPONSE_TIMEOUT_MILLIS);

        final boolean compression =
                ConfUtils.getBoolean(
                        stormConf, Constants.PARAMPREFIX, dottedType, "compression", false);

        final ApacheHttpClient5TransportBuilder builder =
                ApacheHttpClient5TransportBuilder.builder(hosts.toArray(new HttpHost[0]))
                        .setMapper(new JacksonJsonpMapper());

        // Timeouts via ConnectionConfig on the builder's internal connection manager
        builder.setConnectionConfigCallback(
                connConfigBuilder ->
                        connConfigBuilder
                                .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout))
                                .setSocketTimeout(Timeout.ofMilliseconds(socketTimeout)));

        // Auth, proxy, and/or trust-all SSL via HttpClient customisation
        if (needsUser || needsProxy || disableTlsValidation) {
            builder.setHttpClientConfigCallback(
                    httpClientBuilder -> {
                        // hc.client5 auth: password is char[], AuthScope(host, port)
                        if (needsUser) {
                            final BasicCredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(
                                    new AuthScope(null, -1),
                                    new UsernamePasswordCredentials(user, password.toCharArray()));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                        // hc.client5 proxy: HttpHost(scheme, host, port)
                        if (needsProxy) {
                            httpClientBuilder.setProxy(
                                    new HttpHost(proxyscheme, proxyhost, proxyport));
                        }
                        // Custom connection manager overrides the builder's internal one,
                        // so timeouts and TlsDetailsFactory must be replicated here
                        if (disableTlsValidation) {
                            try {
                                final SSLContext sslContext =
                                        SSLContextBuilder.create()
                                                .loadTrustMaterial((chain, authType) -> true)
                                                .build();
                                httpClientBuilder.setConnectionManager(
                                        PoolingAsyncClientConnectionManagerBuilder.create()
                                                .setTlsStrategy(
                                                        ClientTlsStrategyBuilder.create()
                                                                .setSslContext(sslContext)
                                                                .setHostnameVerifier(
                                                                        NoopHostnameVerifier
                                                                                .INSTANCE)
                                                                // HTTP/2 ALPN negotiation
                                                                .setTlsDetailsFactory(
                                                                        sslEngine ->
                                                                                new TlsDetails(
                                                                                        sslEngine
                                                                                                .getSession(),
                                                                                        sslEngine
                                                                                                .getApplicationProtocol()))
                                                                .build())
                                                .setDefaultConnectionConfig(
                                                        ConnectionConfig.custom()
                                                                .setConnectTimeout(
                                                                        Timeout.ofMilliseconds(
                                                                                connectTimeout))
                                                                .setSocketTimeout(
                                                                        Timeout.ofMilliseconds(
                                                                                socketTimeout))
                                                                .build())
                                                .build());
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to disable TLS validation", e);
                            }
                        }
                        return httpClientBuilder;
                    });
        }

        // Compression: first-class builder method, not a request interceptor
        builder.setCompressionEnabled(compression);

        final OpenSearchTransport transport = builder.build();
        final OpenSearchClient openSearchClient = new OpenSearchClient(transport);

        return new ClientResources(openSearchClient, transport);
    }

    /**
     * Parses a time value string (e.g. "5s", "500ms", "1m") into milliseconds.
     *
     * @param value the string to parse
     * @param defaultMillis the default if parsing fails
     * @return milliseconds
     */
    static long parseTimeValueToMillis(String value, long defaultMillis) {
        if (value == null || value.isBlank()) {
            return defaultMillis;
        }
        value = value.trim();
        try {
            if (value.endsWith("ms")) {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (value.endsWith("s")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1000;
            } else if (value.endsWith("m")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 60000;
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse time value '{}', using default {}ms", value, defaultMillis);
            return defaultMillis;
        }
    }
}
