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

import static org.opensearch.client.RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.opensearch.client.RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.HttpAsyncResponseConsumerFactory;
import org.opensearch.client.Node;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.sniff.Sniffer;
import org.opensearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to instantiate an OpenSearch client and bulkprocessor based on the configuration.
 */
public final class OpenSearchConnection {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConnection.class);

    @NotNull private final RestHighLevelClient client;

    @NotNull private final BulkProcessor processor;

    @Nullable private final Sniffer sniffer;

    private OpenSearchConnection(
            @NotNull RestHighLevelClient c, @NotNull BulkProcessor p, @Nullable Sniffer s) {
        processor = p;
        client = c;
        sniffer = s;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public static RestHighLevelClient getClient(Map<String, Object> stormConf, String boltType) {

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
            hosts.add(new HttpHost(uri.getHost(), port, scheme));
        }

        final RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[0]));

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

        if (needsUser || needsProxy || disableTlsValidation) {
            builder.setHttpClientConfigCallback(
                    httpClientBuilder -> {
                        if (needsUser) {
                            final CredentialsProvider credentialsProvider =
                                    new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(
                                    AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                        if (needsProxy) {
                            httpClientBuilder.setProxy(
                                    new HttpHost(proxyhost, proxyport, proxyscheme));
                        }

                        if (disableTlsValidation) {
                            try {
                                final SSLContextBuilder sslContext = new SSLContextBuilder();
                                sslContext.loadTrustMaterial(null, new TrustAllStrategy());
                                httpClientBuilder.setSSLContext(sslContext.build());
                                httpClientBuilder.setSSLHostnameVerifier(
                                        NoopHostnameVerifier.INSTANCE);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to disable TLS validation", e);
                            }
                        }
                        return httpClientBuilder;
                    });
        }

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
                        DEFAULT_SOCKET_TIMEOUT_MILLIS);
        // timeout until connection is established
        builder.setRequestConfigCallback(
                requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(connectTimeout)
                                // Timeout when waiting for data
                                .setSocketTimeout(socketTimeout));

        // TODO check if this has gone somewhere else
        // int maxRetryTimeout = ConfUtils.getInt(stormConf, Constants.PARAMPREFIX +
        // boltType +
        // ".max.retry.timeout",
        // DEFAULT_MAX_RETRY_TIMEOUT_MILLIS);
        // builder.setMaxRetryTimeoutMillis(maxRetryTimeout);

        // TODO configure headers etc...
        // Map<String, String> configSettings = (Map) stormConf
        // .get(Constants.PARAMPREFIX + boltType + ".settings");
        // if (configSettings != null) {
        // configSettings.forEach((k, v) -> settings.put(k, v));
        // }

        // use node selector only to log nodes listed in the config
        // and/or discovered through sniffing
        builder.setNodeSelector(
                nodes -> {
                    for (Node node : nodes) {
                        LOG.debug(
                                "Connected to OpenSearch node {} [{}] for {}",
                                node.getName(),
                                node.getHost(),
                                boltType);
                    }
                });

        final boolean compression =
                ConfUtils.getBoolean(
                        stormConf, Constants.PARAMPREFIX, dottedType, "compression", false);

        builder.setCompressionEnabled(compression);

        return new RestHighLevelClient(builder);
    }

    public void addToProcessor(final DocWriteRequest<?> request) {
        processor.add(request);
    }

    /**
     * Creates a connection with a default listener. The values for bolt type are
     * [indexer,status,metrics]
     */
    public static OpenSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType) {
        BulkProcessor.Listener listener =
                new BulkProcessor.Listener() {
                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1, BulkResponse arg2) {}

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1, Throwable arg2) {}

                    @Override
                    public void beforeBulk(long arg0, BulkRequest arg1) {}
                };
        return getConnection(stormConf, boltType, listener);
    }

    public static OpenSearchConnection getConnection(
            Map<String, Object> stormConf, String boltType, BulkProcessor.Listener listener) {

        final RestHighLevelClient client = getClient(stormConf, boltType);

        final String dottedType = boltType + ".";

        final String flushIntervalString =
                ConfUtils.getString(
                        stormConf, Constants.PARAMPREFIX, dottedType, "flushInterval", "5s");

        final TimeValue flushInterval =
                TimeValue.parseTimeValue(
                        flushIntervalString, TimeValue.timeValueSeconds(5), "flushInterval");

        final int bulkActions =
                ConfUtils.getInt(stormConf, Constants.PARAMPREFIX, dottedType, "bulkActions", 50);

        final int concurrentRequests =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX, dottedType, "concurrentRequests", 1);

        final RequestOptions requestOptions = RequestOptions.DEFAULT;
        final RequestOptions.Builder requestOptionsBuilder = requestOptions.toBuilder();
        final int bufferSize =
                ConfUtils.getInt(
                        stormConf, Constants.PARAMPREFIX, dottedType, "responseBufferSize", 100);

        requestOptionsBuilder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(
                        bufferSize * 1024 * 1024));

        BulkProcessor bulkProcessor = null;
        Sniffer sniffer = null;
        try {
            bulkProcessor =
                    BulkProcessor.builder(
                                    (request, bulkListener) ->
                                            client.bulkAsync(
                                                    request,
                                                    requestOptionsBuilder.build(),
                                                    bulkListener),
                                    listener)
                            .setFlushInterval(flushInterval)
                            .setBulkActions(bulkActions)
                            .setConcurrentRequests(concurrentRequests)
                            .build();

            boolean sniff =
                    ConfUtils.getBoolean(
                            stormConf, Constants.PARAMPREFIX, dottedType, "sniff", true);
            if (sniff) {
                sniffer = Sniffer.builder(client.getLowLevelClient()).build();
            }

            return new OpenSearchConnection(client, bulkProcessor, sniffer);
        } catch (Exception e) {
            if (bulkProcessor != null) {
                try {
                    bulkProcessor.close();
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            try {
                client.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private boolean isClosed = false;

    public void close() {

        if (isClosed) {
            LOG.warn("Tried to close an already closed connection!");
            return;
        }

        // Maybe some kind of identifier?
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

        if (sniffer != null) {
            sniffer.close();
        }

        // Now close the actual client
        try {
            client.close();
        } catch (IOException e) {
            // ignore silently
            LOG.trace("Client threw IO exception.");
        }

        isClosed = true;
    }
}
