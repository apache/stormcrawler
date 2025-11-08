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

package org.apache.stormcrawler.protocol.okhttp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import kotlin.Pair;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.EventListener;
import okhttp3.EventListener.Factory;
import okhttp3.Handshake;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.Route;
import okhttp3.brotli.BrotliInterceptor;
import okio.BufferedSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.http.HttpHeaders;
import org.apache.http.cookie.Cookie;
import org.apache.storm.Config;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.protocol.ProtocolResponse.TrimmedContentReason;
import org.apache.stormcrawler.proxy.SCProxy;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.CookieConverter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpProtocol.class);

    private final MediaType json = MediaType.parse("application/json; charset=utf-8");

    private OkHttpClient client;

    private int globalMaxContent;

    private int completionTimeout = -1;

    /** Accept partially fetched content as trimmed content */
    private boolean partialContentAsTrimmed = false;

    private final List<KeyValue> customRequestHeaders = new LinkedList<>();

    // track the time spent for each URL in DNS resolution
    private final Map<String, Long> DNStimes = new HashMap<>();

    private OkHttpClient.Builder builder;

    private static final TrustManager[] trustAllCerts =
            new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] chain, String authType)
                            throws CertificateException {}

                    @Override
                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] chain, String authType)
                            throws CertificateException {}

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[] {};
                    }
                }
            };

    private static final SSLContext trustAllSslContext;

    static {
        try {
            trustAllSslContext = SSLContext.getInstance("SSL");
            trustAllSslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final SSLSocketFactory trustAllSslSocketFactory =
            trustAllSslContext.getSocketFactory();

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        globalMaxContent = ConfUtils.getInt(conf, "http.content.limit", -1);

        final int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        this.completionTimeout =
                ConfUtils.getInt(conf, "topology.message.timeout.secs", completionTimeout);

        this.partialContentAsTrimmed =
                ConfUtils.getBoolean(conf, "http.content.partial.as.trimmed", false);

        builder =
                new OkHttpClient.Builder()
                        .retryOnConnectionFailure(
                                ConfUtils.getBoolean(
                                        conf, "http.retry.on.connection.failure", true))
                        .followRedirects(ConfUtils.getBoolean(conf, "http.allow.redirects", false))
                        .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(timeout, TimeUnit.MILLISECONDS);

        // protocols in order of preference, see
        // https://square.github.io/okhttp/4.x/okhttp/okhttp3/-ok-http-client/-builder/protocols/
        final List<okhttp3.Protocol> protocols = new ArrayList<>();
        for (String protocolVersion : protocolVersions) {
            switch (protocolVersion) {
                case "h2":
                    protocols.add(okhttp3.Protocol.HTTP_2);
                    break;
                case "h2c":
                    if (protocolVersions.size() > 1) {
                        LOG.error("h2c ignored, it cannot be combined with any other protocol");
                    } else {
                        protocols.add(okhttp3.Protocol.H2_PRIOR_KNOWLEDGE);
                    }
                    break;
                case "http/1.1":
                    protocols.add(okhttp3.Protocol.HTTP_1_1);
                    break;
                case "http/1.0":
                    LOG.warn("http/1.0 ignored, not supported by okhttp for requests");
                    break;
                default:
                    LOG.error("{}: unknown protocol version", protocolVersion);
                    break;
            }
        }
        if (protocols.size() > 0) {
            LOG.info("Using protocol versions: {}", protocols);
            builder.protocols(protocols);
        }

        final String userAgent = getAgentString(conf);
        if (StringUtils.isNotBlank(userAgent)) {
            customRequestHeaders.add(new KeyValue(HttpHeaders.USER_AGENT, userAgent));
        }

        final String accept = ConfUtils.getString(conf, "http.accept");
        if (StringUtils.isNotBlank(accept)) {
            customRequestHeaders.add(new KeyValue(HttpHeaders.ACCEPT, accept));
        }

        final String acceptLanguage = ConfUtils.getString(conf, "http.accept.language");
        if (StringUtils.isNotBlank(acceptLanguage)) {
            customRequestHeaders.add(new KeyValue(HttpHeaders.ACCEPT_LANGUAGE, acceptLanguage));
        }

        final String basicAuthUser = ConfUtils.getString(conf, "http.basicauth.user", null);

        // use a basic auth?
        if (StringUtils.isNotBlank(basicAuthUser)) {
            final String basicAuthPass = ConfUtils.getString(conf, "http.basicauth.password", "");
            final String encoding =
                    Base64.getEncoder()
                            .encodeToString(
                                    (basicAuthUser + ":" + basicAuthPass)
                                            .getBytes(StandardCharsets.UTF_8));
            customRequestHeaders.add(new KeyValue(HttpHeaders.AUTHORIZATION, "Basic " + encoding));
        }

        customHeaders.forEach(customRequestHeaders::add);

        if (storeHttpHeaders) {
            builder.addNetworkInterceptor(new HTTPHeadersInterceptor());
        }

        if (ConfUtils.getBoolean(conf, "http.trust.everything", true)) {
            builder.sslSocketFactory(trustAllSslSocketFactory, (X509TrustManager) trustAllCerts[0]);
            builder.hostnameVerifier(
                    new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                    });
        }

        builder.eventListenerFactory(
                new Factory() {
                    @Override
                    public EventListener create(Call call) {
                        return new DNSResolutionListener(DNStimes);
                    }
                });

        // enable support for Brotli compression (Content-Encoding)
        builder.addInterceptor(BrotliInterceptor.INSTANCE);

        final Map<String, Object> connectionPoolConf =
                (Map<String, Object>) conf.get("okhttp.protocol.connection.pool");
        if (connectionPoolConf != null) {
            final int size = ConfUtils.getInt(connectionPoolConf, "max.idle.connections", 5);
            final int time = ConfUtils.getInt(connectionPoolConf, "connection.keep.alive", 300);
            builder.connectionPool(new ConnectionPool(size, time, TimeUnit.SECONDS));
            LOG.info(
                    "Using connection pool with max. {} idle connections "
                            + "and {} sec. connection keep-alive time",
                    size,
                    time);
        }

        client = builder.build();
    }

    private void addCookiesToRequest(Builder rb, String url, Metadata md) {
        final String[] cookieStrings =
                md.getValues(RESPONSE_COOKIES_HEADER, protocolMetadataPrefix);
        if (cookieStrings == null || cookieStrings.length == 0) {
            return;
        }
        try {
            final List<Cookie> cookies = CookieConverter.getCookies(cookieStrings, new URL(url));
            for (Cookie c : cookies) {
                rb.addHeader("Cookie", c.getName() + "=" + c.getValue());
            }
        } catch (MalformedURLException e) { // Bad url , nothing to do
        }
    }

    protected void addHeadersToRequest(Builder rb, Metadata md) {
        final String[] headerStrings = md.getValues(SET_HEADER_BY_REQUEST, protocolMetadataPrefix);

        if (headerStrings != null && headerStrings.length > 0) {
            for (String hs : headerStrings) {
                KeyValue h = KeyValue.build(hs);
                rb.addHeader(h.getKey(), h.getValue());
            }
        }
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, final Metadata metadata)
            throws Exception {
        // create default local client
        OkHttpClient localClient = client;

        // conditionally add a dynamic proxy
        if (proxyManager != null) {
            // retrieve proxy from proxy manager
            Optional<SCProxy> proxOptional = proxyManager.getProxy(metadata);

            if (proxOptional.isPresent()) {
                SCProxy prox = proxOptional.get();
                // conditionally configure proxy authentication
                if (StringUtils.isNotBlank(prox.getAddress())) {
                    // format SCProxy into native Java proxy
                    Proxy proxy =
                            new Proxy(
                                    Proxy.Type.valueOf(prox.getProtocol().toUpperCase(Locale.ROOT)),
                                    new InetSocketAddress(
                                            prox.getAddress(), Integer.parseInt(prox.getPort())));

                    // set proxy in builder
                    builder.proxy(proxy);

                    // conditionally add proxy authentication
                    if (StringUtils.isNotBlank(prox.getUsername())) {
                        // add proxy authentication header to builder
                        builder.proxyAuthenticator(
                                (Route route, Response response) -> {
                                    String credential =
                                            Credentials.basic(
                                                    prox.getUsername(), prox.getPassword());
                                    return response.request()
                                            .newBuilder()
                                            .header(HttpHeaders.PROXY_AUTHORIZATION, credential)
                                            .build();
                                });
                    }
                }

                // save start time for debugging speed impact of client build
                long buildStart = System.currentTimeMillis();

                // create new local client from builder using proxy
                localClient = builder.build();

                LOG.debug(
                        "time to build okhttp client with proxy: {}ms",
                        System.currentTimeMillis() - buildStart);

                LOG.debug("fetching with proxy {} - {} ", url, prox.toString());
            }
        }

        final Builder rb = new Request.Builder().url(url);
        customRequestHeaders.forEach(
                (k) -> {
                    rb.header(k.getKey(), k.getValue());
                });

        int pageMaxContent = globalMaxContent;

        if (metadata != null) {
            addHeadersToRequest(rb, metadata);

            final String lastModified = metadata.getFirstValue(HttpHeaders.LAST_MODIFIED);
            if (StringUtils.isNotBlank(lastModified)) {
                rb.header(HttpHeaders.IF_MODIFIED_SINCE, formatHttpDate(lastModified));
            }

            final String ifNoneMatch =
                    metadata.getFirstValue(HttpHeaders.ETAG, protocolMetadataPrefix);
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                rb.header(HttpHeaders.IF_NONE_MATCH, ifNoneMatch);
            }

            final String accept = metadata.getFirstValue("http.accept");
            if (StringUtils.isNotBlank(accept)) {
                rb.header(HttpHeaders.ACCEPT, accept);
            }

            final String acceptLanguage = metadata.getFirstValue("http.accept.language");
            if (StringUtils.isNotBlank(acceptLanguage)) {
                rb.header(HttpHeaders.ACCEPT_LANGUAGE, acceptLanguage);
            }

            final String pageMaxContentStr = metadata.getFirstValue("http.content.limit");
            if (StringUtils.isNotBlank(pageMaxContentStr)) {
                try {
                    pageMaxContent = Integer.parseInt(pageMaxContentStr);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid http.content.limit in metadata: {}", pageMaxContentStr);
                }
            }

            if (useCookies) {
                addCookiesToRequest(rb, url, metadata);
            }

            final String postJsonData = metadata.getFirstValue("http.post.json");
            if (StringUtils.isNotBlank(postJsonData)) {
                RequestBody body = RequestBody.create(postJsonData, json);
                rb.post(body);
            }

            final String useHead = metadata.getFirstValue("http.method.head");
            if (Boolean.parseBoolean(useHead)) {
                rb.head();
            }
        }

        final Request request = rb.build();

        final Call call = localClient.newCall(request);

        try (Response response = call.execute()) {

            final Metadata responsemetadata = new Metadata();
            final Headers headers = response.headers();

            for (int i = 0, size = headers.size(); i < size; i++) {
                final String key = headers.name(i);
                String value = headers.value(i);

                if (key.equals(ProtocolResponse.REQUEST_HEADERS_KEY)
                        || key.equals(ProtocolResponse.RESPONSE_HEADERS_KEY)) {
                    value =
                            new String(
                                    Base64.getDecoder().decode(value), StandardCharsets.ISO_8859_1);
                }

                responsemetadata.addValue(key.toLowerCase(Locale.ROOT), value);
            }

            final MutableObject trimmed = new MutableObject(TrimmedContentReason.NOT_TRIMMED);
            final byte[] bytes = toByteArray(response.body(), pageMaxContent, trimmed);
            if (trimmed.getValue() != TrimmedContentReason.NOT_TRIMMED) {
                if (!call.isCanceled()) {
                    call.cancel();
                }
                responsemetadata.setValue(ProtocolResponse.TRIMMED_RESPONSE_KEY, "true");
                responsemetadata.setValue(
                        ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                        trimmed.getValue().toString().toLowerCase(Locale.ROOT));
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }

            final Long dnsResolution = DNStimes.remove(call.toString());
            if (dnsResolution != null) {
                responsemetadata.setValue("metrics.dns.resolution.msec", dnsResolution.toString());
            }

            return new ProtocolResponse(bytes, response.code(), responsemetadata);
        }
    }

    private byte[] toByteArray(
            final ResponseBody responseBody, int maxContent, MutableObject trimmed)
            throws IOException {

        if (responseBody == null) {
            return new byte[] {};
        }

        int maxContentBytes = Constants.MAX_ARRAY_SIZE;
        if (maxContent != -1) {
            maxContentBytes = Math.min(maxContentBytes, maxContent);
        }

        long endDueFor = -1;
        if (completionTimeout != -1) {
            endDueFor = System.currentTimeMillis() + (completionTimeout * 1000L);
        }

        final BufferedSource source = responseBody.source();
        long bytesRequested = 0L;
        int bufferGrowStepBytes = 8192;

        while (source.getBuffer().size() <= maxContentBytes) {
            bytesRequested +=
                    Math.min(
                            bufferGrowStepBytes,
                            /*
                             * request one byte more than required to reliably detect truncated
                             * content, but beware of integer overflows
                             */
                            (maxContentBytes == Constants.MAX_ARRAY_SIZE
                                            ? maxContentBytes
                                            : (1 + maxContentBytes))
                                    - bytesRequested);
            boolean success = false;
            try {
                success = source.request(bytesRequested);
            } catch (IOException e) {
                // requesting more content failed, e.g. by a socket timeout
                if (partialContentAsTrimmed && source.getBuffer().size() > 0) {
                    // treat already fetched content as trimmed
                    trimmed.setValue(TrimmedContentReason.DISCONNECT);
                    LOG.debug("Exception while fetching {}", e);
                } else {
                    throw e;
                }
            }
            if (!success) {
                // source exhausted, no more data to read
                break;
            }

            if (endDueFor != -1 && endDueFor <= System.currentTimeMillis()) {
                // check whether we hit the completion timeout
                trimmed.setValue(TrimmedContentReason.TIME);
                break;
            }

            // okhttp may fetch more content than requested, quickly "increment"
            // bytes
            bytesRequested = (int) source.getBuffer().size();
        }
        int bytesToCopy = (int) source.getBuffer().size(); // bytesBuffered
        if (maxContent != -1 && bytesToCopy > maxContent) {
            // okhttp's internal buffer is larger than maxContent
            trimmed.setValue(TrimmedContentReason.LENGTH);
            bytesToCopy = maxContentBytes;
        }
        final byte[] arr = new byte[bytesToCopy];
        source.getBuffer().readFully(arr);
        return arr;
    }

    static class HTTPHeadersInterceptor implements Interceptor {

        private String getNormalizedProtocolName(Protocol protocol) {
            String name = protocol.toString().toUpperCase(Locale.ROOT);
            if ("H2".equals(name)) {
                // back-ward compatible protocol version name
                name = "HTTP/2";
            }
            return name;
        }

        @NotNull
        @Override
        public Response intercept(Interceptor.Chain chain) throws IOException {

            final long startFetchTime = System.currentTimeMillis();

            final Connection connection = Objects.requireNonNull(chain.connection());
            final String ipAddress = connection.socket().getInetAddress().getHostAddress();
            final Request request = chain.request();

            final int position = request.url().toString().indexOf(request.url().host());
            final String u =
                    request.url().toString().substring(position + request.url().host().length());

            final StringBuilder requestverbatim = new StringBuilder();

            requestverbatim
                    .append(request.method())
                    .append(" ")
                    .append(u)
                    .append(" ")
                    .append(getNormalizedProtocolName(connection.protocol()))
                    .append("\r\n");

            for (Pair<? extends String, ? extends String> header : request.headers()) {
                requestverbatim
                        .append(header.getFirst())
                        .append(": ")
                        .append(header.getSecond())
                        .append("\r\n");
            }

            requestverbatim.append("\r\n");

            final Response response = chain.proceed(request);

            final StringBuilder responseverbatim = new StringBuilder();

            /*
             * Note: the protocol version between request and response may
             * differ, a server may respond with HTTP/1.0 on a HTTP/1.1 request
             */

            responseverbatim
                    .append(getNormalizedProtocolName(response.protocol()))
                    .append(" ")
                    .append(response.code())
                    .append(" ")
                    .append(response.message())
                    .append("\r\n");

            for (Pair<? extends String, ? extends String> header : response.headers()) {
                responseverbatim
                        .append(header.getFirst())
                        .append(": ")
                        .append(header.getSecond())
                        .append("\r\n");
            }

            responseverbatim.append("\r\n");

            final byte[] encodedBytesResponse =
                    Base64.getEncoder()
                            .encode(
                                    responseverbatim
                                            .toString()
                                            .getBytes(StandardCharsets.ISO_8859_1));

            final byte[] encodedBytesRequest =
                    Base64.getEncoder()
                            .encode(
                                    requestverbatim
                                            .toString()
                                            .getBytes(StandardCharsets.ISO_8859_1));

            final StringBuilder protocols = new StringBuilder(response.protocol().toString());
            final Handshake handshake = connection.handshake();
            if (handshake != null) {
                protocols.append(',').append(handshake.tlsVersion());
                protocols.append(',').append(handshake.cipherSuite());
            }

            // returns a modified version of the response
            return response.newBuilder()
                    .header(
                            ProtocolResponse.REQUEST_HEADERS_KEY,
                            new String(encodedBytesRequest, StandardCharsets.ISO_8859_1))
                    .header(
                            ProtocolResponse.RESPONSE_HEADERS_KEY,
                            new String(encodedBytesResponse, StandardCharsets.ISO_8859_1))
                    .header(ProtocolResponse.RESPONSE_IP_KEY, ipAddress)
                    .header(ProtocolResponse.REQUEST_TIME_KEY, Long.toString(startFetchTime))
                    .header(ProtocolResponse.PROTOCOL_VERSIONS_KEY, protocols.toString())
                    .build();
        }
    }

    public static void main(String args[]) throws Exception {
        org.apache.stormcrawler.protocol.Protocol.main(new HttpProtocol(), args);
    }
}
