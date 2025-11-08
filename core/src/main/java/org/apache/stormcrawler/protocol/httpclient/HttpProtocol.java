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

package org.apache.stormcrawler.protocol.httpclient;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.Args;
import org.apache.http.util.ByteArrayBuffer;
import org.apache.storm.Config;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.proxy.SCProxy;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.CookieConverter;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/** Uses Apache httpclient to handle http and https. */
public class HttpProtocol extends AbstractHttpProtocol
        implements ResponseHandler<ProtocolResponse> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpProtocol.class);

    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER =
            new PoolingHttpClientConnectionManager();

    private int globalMaxContent;

    private HttpClientBuilder builder;

    private RequestConfig requestConfig;
    private RequestConfig.Builder requestConfigBuilder;

    @Override
    public void configure(final Config conf) {

        super.configure(conf);

        // allow up to 200 connections or same as the number of threads used for
        // fetching
        int maxFetchThreads = ConfUtils.getInt(conf, "fetcher.threads.number", 200);
        CONNECTION_MANAGER.setMaxTotal(maxFetchThreads);
        int maxPerRoute = ConfUtils.getInt(conf, "fetcher.threads.per.queue", 1);
        if (maxPerRoute < 20) {
            maxPerRoute = 20;
        }
        CONNECTION_MANAGER.setDefaultMaxPerRoute(maxPerRoute);

        globalMaxContent = ConfUtils.getInt(conf, "http.content.limit", -1);

        String userAgent = getAgentString(conf);

        Collection<BasicHeader> defaultHeaders = new LinkedList<>();

        String accept = ConfUtils.getString(conf, "http.accept");
        if (StringUtils.isNotBlank(accept)) {
            defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT, accept));
        }

        customHeaders.forEach(
                h -> {
                    defaultHeaders.add(new BasicHeader(h.getKey(), h.getValue()));
                });

        String basicAuthUser = ConfUtils.getString(conf, "http.basicauth.user", null);

        // use a basic auth?
        if (StringUtils.isNotBlank(basicAuthUser)) {
            String basicAuthPass = ConfUtils.getString(conf, "http.basicauth.password", "");
            String encoding =
                    Base64.getEncoder()
                            .encodeToString(
                                    (basicAuthUser + ":" + basicAuthPass)
                                            .getBytes(StandardCharsets.UTF_8));
            defaultHeaders.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding));
        }

        String acceptLanguage = ConfUtils.getString(conf, "http.accept.language");
        if (StringUtils.isNotBlank(acceptLanguage)) {
            defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_LANGUAGE, acceptLanguage));
        }

        builder =
                HttpClients.custom()
                        .setUserAgent(userAgent)
                        .setDefaultHeaders(defaultHeaders)
                        .setConnectionManager(CONNECTION_MANAGER)
                        .setConnectionManagerShared(true)
                        .disableRedirectHandling()
                        .disableAutomaticRetries();

        int timeout = ConfUtils.getInt(conf, "http.timeout", 10000);

        requestConfigBuilder =
                RequestConfig.custom()
                        .setSocketTimeout(timeout)
                        .setConnectTimeout(timeout)
                        .setConnectionRequestTimeout(timeout)
                        .setCookieSpec(CookieSpecs.STANDARD);

        requestConfig = requestConfigBuilder.build();
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md) throws Exception {

        LOG.debug("HTTP connection manager stats {}", CONNECTION_MANAGER.getTotalStats());

        // set default request config to global config
        RequestConfig reqConfig = requestConfig;

        // conditionally add a dynamic proxy
        if (proxyManager != null) {
            // retrieve proxy from proxy manager
            Optional<SCProxy> proxOptional = proxyManager.getProxy(md);
            if (proxOptional.isPresent()) {
                SCProxy prox = proxOptional.get();
                // conditionally configure proxy authentication
                if (StringUtils.isNotBlank(prox.getUsername())) {
                    List<String> authSchemes = new ArrayList<>();

                    // Can make configurable and add more in future
                    authSchemes.add(AuthSchemes.BASIC);
                    requestConfigBuilder.setProxyPreferredAuthSchemes(authSchemes);

                    BasicCredentialsProvider basicAuthCreds = new BasicCredentialsProvider();
                    basicAuthCreds.setCredentials(
                            new AuthScope(prox.getAddress(), Integer.parseInt(prox.getPort())),
                            new UsernamePasswordCredentials(
                                    prox.getUsername(), prox.getPassword()));
                    builder.setDefaultCredentialsProvider(basicAuthCreds);
                }

                HttpHost proxy = new HttpHost(prox.getAddress(), Integer.parseInt(prox.getPort()));
                DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy);
                builder.setRoutePlanner(routePlanner);

                // save start time for debugging speed impact of request config
                // build
                long buildStart = System.currentTimeMillis();

                // set request config to new configuration with dynamic proxy
                reqConfig = requestConfigBuilder.build();

                LOG.debug(
                        "time to build http request config with proxy: {}ms",
                        System.currentTimeMillis() - buildStart);

                LOG.debug("fetching with " + prox.toString());
            }
        }

        HttpRequestBase request = new HttpGet(url);
        ResponseHandler<ProtocolResponse> responseHandler = this;

        if (md != null) {

            addHeadersToRequest(request, md);

            String useHead = md.getFirstValue("http.method.head");
            if (Boolean.parseBoolean(useHead)) {
                request = new HttpHead(url);
            }

            String lastModified = md.getFirstValue(HttpHeaders.LAST_MODIFIED);
            if (StringUtils.isNotBlank(lastModified)) {
                request.addHeader(HttpHeaders.IF_MODIFIED_SINCE, formatHttpDate(lastModified));
            }

            String ifNoneMatch = md.getFirstValue(HttpHeaders.ETAG, protocolMetadataPrefix);
            if (StringUtils.isNotBlank(ifNoneMatch)) {
                request.addHeader(HttpHeaders.IF_NONE_MATCH, ifNoneMatch);
            }

            String accept = md.getFirstValue("http.accept");
            if (StringUtils.isNotBlank(accept)) {
                request.setHeader(new BasicHeader(HttpHeaders.ACCEPT, accept));
            }

            String acceptLanguage = md.getFirstValue("http.accept.language");
            if (StringUtils.isNotBlank(acceptLanguage)) {
                request.setHeader(new BasicHeader(HttpHeaders.ACCEPT_LANGUAGE, acceptLanguage));
            }

            String pageMaxContentStr = md.getFirstValue("http.content.limit");
            if (StringUtils.isNotBlank(pageMaxContentStr)) {
                try {
                    int pageMaxContent = Integer.parseInt(pageMaxContentStr);
                    responseHandler = getResponseHandlerWithContentLimit(pageMaxContent);
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid http.content.limit in metadata: {}", pageMaxContentStr);
                }
            }

            if (useCookies) {
                addCookiesToRequest(request, md);
            }
        }

        request.setConfig(reqConfig);

        // no need to release the connection explicitly as this is handled
        // automatically. The client itself must be closed though.
        try (CloseableHttpClient httpclient = builder.build()) {
            return httpclient.execute(request, responseHandler);
        }
    }

    private void addCookiesToRequest(HttpRequestBase request, Metadata md) {
        String[] cookieStrings = md.getValues(RESPONSE_COOKIES_HEADER, protocolMetadataPrefix);
        if (cookieStrings != null && cookieStrings.length > 0) {
            List<Cookie> cookies;
            try {
                cookies = CookieConverter.getCookies(cookieStrings, request.getURI().toURL());
                for (Cookie c : cookies) {
                    request.addHeader("Cookie", c.getName() + "=" + c.getValue());
                }
            } catch (MalformedURLException e) { // Bad url , nothing to do
            }
        }
    }

    protected void addHeadersToRequest(HttpRequestBase request, Metadata md) {
        String[] headerStrings = md.getValues(SET_HEADER_BY_REQUEST, protocolMetadataPrefix);
        if ((headerStrings != null) && (headerStrings.length > 0)) {
            for (String hs : headerStrings) {
                KeyValue h = KeyValue.build(hs);
                request.addHeader(h.getKey(), h.getValue());
            }
        }
    }

    @Override
    public ProtocolResponse handleResponse(HttpResponse response) throws IOException {
        return handleResponseWithContentLimit(response, globalMaxContent);
    }

    public ProtocolResponse handleResponseWithContentLimit(HttpResponse response, int maxContent)
            throws IOException {
        StatusLine statusLine = response.getStatusLine();
        int status = statusLine.getStatusCode();

        StringBuilder verbatim = new StringBuilder();
        if (storeHttpHeaders) {
            verbatim.append(statusLine).append("\r\n");
        }

        Metadata metadata = new Metadata();
        HeaderIterator iter = response.headerIterator();
        while (iter.hasNext()) {
            Header header = iter.nextHeader();
            if (storeHttpHeaders) {
                verbatim.append(header.toString()).append("\r\n");
            }
            metadata.addValue(header.getName().toLowerCase(Locale.ROOT), header.getValue());
        }

        MutableBoolean trimmed = new MutableBoolean();

        byte[] bytes = new byte[] {};

        if (!Status.REDIRECTION.equals(Status.fromHTTPCode(status))) {
            bytes = HttpProtocol.toByteArray(response.getEntity(), maxContent, trimmed);
            if (trimmed.booleanValue()) {
                metadata.setValue(ProtocolResponse.TRIMMED_RESPONSE_KEY, "true");
                LOG.warn("HTTP content trimmed to {}", bytes.length);
            }
        }

        if (storeHttpHeaders) {
            verbatim.append("\r\n");
            metadata.setValue(ProtocolResponse.RESPONSE_HEADERS_KEY, verbatim.toString());
        }

        return new ProtocolResponse(bytes, status, metadata);
    }

    private ResponseHandler<ProtocolResponse> getResponseHandlerWithContentLimit(
            int pageMaxContent) {
        return new ResponseHandler<>() {
            public ProtocolResponse handleResponse(final HttpResponse response) throws IOException {
                return handleResponseWithContentLimit(response, pageMaxContent);
            }
        };
    }

    @Nullable
    private static byte[] toByteArray(
            final HttpEntity entity, int maxContent, MutableBoolean trimmed) throws IOException {

        if (entity == null) {
            return new byte[] {};
        }

        final InputStream instream = entity.getContent();
        if (instream == null) {
            return null;
        }
        Args.check(
                (entity.getContentLength() <= Constants.MAX_ARRAY_SIZE)
                        || (maxContent >= 0 && maxContent <= Constants.MAX_ARRAY_SIZE),
                "HTTP entity too large to be buffered in memory");
        int reportedLength = (int) entity.getContentLength();
        // set default size for buffer: 100 KB
        int bufferInitSize = 102400;
        if (reportedLength != -1) {
            bufferInitSize = reportedLength;
        }
        // avoid init of too large a buffer when we will trim anyway
        if (maxContent != -1 && bufferInitSize > maxContent) {
            bufferInitSize = maxContent;
        }
        final ByteArrayBuffer buffer = new ByteArrayBuffer(bufferInitSize);
        final byte[] tmp = new byte[4096];
        int lengthRead;
        while ((lengthRead = instream.read(tmp)) != -1) {
            // check whether we need to trim
            if (maxContent != -1 && buffer.length() + lengthRead > maxContent) {
                buffer.append(tmp, 0, maxContent - buffer.length());
                trimmed.setValue(true);
                break;
            }
            buffer.append(tmp, 0, lengthRead);
        }
        return buffer.toByteArray();
    }

    public static void main(String[] args) throws Exception {
        Protocol.main(new HttpProtocol(), args);
    }
}
