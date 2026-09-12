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
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import kotlin.Pair;
import okhttp3.Call;
import okhttp3.CompressionInterceptor;
import okhttp3.Connection;
import okhttp3.ConnectionPool;
import okhttp3.Credentials;
import okhttp3.EventListener;
import okhttp3.EventListener.Factory;
import okhttp3.Gzip;
import okhttp3.Handshake;
import okhttp3.Headers;
import okhttp3.HttpUrl;
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
import okhttp3.brotli.Brotli;
import okhttp3.zstd.Zstd;
import okio.BufferedSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.http.HttpHeaders;
import org.apache.http.cookie.Cookie;
import org.apache.storm.Config;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilters;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.IPFilterRules;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.protocol.ProtocolResponse.TrimmedContentReason;
import org.apache.stormcrawler.proxy.SCProxy;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.CookieConverter;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpProtocol.class);

    /**
     * Default maximum number of redirect hops followed when {@code http.allow.redirects} is
     * enabled. A chain which does not end within this many hops returns its last redirect response,
     * which the caller handles like it does when redirect following is off.
     */
    private static final int DEFAULT_MAX_REDIRECT_HOPS = 5;

    private final MediaType json = MediaType.parse("application/json; charset=utf-8");

    private OkHttpClient client;

    private int globalMaxContent;

    private int completionTimeout = -1;

    /** Accept partially fetched content as trimmed content */
    private boolean partialContentAsTrimmed = false;

    /** Redirect targets run through the URL filters before a hop is taken. */
    private URLFilters urlFilters = URLFilters.emptyURLFilters;

    /** Whether redirect responses are followed, from {@code http.allow.redirects}. */
    private boolean followRedirects = false;

    /** Maximum number of redirect hops followed, from {@code http.allow.redirects.max}. */
    private int maxRedirectHops = DEFAULT_MAX_REDIRECT_HOPS;

    private final List<KeyValue> customRequestHeaders = new LinkedList<>();

    // track the time spent for each URL in DNS resolution
    private final Map<String, Long> DNStimes = new ConcurrentHashMap<>();

    // makes sure that a missing cookie origin is reported once and not for every url
    private final AtomicBoolean missingCookieOriginLogged = new AtomicBoolean();

    // makes sure that withheld cookies are reported once and not for every url
    private final AtomicBoolean withheldCookiesLogged = new AtomicBoolean();

    // makes sure that withheld request headers are reported once and not for every url
    private final AtomicBoolean withheldRequestHeadersLogged = new AtomicBoolean();

    /** Default for http.credentials.headers: header names (lower case) carrying credentials. */
    private static final Set<String> DEFAULT_CREDENTIAL_HEADERS =
            Set.of(
                    HttpHeaders.AUTHORIZATION.toLowerCase(Locale.ROOT),
                    HttpHeaders.PROXY_AUTHORIZATION.toLowerCase(Locale.ROOT),
                    // the cookie header is not a constant in HttpHeaders
                    "cookie",
                    "x-api-key");

    // lower case header names considered to carry credentials
    private Set<String> credentialHeaders = DEFAULT_CREDENTIAL_HEADERS;

    // request headers withheld from servers which were not authenticated
    private final List<KeyValue> credentialRequestHeaders = new LinkedList<>();

    // http.trust.everything: accept any certificate chain
    private boolean trustEverything = false;

    // http.verify.hostnames: check the certificate against the host name contacted
    private boolean verifyHostnames = true;

    // http.credentials.allow.insecure
    private boolean insecureCredentialsAllowed = false;

    /** Tags a request whose Proxy-Authorization was set by the proxy authenticator. */
    private enum ProxyAuthenticated {
        INSTANCE
    }

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

    // package-private so that the OkHttpTrustEverythingTest can check the protocol
    static final SSLContext trustAllSslContext;

    static {
        try {
            trustAllSslContext = SSLContext.getInstance("TLS");
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

        this.trustEverything = ConfUtils.getBoolean(conf, "http.trust.everything", false);
        this.verifyHostnames = ConfUtils.getBoolean(conf, "http.verify.hostnames", true);
        this.insecureCredentialsAllowed =
                ConfUtils.getBoolean(conf, "http.credentials.allow.insecure", false);

        // http.credentials.headers adds names to the built-in ones, it cannot remove them
        final List<String> configuredCredentialHeaders =
                ConfUtils.loadListFromConf("http.credentials.headers", conf);
        if (!configuredCredentialHeaders.isEmpty()) {
            final Set<String> names = new HashSet<>(DEFAULT_CREDENTIAL_HEADERS);
            for (String name : configuredCredentialHeaders) {
                if (StringUtils.isNotBlank(name)) {
                    names.add(name.trim().toLowerCase(Locale.ROOT));
                }
            }
            credentialHeaders = names;
        }

        if (trustEverything) {
            LOG.warn(
                    "http.trust.everything is enabled: TLS certificate chains are accepted without "
                            + "validation, the identity of the servers is not authenticated. Anybody "
                            + "able to answer for the host name receives everything sent to them.");
        }
        if (!verifyHostnames) {
            LOG.warn(
                    "http.verify.hostnames is disabled: the certificates are not checked against "
                            + "the host name either, the identity of the servers is not authenticated.");
        }

        /*
         * Redirects are followed in getProtocolOutput and not by the client:
         * every target has to pass through the URL filters first, so that the
         * scheme exclusions, host confinement and depth rules configured for
         * the crawl also apply to the hops a fetched page steers the fetcher
         * to. The client must therefore never follow them on its own.
         */
        final boolean allowRedirects = ConfUtils.getBoolean(conf, "http.allow.redirects", false);
        this.followRedirects = allowRedirects;
        this.maxRedirectHops =
                Math.max(
                        1,
                        ConfUtils.getInt(
                                conf, "http.allow.redirects.max", DEFAULT_MAX_REDIRECT_HOPS));
        urlFilters = URLFilters.fromConf(conf);
        builder =
                new OkHttpClient.Builder()
                        .retryOnConnectionFailure(
                                ConfUtils.getBoolean(
                                        conf, "http.retry.on.connection.failure", true))
                        .followRedirects(false)
                        .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .readTimeout(timeout, TimeUnit.MILLISECONDS);

        if (completionTimeout >= 0) {
            builder.callTimeout(completionTimeout, TimeUnit.SECONDS);
        }

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
        if (!protocols.isEmpty()) {
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

        // use a basic auth? the header is withheld unless the server was authenticated
        if (StringUtils.isNotBlank(basicAuthUser)) {
            final String basicAuthPass = ConfUtils.getString(conf, "http.basicauth.password", "");
            final String encoding =
                    Base64.getEncoder()
                            .encodeToString(
                                    (basicAuthUser + ":" + basicAuthPass)
                                            .getBytes(StandardCharsets.UTF_8));
            credentialRequestHeaders.add(
                    new KeyValue(HttpHeaders.AUTHORIZATION, "Basic " + encoding));
        }

        for (KeyValue customHeader : customHeaders) {
            if (isCredentialHeader(customHeader.getKey())) {
                credentialRequestHeaders.add(customHeader);
            } else {
                customRequestHeaders.add(customHeader);
            }
        }

        if (!credentialRequestHeaders.isEmpty() || useCookies) {
            if (trustEverything && !insecureCredentialsAllowed) {
                LOG.warn(
                        "Credentials configured with http.basicauth.*, credential headers in "
                                + "http.custom.headers and cookies are withheld from every request "
                                + "because the servers are not authenticated (http.trust.everything). "
                                + "Set http.credentials.allow.insecure to true to send them anyway.");
            } else if (!insecureCredentialsAllowed) {
                LOG.info(
                        "Credentials configured with http.basicauth.*, credential headers in "
                                + "http.custom.headers and cookies are sent only over https:// urls "
                                + "with certificate validation. They are withheld from cleartext "
                                + "http:// urls and from https:// urls when http.trust.everything "
                                + "is enabled; set http.credentials.allow.insecure to true to send "
                                + "them there anyway.");
            }
        }

        // optionally block connections to forbidden IP address ranges
        // (e.g. localhost/loopback, private/site-local addresses), see
        // https://github.com/apache/stormcrawler/issues/1107
        final IPFilterRules ipFilterRules = new IPFilterRules(conf);
        if (!ipFilterRules.isEmpty()) {
            builder.addNetworkInterceptor(new HTTPFilterIPAddressInterceptor(ipFilterRules));
            if (proxyManager != null) {
                LOG.info(
                        "http.filter.ipaddress.* do not apply to fetches through a proxy, the "
                                + "proxy resolves the target host and its own egress rules decide "
                                + "which addresses are reached");
            }
        }

        // getProtocolOutput only filters the initial request, the redirect follower copies
        // the headers onto the next hop without re-checking. Registered before the
        // HTTPHeadersInterceptor so that the request headers it records are the ones sent.
        builder.addNetworkInterceptor(
                chain -> {
                    Request hop = chain.request();
                    if (!credentialsAllowed(hop.url())) {
                        // the proxy authenticator's header is read by the proxy on a cleartext
                        // hop, anywhere else Proxy-Authorization reaches the crawled server
                        final boolean forProxy =
                                hop.tag(ProxyAuthenticated.class) != null && !hop.url().isHttps();
                        Request.Builder stripped = hop.newBuilder();
                        for (String name : new HashSet<>(hop.headers().names())) {
                            if (forProxy
                                    && HttpHeaders.PROXY_AUTHORIZATION.equalsIgnoreCase(name)) {
                                continue;
                            }
                            if (isCredentialHeader(name)) {
                                stripped.removeHeader(name);
                            }
                        }
                        hop = stripped.build();
                    }
                    return chain.proceed(hop);
                });

        if (storeHttpHeaders) {
            builder.addNetworkInterceptor(new HTTPHeadersInterceptor());
        }

        if (trustEverything) {
            builder.sslSocketFactory(trustAllSslSocketFactory, (X509TrustManager) trustAllCerts[0]);
        }
        if (!verifyHostnames) {
            builder.hostnameVerifier((hostname, session) -> true);
        }

        builder.eventListenerFactory(
                new Factory() {
                    @Override
                    public EventListener create(Call call) {
                        return new DNSResolutionListener(DNStimes);
                    }
                });

        // enable support for Zstd, Brotli, Gzip Content-Encoding
        builder.addInterceptor(
                new CompressionInterceptor(Zstd.INSTANCE, Brotli.INSTANCE, Gzip.INSTANCE));

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

    private void addCookiesToRequest(Builder rb, String url, Metadata md, boolean sendCredentials) {
        final String[] cookieStrings =
                md.getValues(RESPONSE_COOKIES_HEADER, protocolMetadataPrefix);
        if (cookieStrings == null || cookieStrings.length == 0) {
            return;
        }
        if (!sendCredentials) {
            if (withheldCookiesLogged.compareAndSet(false, true)) {
                LOG.warn(
                        "Cookies are withheld because the server is not authenticated. Set "
                                + "http.credentials.allow.insecure to true to send them anyway.");
            }
            return;
        }
        try {
            final List<Cookie> cookies =
                    CookieConverter.getCookies(
                            cookieStrings, getCookieOrigin(md, url), URLUtil.toURL(url));
            for (Cookie c : cookies) {
                rb.addHeader("Cookie", c.getName() + "=" + c.getValue());
            }
        } catch (MalformedURLException e) { // Bad url , nothing to do
        }
    }

    /**
     * Returns the url whose response set the cookies, or null when it was not recorded, in which
     * case cookies without a domain attribute are not sent.
     */
    private URL getCookieOrigin(Metadata md, String url) {
        final String origin = md.getFirstValue(RESPONSE_COOKIES_ORIGIN, protocolMetadataPrefix);
        if (StringUtils.isBlank(origin)) {
            if (missingCookieOriginLogged.compareAndSet(false, true)) {
                LOG.warn(
                        "No {}{} for {}, cookies without a domain attribute are not sent. Add {}{}"
                                + " to metadata.transfer and metadata.persist next to {}{}.",
                        protocolMetadataPrefix,
                        RESPONSE_COOKIES_ORIGIN,
                        url,
                        protocolMetadataPrefix,
                        RESPONSE_COOKIES_ORIGIN,
                        protocolMetadataPrefix,
                        RESPONSE_COOKIES_HEADER);
            } else {
                LOG.debug("No {}{} for {}", protocolMetadataPrefix, RESPONSE_COOKIES_ORIGIN, url);
            }
            return null;
        }
        try {
            return URLUtil.toURL(origin);
        } catch (MalformedURLException e) {
            LOG.warn(
                    "Invalid {}{} {} for {}",
                    protocolMetadataPrefix,
                    RESPONSE_COOKIES_ORIGIN,
                    origin,
                    url);
            return null;
        }
    }

    /** Whether the header carries credentials, see http.credentials.headers. */
    private boolean isCredentialHeader(String name) {
        if (name == null) {
            return false;
        }
        final String normalised = name.trim().toLowerCase(Locale.ROOT);
        return credentialHeaders.contains(normalised);
    }

    /**
     * Whether credentials may be sent to the url. The server is only authenticated over https with
     * both the certificate chain and the host name checked; http.credentials.allow.insecure sends
     * them anyway.
     */
    // package-private for the decision matrix in OkHttpTrustEverythingTest
    boolean credentialsAllowed(String url) {
        if (insecureCredentialsAllowed) {
            return true;
        }
        final HttpUrl parsed = HttpUrl.parse(url);
        return parsed != null && credentialsAllowed(parsed);
    }

    private boolean credentialsAllowed(HttpUrl url) {
        return insecureCredentialsAllowed || (url.isHttps() && !trustEverything && verifyHostnames);
    }

    protected void addHeadersToRequest(Builder rb, Metadata md, boolean sendCredentials) {
        final String[] headerStrings = md.getValues(SET_HEADER_BY_REQUEST, protocolMetadataPrefix);

        if (headerStrings != null && headerStrings.length > 0) {
            for (String hs : headerStrings) {
                KeyValue h = KeyValue.build(hs);
                if (!sendCredentials && isCredentialHeader(h.getKey())) {
                    if (withheldRequestHeadersLogged.compareAndSet(false, true)) {
                        LOG.warn(
                                "Header {} set by request is withheld because the server is not "
                                        + "authenticated. Set http.credentials.allow.insecure to "
                                        + "true to send it anyway.",
                                h.getKey());
                    }
                    continue;
                }
                rb.addHeader(h.getKey(), h.getValue());
            }
        }
    }

    /** Removes every credential header except Proxy-Authorization (which is for the proxy). */
    private void stripCredentialHeaders(Request.Builder builder) {
        for (String name : new HashSet<>(builder.build().headers().names())) {
            if (HttpHeaders.PROXY_AUTHORIZATION.equalsIgnoreCase(name)) {
                continue;
            }
            if (isCredentialHeader(name)) {
                builder.removeHeader(name);
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
                    // create a new builder from the existing client to avoid
                    // polluting shared state across concurrent requests
                    OkHttpClient.Builder localBuilder = client.newBuilder();

                    // format SCProxy into native Java proxy
                    Proxy proxy =
                            new Proxy(
                                    Proxy.Type.valueOf(prox.getProtocol().toUpperCase(Locale.ROOT)),
                                    new InetSocketAddress(
                                            prox.getAddress(), Integer.parseInt(prox.getPort())));

                    // set proxy in builder
                    localBuilder.proxy(proxy);

                    // conditionally add proxy authentication
                    if (StringUtils.isNotBlank(prox.getUsername())) {
                        // authenticates against the proxy, not the crawled server,
                        // so it is sent whatever credentialsAllowed decides
                        localBuilder.proxyAuthenticator(
                                (Route route, Response response) -> {
                                    String credential =
                                            Credentials.basic(
                                                    prox.getUsername(), prox.getPassword());
                                    return response.request()
                                            .newBuilder()
                                            .header(HttpHeaders.PROXY_AUTHORIZATION, credential)
                                            .tag(
                                                    ProxyAuthenticated.class,
                                                    ProxyAuthenticated.INSTANCE)
                                            .build();
                                });
                    }

                    // save start time for debugging speed impact of client build
                    long buildStart = System.currentTimeMillis();

                    // create new local client from local builder using proxy
                    localClient = localBuilder.build();

                    LOG.debug(
                            "time to build okhttp client with proxy: {}ms",
                            System.currentTimeMillis() - buildStart);
                }

                LOG.debug("fetching with proxy {} - {} ", url, prox.toString());
            }
        }

        final boolean sendCredentials = credentialsAllowed(url);

        final Builder rb = new Request.Builder().url(url);
        customRequestHeaders.forEach(
                (k) -> {
                    rb.header(k.getKey(), k.getValue());
                });
        if (sendCredentials) {
            credentialRequestHeaders.forEach(
                    (k) -> {
                        rb.header(k.getKey(), k.getValue());
                    });
        } else if (!credentialRequestHeaders.isEmpty()
                && withheldRequestHeadersLogged.compareAndSet(false, true)) {
            LOG.warn(
                    "Configured credential headers (http.basicauth.*, http.custom.headers) are "
                            + "withheld because {} is not authenticated. Set "
                            + "http.credentials.allow.insecure to true to send them anyway.",
                    url.startsWith("https")
                            ? "https with http.trust.everything or without http.verify.hostnames"
                            : "cleartext http");
        }

        int pageMaxContent = globalMaxContent;

        if (metadata != null) {
            addHeadersToRequest(rb, metadata, sendCredentials);

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
                    int metadataLimit = Integer.parseInt(pageMaxContentStr);
                    // -1 means no limit, anything below is invalid and ignored
                    if (metadataLimit >= -1 && (metadataLimit != -1 || globalMaxContent == -1)) {
                        pageMaxContent = metadataLimit;
                    }
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid http.content.limit in metadata: {}", pageMaxContentStr);
                }
            }

            if (useCookies) {
                addCookiesToRequest(rb, url, metadata, sendCredentials);
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

        /*
         * Follow redirect responses manually: every target runs through the
         * URL filters before the hop is taken, and a target which is rejected
         * ends the chain - the redirect response is then returned as is and
         * the caller handles it like it does when http.allow.redirects is
         * off. The final URL is recorded in the response metadata so that
         * callers can tell that the content is not from the URL they asked
         * for.
         */
        Response lastResponse = null;
        Call call = null;
        Request currentRequest = request;
        String currentUrl = url;
        // the proxy is chosen per fetch above, so the chain has to use the
        // same client the first request went out on
        final OkHttpClient fetchClient = localClient;
        // every hop creates its own Call and DNS timing entry: track them all
        // so intermediate entries are cleaned up too, including on exceptions
        final List<Call> hopCalls = new ArrayList<>();

        try {
            for (int hops = 0; hops <= maxRedirectHops; hops++) {
                if (lastResponse != null) {
                    // release the connection before issuing the next request
                    lastResponse.close();
                    lastResponse = null;
                }
                call = fetchClient.newCall(currentRequest);
                hopCalls.add(call);
                try {
                    lastResponse = call.execute();
                } catch (IOException | RuntimeException e) {
                    DNStimes.remove(call.toString());
                    throw e;
                }

                if (hops == maxRedirectHops) {
                    if (isRedirect(lastResponse)) {
                        LOG.warn("More than {} redirect hops for {}", maxRedirectHops, url);
                    }
                    break;
                }
                if (!followRedirects || !isRedirect(lastResponse)) {
                    break;
                }

                final String location = lastResponse.header(HttpHeaders.LOCATION);
                if (StringUtils.isBlank(location)) {
                    LOG.debug(
                            "Got redirect response {} for {} without location",
                            lastResponse.code(),
                            url);
                    break;
                }

                final HttpUrl target = currentRequest.url().resolve(location);
                if (target == null) {
                    LOG.warn(
                            "Redirect target {} could not be resolved against {}",
                            location,
                            currentUrl);
                    break;
                }

                final Metadata sourceMetadata = metadata != null ? metadata : new Metadata();
                final String filtered =
                        urlFilters.filter(
                                currentRequest.url().url(), sourceMetadata, target.toString());
                if (filtered == null) {
                    LOG.info("Redirect target {} rejected by the URL filters", target);
                    break;
                }

                final HttpUrl accepted = HttpUrl.parse(filtered);
                if (accepted == null) {
                    LOG.warn("Filtered redirect target {} is not a URL", filtered);
                    break;
                }

                // an https to http downgrade is followed like the client does, the
                // credential headers are stripped below unless
                // http.credentials.allow.insecure is set
                if (currentRequest.url().isHttps() && !accepted.isHttps()) {
                    LOG.warn("Redirect target {} downgrades https to http", accepted);
                }

                final Request.Builder followBuilder = currentRequest.newBuilder().url(accepted);
                final boolean hostChanged = !currentRequest.url().host().equals(accepted.host());
                if (hostChanged || !credentialsAllowed(accepted)) {
                    // a redirect must not hand the credentials of one host to
                    // another, even between two validated HTTPS hosts, and must
                    // not send them to a server which is not authenticated
                    // (cleartext http://, trust-all or unverified host unless
                    // opted in)
                    stripCredentialHeaders(followBuilder);
                }
                // for 303, and for 301 or 302 after a POST, the request is
                // repeated as a GET, like the client would do
                final int code = lastResponse.code();
                if (code == 303
                        || ((code == 301 || code == 302)
                                && !"GET".equals(currentRequest.method()))) {
                    followBuilder.method("GET", null);
                }
                currentRequest = followBuilder.build();
                currentUrl = filtered;
            }

            final Response response = lastResponse;
            final Call executedCall = call;

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

            if (isRedirect(response)) {
                // a chain stopped after an intermediate hop returns a redirect
                // whose Location is relative to the last request, not the
                // original URL: resolve it there so callers (FetcherBolt)
                // resolve the same target instead of one level too high
                final String location = responsemetadata.getFirstValue("location");
                if (StringUtils.isNotBlank(location)) {
                    final HttpUrl resolved = response.request().url().resolve(location);
                    if (resolved != null) {
                        responsemetadata.setValue("location", resolved.toString());
                    }
                }
            }

            if (!currentUrl.equals(url)) {
                responsemetadata.setValue(ProtocolResponse.REDIRECTED_TO_KEY, currentUrl);
            }

            // the Set-Cookie header does not say which host sent it: record the url of this
            // response so that the cookies can be scoped to it when they are sent back. The
            // key is dropped first so that a server sending a header of that name can not
            // forge the origin of the cookies inherited from another page.
            responsemetadata.remove(RESPONSE_COOKIES_ORIGIN);
            if (responsemetadata.getFirstValue(RESPONSE_COOKIES_HEADER) != null) {
                responsemetadata.setValue(
                        RESPONSE_COOKIES_ORIGIN, response.request().url().toString());
            }

            final MutableObject<TrimmedContentReason> trimmed =
                    new MutableObject<>(TrimmedContentReason.NOT_TRIMMED);
            final byte[] bytes = toByteArray(response.body(), pageMaxContent, trimmed);
            if (trimmed.get() != TrimmedContentReason.NOT_TRIMMED) {
                if (!executedCall.isCanceled()) {
                    executedCall.cancel();
                }
                responsemetadata.setValue(ProtocolResponse.TRIMMED_RESPONSE_KEY, "true");
                responsemetadata.setValue(
                        ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY,
                        trimmed.get().toString().toLowerCase(Locale.ROOT));
                LOG.warn("HTTP content trimmed to {} (reason: {})", bytes.length, trimmed.get());
            }

            final Long dnsResolution = DNStimes.remove(executedCall.toString());
            if (dnsResolution != null) {
                responsemetadata.setValue("metrics.dns.resolution.msec", dnsResolution.toString());
            }
            // drop intermediate hops' timing entries: only the final hop is reported
            for (Call hopCall : hopCalls) {
                if (hopCall != executedCall) {
                    DNStimes.remove(hopCall.toString());
                }
            }

            return new ProtocolResponse(bytes, response.code(), responsemetadata);
        } finally {
            if (lastResponse != null) {
                lastResponse.close();
            }
            // exception paths: no response to report from, but the hops'
            // DNS entries must not accumulate over a long crawl
            for (Call hopCall : hopCalls) {
                DNStimes.remove(hopCall.toString());
            }
        }
    }

    /** Checks whether the response is a redirect whose Location must be resolved. */
    private static boolean isRedirect(Response response) {
        final int code = response.code();
        return code == 301 || code == 302 || code == 303 || code == 307 || code == 308;
    }

    private byte[] toByteArray(
            final ResponseBody responseBody,
            int maxContent,
            MutableObject<TrimmedContentReason> trimmed)
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
                    if (e instanceof InterruptedIOException) {
                        // thrown by OkHttp if the call timeout is hit
                        trimmed.setValue(TrimmedContentReason.TIME);
                    } else {
                        trimmed.setValue(TrimmedContentReason.DISCONNECT);
                    }
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
            bytesRequested = source.getBuffer().size();
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

    /**
     * Network interceptor blocking connections to IP addresses rejected by the configured {@link
     * IPFilterRules}. The IP address is only known once the connection has been established, hence
     * the filtering happens at the protocol level rather than by filtering URLs.
     *
     * <p>Fetches through a proxy are not filtered: the connection is established to the proxy,
     * which resolves the target host itself, so the only address known here is the proxy's own.
     */
    static class HTTPFilterIPAddressInterceptor implements Interceptor {

        private final IPFilterRules rules;

        HTTPFilterIPAddressInterceptor(IPFilterRules rules) {
            this.rules = rules;
        }

        @NotNull
        @Override
        public Response intercept(Interceptor.Chain chain) throws IOException {
            final Connection connection = Objects.requireNonNull(chain.connection());
            final Request request = chain.request();

            if (connection.route().proxy().type() != Proxy.Type.DIRECT) {
                return chain.proceed(request);
            }

            final InetAddress address = connection.socket().getInetAddress();

            if (rules.accept(address)) {
                return chain.proceed(request);
            }

            final String hostAddress = address == null ? "unknown" : address.getHostAddress();
            LOG.warn("Blocked connection to IP address {}: {}", hostAddress, request.url());
            throw new IOException("Forbidden connection to IP address " + hostAddress);
        }
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

    public static void main(String[] args) throws Exception {
        org.apache.stormcrawler.protocol.Protocol.main(new HttpProtocol(), args);
    }
}
