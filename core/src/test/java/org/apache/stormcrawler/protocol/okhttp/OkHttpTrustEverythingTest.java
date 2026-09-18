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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for https://github.com/apache/stormcrawler/issues/2079: the trust-all
 * configuration of the okhttp protocol must be an explicit choice, must not disable hostname
 * verification and must not disclose credentials to servers which are not authenticated.
 */
/*
 * The keystores under src/test/resources/ssl are self-signed PKCS12 keystores, generated with
 * (JDK keytool, passwords are the "changeit" default, valid for 9000 days):
 *
 * keytool -genkeypair -alias localhost -keyalg RSA -keysize 2048 -validity 9000 \
 *   -dname "CN=localhost" -ext "SAN=dns:localhost,ip:127.0.0.1" \
 *   -keystore localhost.p12 -storetype PKCS12 -storepass changeit -keypass changeit
 *
 * keytool -genkeypair -alias otherhost -keyalg RSA -keysize 2048 -validity 9000 \
 *   -dname "CN=otherhost.invalid" -ext "SAN=dns:otherhost.invalid" \
 *   -keystore otherhost.p12 -storetype PKCS12 -storepass changeit -keypass changeit
 */
class OkHttpTrustEverythingTest {

    private static final String KEYSTORE_PASSWORD = "changeit";

    /** Certificate issued for localhost: valid for the host the tests connect to. */
    private static final String LOCALHOST_KEYSTORE = "/ssl/localhost.p12";

    /** Certificate issued for another host name: trusted under trust-all, wrong name. */
    private static final String OTHERHOST_KEYSTORE = "/ssl/otherhost.p12";

    private WireMockServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Test
    void trustAllContextUsesTls() {
        assertEquals(
                "TLS",
                HttpProtocol.trustAllSslContext.getProtocol(),
                "the trust-all SSLContext should be a TLS context");
    }

    @Test
    void selfSignedCertificateRejectedByDefault() throws Exception {
        // http.trust.everything defaults to false: an unvalidatable certificate
        // must not be accepted
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(config());
        assertThrows(
                SSLHandshakeException.class,
                () -> fetch(protocol, "/default"),
                "self-signed certificates must be rejected by default");
    }

    @Test
    void trustAllFetchesServerWithSelfSignedCertificate() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/trustall");
        assertEquals(200, response.getStatusCode(), "the self-signed certificate is accepted");
    }

    @Test
    void hostnameIsStillVerified() throws Exception {
        // the certificate is issued for another host name: trusting any
        // certificate must not imply accepting any name
        final Config conf = config();
        conf.put("http.trust.everything", true);
        startServer(OTHERHOST_KEYSTORE);
        assertThrows(
                SSLPeerUnverifiedException.class,
                () -> fetch(protocol(conf), "/hostname"),
                "the hostname verifier should not accept any name unconditionally");
    }

    @Test
    void hostnameVerificationCanBeDisabledSeparately() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.verify.hostnames", false);
        startServer(OTHERHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/nohostnamecheck");
        assertEquals(200, response.getStatusCode(), "the name mismatch is accepted as configured");
    }

    @Test
    void basicAuthIsWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/basicauth");
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/basicauth")).withoutHeader("Authorization"));
    }

    @Test
    void basicAuthIsSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/basicauth");
        final String expected =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString("user:secret".getBytes(StandardCharsets.UTF_8));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/basicauth"))
                        .withHeader("Authorization", equalTo(expected)));
    }

    @Test
    void credentialCustomHeadersAreWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret", "X-Trace=public"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/customheaders");
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/customheaders")).withoutHeader("X-Api-Key"));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/customheaders"))
                        .withHeader("X-Trace", equalTo("public")));
    }

    @Test
    void credentialCustomHeadersAreSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret", "X-Trace=public"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/customheaders");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/customheaders"))
                        .withHeader("X-Api-Key", equalTo("s3cret")));
    }

    @Test
    void cookiesAreWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.use.cookies", true);
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/cookies", metadata());
        server.verify(1, getRequestedFor(urlPathEqualTo("/cookies")).withoutHeader("Cookie"));
    }

    @Test
    void cookiesAreSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.use.cookies", true);
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/cookies", metadata());
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/cookies")).withHeader("Cookie", equalTo("sid=x")));
    }

    @Test
    void basicAuthIsWithheldOverCleartextHttp() throws Exception {
        // a cleartext http:// request does not authenticate the server either
        final Config conf = config();
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response =
                fetchUrl(protocol(conf), httpUrl("/cleartext"), new Metadata());
        assertEquals(200, response.getStatusCode(), "the connection must succeed");
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/cleartext")).withoutHeader("Authorization"));
    }

    @Test
    void basicAuthIsSentOverCleartextHttpWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        startServer(LOCALHOST_KEYSTORE);
        fetchUrl(protocol(conf), httpUrl("/cleartext"), new Metadata());
        final String expected = "Basic " + base64("user:secret");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/cleartext"))
                        .withHeader("Authorization", equalTo(expected)));
    }

    @Test
    void credentialHeaderNamesCanBeConfigured() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.headers", List.of("X-Auth-Token"));
        conf.put("http.custom.headers", List.of("X-Auth-Token=token1", "X-Other=plain"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/configuredheaders");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/configuredheaders"))
                        .withoutHeader("X-Auth-Token"));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/configuredheaders"))
                        .withHeader("X-Other", equalTo("plain")));
    }

    /** http.credentials.headers adds to the built-in names, it does not replace them. */
    @Test
    void configuredHeaderNamesDoNotDropTheBuiltInOnes() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.headers", List.of("x-auth-token"));
        conf.put(
                "http.custom.headers",
                List.of("Authorization=Basic c2VjcmV0", "X-Api-Key=key1", "Cookie=sid=x"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/builtinheaders");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/builtinheaders"))
                        .withoutHeader("Authorization")
                        .withoutHeader("X-Api-Key")
                        .withoutHeader("Cookie"));
    }

    /**
     * Credentials must also be withheld when hostname verification is switched off: a valid
     * certificate for a different name, accepted only because the name is not checked, does not
     * authenticate the server any more than a trust-all chain does.
     */
    @Test
    void credentialsAreWithheldWhenHostnameVerificationIsDisabled() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.verify.hostnames", false);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        startServer(OTHERHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/nohostnamecheck");
        assertEquals(200, response.getStatusCode(), "the connection succeeds as configured");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/nohostnamecheck")).withoutHeader("Authorization"));
    }

    /** The opt-in still sends them on that path. */
    @Test
    void credentialsAreSentWhenHostnameVerificationIsDisabledAndInsecureAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.verify.hostnames", false);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        startServer(OTHERHOST_KEYSTORE);
        fetch(protocol(conf), "/nohostnamecheckinsecure");
        final String expected = "Basic " + base64("user:secret");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/nohostnamecheckinsecure"))
                        .withHeader("Authorization", equalTo(expected)));
    }

    /** Credential headers set per request through metadata go through the same policy. */
    @Test
    void credentialHeaderSetByRequestIsWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        startServer(LOCALHOST_KEYSTORE);
        final Metadata md = new Metadata();
        md.setValue("protocol.set-header", "X-Api-Key=s3cret");
        md.setValue("protocol.set-header", "X-Trace=public");
        fetch(protocol(conf), "/setheaders", md);
        server.verify(1, getRequestedFor(urlPathEqualTo("/setheaders")).withoutHeader("X-Api-Key"));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/setheaders"))
                        .withHeader("X-Trace", equalTo("public")));
    }

    @Test
    void credentialHeaderSetByRequestIsSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        startServer(LOCALHOST_KEYSTORE);
        final Metadata md = new Metadata();
        md.setValue("protocol.set-header", "X-Api-Key=s3cret");
        fetch(protocol(conf), "/setheadersallowed", md);
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/setheadersallowed"))
                        .withHeader("X-Api-Key", equalTo("s3cret")));
    }

    /**
     * The decision itself over the full configuration matrix, including the positive case of a
     * normally validated HTTPS connection, which the WireMock tests above cannot express because
     * their certificates are self-signed.
     */
    @Test
    void credentialsAllowedDecision() {
        // default: chains validated, hostnames checked -> https is authenticated
        HttpProtocol p = protocol(config());
        assertTrue(p.credentialsAllowed("https://example.org/"), "validated https sends");
        assertFalse(p.credentialsAllowed("http://example.org/"), "cleartext never sends");
        // trust-all: chains are not validated
        final Config trustAll = config();
        trustAll.put("http.trust.everything", true);
        assertFalse(protocol(trustAll).credentialsAllowed("https://example.org/"));
        // hostname verification disabled: a name mismatch is not caught
        final Config noHostCheck = config();
        noHostCheck.put("http.verify.hostnames", false);
        assertFalse(protocol(noHostCheck).credentialsAllowed("https://example.org/"));
        // the opt-in covers every case
        final Config allowInsecure = config();
        allowInsecure.put("http.credentials.allow.insecure", true);
        assertTrue(protocol(allowInsecure).credentialsAllowed("http://example.org/"));
    }

    /**
     * An automatic redirect from trusted HTTPS to cleartext HTTP must not forward credentials: the
     * policy is enforced on every hop, not just the initial URL. The self-signed test certificate
     * is trusted via the client transport (without flipping http.trust.everything, which would also
     * flip the policy), so the initial HTTPS hop is authenticated and sends credentials while the
     * HTTP hop must not receive them.
     */
    @Test
    void credentialsAreStrippedOnHttpsToHttpRedirect() throws Exception {
        final Config conf = config();
        conf.put("http.allow.redirects", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret"));
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(conf);
        trustTestKeystore(protocol, LOCALHOST_KEYSTORE);
        server.stubFor(
                get(urlPathEqualTo("/redirect"))
                        .atPriority(1)
                        .willReturn(
                                aResponse()
                                        .withStatus(302)
                                        .withHeader("Location", httpUrl("/target"))));
        final ProtocolResponse response =
                fetchUrl(
                        protocol,
                        "https://localhost:" + server.httpsPort() + "/redirect",
                        new Metadata());
        assertEquals(200, response.getStatusCode(), "the redirect is followed");
        final String expected = "Basic " + base64("user:secret");
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/redirect"))
                        .withHeader("Authorization", equalTo(expected)));
        server.verify(1, getRequestedFor(urlPathEqualTo("/target")).withoutHeader("Authorization"));
        server.verify(1, getRequestedFor(urlPathEqualTo("/target")).withoutHeader("X-Api-Key"));
    }

    /**
     * The opt-in covers the transport of the URL the crawl targeted, not redirect hops: an https to
     * http redirect changes the origin (scheme and port), so credential headers are stripped even
     * then, like OkHttp's own redirect follower does.
     */
    @Test
    void credentialsAreStrippedOnHttpsToHttpRedirectEvenWhenInsecureAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.allow.redirects", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.basicauth.hosts", "localhost");
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret"));
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(conf);
        trustTestKeystore(protocol, LOCALHOST_KEYSTORE);
        server.stubFor(
                get(urlPathEqualTo("/redirect-allowed"))
                        .atPriority(1)
                        .willReturn(
                                aResponse()
                                        .withStatus(302)
                                        .withHeader("Location", httpUrl("/target-allowed"))));
        fetchUrl(
                protocol,
                "https://localhost:" + server.httpsPort() + "/redirect-allowed",
                new Metadata());
        server.verify(
                1, getRequestedFor(urlPathEqualTo("/target-allowed")).withoutHeader("X-Api-Key"));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/target-allowed")).withoutHeader("Authorization"));
    }

    /**
     * With http.store.headers, the request headers recorded for a hop are the ones sent on it: a
     * credential stripped from the cleartext hop must not show up in the stored request either.
     */
    @Test
    void storedRequestHeadersMatchTheStrippedHop() throws Exception {
        final Config conf = config();
        conf.put("http.allow.redirects", true);
        conf.put("http.store.headers", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret"));
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(conf);
        trustTestKeystore(protocol, LOCALHOST_KEYSTORE);
        server.stubFor(
                get(urlPathEqualTo("/redirect-stored"))
                        .atPriority(1)
                        .willReturn(
                                aResponse()
                                        .withStatus(302)
                                        .withHeader("Location", httpUrl("/target-stored"))));
        final ProtocolResponse response =
                fetchUrl(
                        protocol,
                        "https://localhost:" + server.httpsPort() + "/redirect-stored",
                        new Metadata());
        final String stored =
                response.getMetadata().getFirstValue(ProtocolResponse.REQUEST_HEADERS_KEY);
        assertTrue(stored.contains("/target-stored "), "the last hop is recorded: " + stored);
        assertFalse(
                stored.toLowerCase(Locale.ROOT).contains("x-api-key"),
                "the stripped header must not be recorded: " + stored);
    }

    /**
     * A Proxy-Authorization header configured in http.custom.headers is withheld like any other
     * credential. On a direct connection it would reach the crawled server, so a redirect to
     * cleartext HTTP must strip it too.
     */
    @Test
    void staticProxyAuthorizationIsStrippedOnHttpsToHttpRedirect() throws Exception {
        final Config conf = config();
        conf.put("http.allow.redirects", true);
        conf.put("http.custom.headers", List.of("Proxy-Authorization=Basic cHJveHk6c2VjcmV0"));
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(conf);
        trustTestKeystore(protocol, LOCALHOST_KEYSTORE);
        server.stubFor(
                get(urlPathEqualTo("/redirect-proxyauth"))
                        .atPriority(1)
                        .willReturn(
                                aResponse()
                                        .withStatus(302)
                                        .withHeader("Location", httpUrl("/target-proxyauth"))));
        fetchUrl(
                protocol,
                "https://localhost:" + server.httpsPort() + "/redirect-proxyauth",
                new Metadata());
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/redirect-proxyauth"))
                        .withHeader("Proxy-Authorization", equalTo("Basic cHJveHk6c2VjcmV0")));
        server.verify(
                1,
                getRequestedFor(urlPathEqualTo("/target-proxyauth"))
                        .withoutHeader("Proxy-Authorization"));
    }

    /**
     * Trusts the self-signed test keystore on the protocol transport without changing the
     * credential policy: http.trust.everything stays false, so credentialsAllowed still treats the
     * HTTPS URL as authenticated, while the TLS handshake succeeds.
     */
    private void trustTestKeystore(HttpProtocol protocol, String keystoreResource)
            throws Exception {
        final KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = getClass().getResourceAsStream(keystoreResource)) {
            trustStore.load(in, KEYSTORE_PASSWORD.toCharArray());
        }
        final TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        final X509TrustManager trustManager = (X509TrustManager) tmf.getTrustManagers()[0];
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] {trustManager}, new SecureRandom());
        final Field clientField = HttpProtocol.class.getDeclaredField("client");
        clientField.setAccessible(true);
        final OkHttpClient client = (OkHttpClient) clientField.get(protocol);
        final OkHttpClient trusted =
                client.newBuilder()
                        .sslSocketFactory(sslContext.getSocketFactory(), trustManager)
                        .build();
        clientField.set(protocol, trusted);
    }

    /** Metadata as an outlink would inherit it, with a cookie scoped to the server. */
    private Metadata metadata() {
        final Metadata md = new Metadata();
        md.setValue("protocol.set-cookie", "sid=x; Path=/");
        md.setValue("protocol.set-cookie-origin", "https://localhost:" + server.httpsPort() + "/");
        return md;
    }

    private String base64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private String httpUrl(String path) {
        return "http://localhost:" + server.port() + path;
    }

    private Config config() {
        final Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "test");
        conf.put("http.agent.url", "http://test.example.com");
        conf.put("http.agent.email", "test@example.com");
        conf.put("protocol.md.prefix", "protocol.");
        return conf;
    }

    private HttpProtocol protocol(Config conf) {
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    private ProtocolResponse fetch(HttpProtocol protocol, String path) throws Exception {
        return fetch(protocol, path, new Metadata());
    }

    private ProtocolResponse fetch(HttpProtocol protocol, String path, Metadata md)
            throws Exception {
        return fetchUrl(protocol, "https://localhost:" + server.httpsPort() + path, md);
    }

    private ProtocolResponse fetchUrl(HttpProtocol protocol, String url, Metadata md)
            throws Exception {
        return protocol.getProtocolOutput(url, md);
    }

    /**
     * Starts an HTTPS server on a random port presenting the certificate of the keystore. The
     * keystore is copied to a temporary file as WireMock reads it from the file system.
     */
    private void startServer(String keystoreResource) throws Exception {
        final Path keystoreFile = Files.createTempFile("wiremock-keystore", ".p12");
        keystoreFile.toFile().deleteOnExit();
        try (InputStream in =
                OkHttpTrustEverythingTest.class.getResourceAsStream(keystoreResource)) {
            Files.copy(in, keystoreFile, StandardCopyOption.REPLACE_EXISTING);
        }

        server =
                new WireMockServer(
                        WireMockConfiguration.options()
                                .dynamicPort()
                                .dynamicHttpsPort()
                                .keystorePath(keystoreFile.toAbsolutePath().toString())
                                .keystorePassword(KEYSTORE_PASSWORD)
                                .keyManagerPassword(KEYSTORE_PASSWORD)
                                .keystoreType("PKCS12")
                                .notifier(new ConsoleNotifier(false)));
        server.start();
        server.stubFor(any(anyUrl()).willReturn(ok("Success!")));
    }
}
