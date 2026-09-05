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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
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
class OkHttpTrustEverythingTest {

    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

    /** Certificate issued for localhost: valid for the host the tests connect to. */
    private static final String LOCALHOST_KEYSTORE = "/ssl/localhost.p12";

    /** Certificate issued for another host name: trusted under trust-all, wrong name. */
    private static final String OTHERHOST_KEYSTORE = "/ssl/otherhost.p12";

    private HttpsServer server;

    private RecordingHandler handler;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
            server = null;
            handler = null;
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
        // must not be accepted. Depending on the platform the handshake fails or
        // the connection is dropped while it is retried.
        startServer(LOCALHOST_KEYSTORE);
        final HttpProtocol protocol = protocol(config());
        assertThrows(
                Exception.class,
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
        // certificate must not imply accepting any name. Depending on the
        // platform the failed verification surfaces as an SSL exception or the
        // connection is dropped while it is retried.
        final Config conf = config();
        conf.put("http.trust.everything", true);
        startServer(OTHERHOST_KEYSTORE);
        assertThrows(
                Exception.class,
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
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/basicauth");
        assertEquals(200, response.getStatusCode(), "the connection must succeed");
        assertNull(
                handler.lastHeaders.get("authorization"),
                "credentials must not be sent to unauthenticated servers");
    }

    @Test
    void basicAuthIsSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/basicauth");
        final String expected =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString("user:secret".getBytes(StandardCharsets.UTF_8));
        assertEquals(
                expected,
                handler.lastHeaders.get("authorization"),
                "the opt-in sends credentials");
    }

    @Test
    void credentialCustomHeadersAreWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret", "X-Trace=public"));
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/customheaders");
        assertEquals(200, response.getStatusCode(), "the connection must succeed");
        assertNull(
                handler.lastHeaders.get("x-api-key"), "credential headers must be withheld");
        assertEquals("public", handler.lastHeaders.get("x-trace"), "other headers are sent");
    }

    @Test
    void credentialCustomHeadersAreSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.custom.headers", List.of("X-Api-Key=s3cret", "X-Trace=public"));
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/customheaders");
        assertEquals("s3cret", handler.lastHeaders.get("x-api-key"), "the opt-in sends credentials");
        assertEquals("public", handler.lastHeaders.get("x-trace"));
    }

    @Test
    void cookiesAreWithheldFromUnauthenticatedServers() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.use.cookies", true);
        startServer(LOCALHOST_KEYSTORE);
        final ProtocolResponse response = fetch(protocol(conf), "/cookies", metadata());
        assertEquals(200, response.getStatusCode(), "the connection must succeed");
        assertNull(
                handler.lastHeaders.get("cookie"),
                "cookies must not be sent to unauthenticated servers");
    }

    @Test
    void cookiesAreSentWhenExplicitlyAllowed() throws Exception {
        final Config conf = config();
        conf.put("http.trust.everything", true);
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.use.cookies", true);
        startServer(LOCALHOST_KEYSTORE);
        fetch(protocol(conf), "/cookies", metadata());
        assertEquals("sid=x", handler.lastHeaders.get("cookie"), "the opt-in sends cookies");
    }

    /** Metadata as an outlink would inherit it, with a cookie scoped to the server. */
    private Metadata metadata() {
        final Metadata md = new Metadata();
        md.setValue("protocol.set-cookie", "sid=x; Path=/");
        md.setValue(
                "protocol.set-cookie-origin",
                "https://localhost:" + server.getAddress().getPort() + "/");
        return md;
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
        return protocol.getProtocolOutput(
                "https://localhost:" + server.getAddress().getPort() + path, md);
    }

    /** Starts an HTTPS server on a random port presenting the certificate of the keystore. */
    private void startServer(String keystoreResource) throws Exception {
        final KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = OkHttpTrustEverythingTest.class.getResourceAsStream(keystoreResource)) {
            keyStore.load(in, KEYSTORE_PASSWORD);
        }
        final KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD);
        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);

        server = HttpsServer.create(new InetSocketAddress(java.net.InetAddress.getByName("127.0.0.1"), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        handler = new RecordingHandler();
        server.createContext("/", handler);
        server.start();
    }

    /** Records the request headers of the last request received. */
    static class RecordingHandler implements HttpHandler {

        final Map<String, String> lastHeaders = new ConcurrentHashMap<>();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            for (Map.Entry<String, List<String>> header :
                    exchange.getRequestHeaders().entrySet()) {
                lastHeaders.put(
                        header.getKey().toLowerCase(Locale.ROOT),
                        String.join(",", header.getValue()));
            }
            final byte[] body = "Success!".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }
}
