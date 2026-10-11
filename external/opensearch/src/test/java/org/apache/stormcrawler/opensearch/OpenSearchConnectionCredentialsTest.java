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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.client.sniff.OpenSearchNodesSniffer;

class OpenSearchConnectionCredentialsTest {

    private static final Credentials CREDENTIALS =
            new UsernamePasswordCredentials("crawler", "s3cret");

    private static CredentialsProvider providerFor(HttpHost... hosts) {
        return new OpenSearchConnection.OriginCredentialsProvider(List.of(hosts), CREDENTIALS);
    }

    /** The scope HttpClient looks the credentials up with for a request to the given node. */
    private static AuthScope request(String scheme, String host, int port) {
        return new AuthScope(new HttpHost(host, port, scheme), AuthScope.ANY_REALM, "Basic");
    }

    @Test
    void credentialsAreGivenToTheConfiguredAddresses() {
        final CredentialsProvider provider =
                providerFor(
                        new HttpHost("opensearch1.example.org", 9200, "https"),
                        new HttpHost("OpenSearch2.example.org", 9201, "https"));
        assertEquals(
                CREDENTIALS,
                provider.getCredentials(request("https", "opensearch1.example.org", 9200)));
        assertEquals(
                CREDENTIALS,
                provider.getCredentials(request("HTTPS", "opensearch2.example.org", 9201)));
    }

    @Test
    void credentialsAreOnlyGivenToTheConfiguredHosts() {
        final CredentialsProvider provider =
                providerFor(new HttpHost("opensearch1.example.org", 9200, "https"));

        // a node found by the sniffer under the address it publishes
        assertNull(provider.getCredentials(request("https", "10.0.0.12", 9200)));
        // same host, another port
        assertNull(provider.getCredentials(request("https", "opensearch1.example.org", 9300)));
        assertNull(provider.getCredentials(request("https", "other.example.org", 9200)));
    }

    @Test
    void credentialsAreMatchedOnTheScheme() {
        final CredentialsProvider provider =
                providerFor(new HttpHost("opensearch1.example.org", 9200, "https"));
        assertNull(provider.getCredentials(request("http", "opensearch1.example.org", 9200)));
    }

    @Test
    void credentialsNeedAnOrigin() {
        final CredentialsProvider provider =
                providerFor(new HttpHost("opensearch1.example.org", 9200, "https"));
        assertNull(provider.getCredentials(new AuthScope("opensearch1.example.org", 9200)));
        assertNull(provider.getCredentials(AuthScope.ANY));
    }

    @Test
    void noAddressGivesTheCredentialsToNoOne() {
        assertNull(providerFor().getCredentials(request("https", "opensearch1.example.org", 9200)));
    }

    /**
     * Sends a request with the client to a local server and returns the Authorization header it
     * received, if any.
     */
    private static String authorizationSentTo(HttpHost configured) throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            final int port = server.getLocalPort();
            final CompletableFuture<String> authorization =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try (Socket socket = server.accept()) {
                                    final BufferedReader in =
                                            new BufferedReader(
                                                    new InputStreamReader(
                                                            socket.getInputStream(),
                                                            StandardCharsets.US_ASCII));
                                    String header = null;
                                    for (String line = in.readLine();
                                            line != null && !line.isEmpty();
                                            line = in.readLine()) {
                                        if (line.regionMatches(true, 0, "Authorization:", 0, 14)) {
                                            header = line.substring(14).trim();
                                        }
                                    }
                                    socket.getOutputStream()
                                            .write(
                                                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                                            .getBytes(StandardCharsets.US_ASCII));
                                    return header;
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
            final HttpHost configuredHere =
                    new HttpHost(configured.getHostName(), port, configured.getSchemeName());
            try (RestClient client =
                    RestClient.builder(new HttpHost("127.0.0.1", port, "http"))
                            .setHttpClientConfigCallback(
                                    b ->
                                            b.setDefaultCredentialsProvider(
                                                    providerFor(configuredHere)))
                            .build()) {
                client.performRequest(new Request("GET", "/"));
            }
            return authorization.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void clientSendsTheCredentialsToAConfiguredAddress() throws Exception {
        assertNotNull(authorizationSentTo(new HttpHost("127.0.0.1", 0, "http")));
    }

    @Test
    void clientSendsNoCredentialsToAnotherAddress() throws Exception {
        assertNull(authorizationSentTo(new HttpHost("127.0.0.1", 0, "https")));
    }

    @Test
    void plainHttpToARemoteHostIsReported() {
        final HttpHost remote = new HttpHost("opensearch1.example.org", 9200, "http");
        final List<HttpHost> plain =
                OpenSearchConnection.plainHttpHosts(
                        List.of(
                                remote,
                                new HttpHost("opensearch2.example.org", 9200, "https"),
                                new HttpHost("localhost", 9200, "http"),
                                new HttpHost("127.0.0.1", 9200, "http"),
                                new HttpHost("[::1]", 9200, "http")));
        assertEquals(List.of(remote), plain);
    }

    @Test
    void sniffedNodesKeepTheSchemeOfTheAddresses() {
        assertEquals(
                OpenSearchNodesSniffer.Scheme.HTTPS,
                OpenSearchConnection.sniffScheme(
                        List.of(new HttpHost("opensearch1.example.org", 9200, "https"))));
        assertEquals(
                OpenSearchNodesSniffer.Scheme.HTTPS,
                OpenSearchConnection.sniffScheme(
                        List.of(
                                new HttpHost("opensearch1.example.org", 9200, "http"),
                                new HttpHost("opensearch2.example.org", 9200, "https"))));
        assertEquals(
                OpenSearchNodesSniffer.Scheme.HTTP,
                OpenSearchConnection.sniffScheme(
                        List.of(new HttpHost("opensearch1.example.org", 9200, "http"))));
    }

    @Test
    void credentialsNeedUserAndPassword() {
        assertTrue(OpenSearchConnection.hasCredentials("crawler", "s3cret"));
        assertFalse(OpenSearchConnection.hasCredentials("crawler", null));
        assertFalse(OpenSearchConnection.hasCredentials("crawler", " "));
        assertFalse(OpenSearchConnection.hasCredentials(null, "s3cret"));
        assertFalse(OpenSearchConnection.hasCredentials("", ""));
    }

    @Test
    void loopbackAddresses() {
        assertTrue(OpenSearchConnection.isLoopback("localhost"));
        assertTrue(OpenSearchConnection.isLoopback("LOCALHOST"));
        assertTrue(OpenSearchConnection.isLoopback("127.0.0.1"));
        assertTrue(OpenSearchConnection.isLoopback("[::1]"));
        assertTrue(OpenSearchConnection.isLoopback("0:0:0:0:0:0:0:1"));
        assertFalse(OpenSearchConnection.isLoopback("opensearch1.example.org"));
        assertFalse(OpenSearchConnection.isLoopback("10.0.0.12"));
        assertFalse(OpenSearchConnection.isLoopback("localhost.example.org"));
        assertFalse(OpenSearchConnection.isLoopback("127.example.org"));
        assertTrue(OpenSearchConnection.isLoopback("127.1.2.3"));
    }
}
