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

import java.util.List;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Test;

class OpenSearchConnectionCredentialsTest {

    private static BasicCredentialsProvider providerFor(List<HttpHost> hosts) {
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        final UsernamePasswordCredentials credentials =
                new UsernamePasswordCredentials("crawler", "s3cret".toCharArray());
        for (AuthScope scope : OpenSearchConnection.credentialScopes(hosts)) {
            provider.setCredentials(scope, credentials);
        }
        return provider;
    }

    @Test
    void scopesCoverTheConfiguredHostsAndPorts() {
        final List<AuthScope> scopes =
                OpenSearchConnection.credentialScopes(
                        List.of(
                                new HttpHost("https", "opensearch1.example.org", 9200),
                                new HttpHost("https", "opensearch2.example.org", 9201)));
        assertEquals(
                List.of(
                        new AuthScope("https", "opensearch1.example.org", 9200, null, null),
                        new AuthScope("https", "opensearch2.example.org", 9201, null, null)),
                scopes);
    }

    @Test
    void duplicateAddressesGiveOneScope() {
        final List<AuthScope> scopes =
                OpenSearchConnection.credentialScopes(
                        List.of(
                                new HttpHost("https", "opensearch1.example.org", 9200),
                                new HttpHost("https", "OpenSearch1.example.org", 9200)));
        assertEquals(1, scopes.size());
    }

    @Test
    void noAddressGivesNoScope() {
        assertTrue(OpenSearchConnection.credentialScopes(List.of()).isEmpty());
    }

    @Test
    void credentialsAreOnlyGivenToTheConfiguredHosts() {
        final BasicCredentialsProvider provider =
                providerFor(List.of(new HttpHost("https", "opensearch1.example.org", 9200)));

        assertNotNull(
                provider.getCredentials(request("https", "opensearch1.example.org", 9200), null));
        // another node of the cluster which is not listed in the addresses
        assertNull(provider.getCredentials(request("https", "10.0.0.12", 9200), null));
        // same host, another port
        assertNull(
                provider.getCredentials(request("https", "opensearch1.example.org", 9300), null));
        assertNull(provider.getCredentials(request("https", "other.example.org", 9200), null));
    }

    @Test
    void credentialsAreMatchedOnTheScheme() {
        final BasicCredentialsProvider provider =
                providerFor(List.of(new HttpHost("https", "opensearch1.example.org", 9200)));
        assertNull(provider.getCredentials(request("http", "opensearch1.example.org", 9200), null));
    }

    /** The scope HttpClient looks the credentials up with for a request to the given node. */
    private static AuthScope request(String scheme, String host, int port) {
        return new AuthScope(new HttpHost(scheme, host, port), null, "Basic");
    }

    @Test
    void plainHttpToARemoteHostIsReported() {
        final HttpHost remote = new HttpHost("http", "opensearch1.example.org", 9200);
        final List<HttpHost> plain =
                OpenSearchConnection.plainHttpHosts(
                        List.of(
                                remote,
                                new HttpHost("https", "opensearch2.example.org", 9200),
                                new HttpHost("http", "localhost", 9200),
                                new HttpHost("http", "127.0.0.1", 9200),
                                new HttpHost("http", "[::1]", 9200)));
        assertEquals(List.of(remote), plain);
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
