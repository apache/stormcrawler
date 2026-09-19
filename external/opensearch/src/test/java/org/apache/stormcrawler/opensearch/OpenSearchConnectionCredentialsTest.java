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
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.junit.jupiter.api.Test;

class OpenSearchConnectionCredentialsTest {

    private static BasicCredentialsProvider providerFor(List<HttpHost> hosts) {
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        final UsernamePasswordCredentials credentials =
                new UsernamePasswordCredentials("crawler", "s3cret");
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
                                new HttpHost("opensearch1.example.org", 9200, "https"),
                                new HttpHost("opensearch2.example.org", 9201, "https")));
        assertEquals(
                List.of(
                        new AuthScope("opensearch1.example.org", 9200),
                        new AuthScope("opensearch2.example.org", 9201)),
                scopes);
    }

    @Test
    void duplicateAddressesGiveOneScope() {
        final List<AuthScope> scopes =
                OpenSearchConnection.credentialScopes(
                        List.of(
                                new HttpHost("opensearch1.example.org", 9200, "https"),
                                new HttpHost("OpenSearch1.example.org", 9200, "https")));
        assertEquals(1, scopes.size());
    }

    @Test
    void noAddressGivesNoScope() {
        assertTrue(OpenSearchConnection.credentialScopes(List.of()).isEmpty());
    }

    @Test
    void credentialsAreOnlyGivenToTheConfiguredHosts() {
        final BasicCredentialsProvider provider =
                providerFor(List.of(new HttpHost("opensearch1.example.org", 9200, "https")));

        assertNotNull(provider.getCredentials(new AuthScope("opensearch1.example.org", 9200)));
        // a node found by the sniffer under the address it publishes
        assertNull(provider.getCredentials(new AuthScope("10.0.0.12", 9200)));
        // same host, another port
        assertNull(provider.getCredentials(new AuthScope("opensearch1.example.org", 9300)));
        assertNull(provider.getCredentials(new AuthScope("other.example.org", 9200)));
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
