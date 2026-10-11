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

import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Test;

/** Tests the protocol metadata collected by the response interceptor. */
class HttpProtocolHeadersTest extends AbstractProtocolTest {

    /**
     * Over an unencrypted connection there is no handshake, hence no TLS version and no cipher
     * suite. The cipher suite header must be skipped entirely: OkHttp's {@code
     * Response.Builder.header(...)} does not accept a null value.
     */
    @Test
    void plainHttpRequestStoresProtocolVersionButNoCipherSuite() throws Exception {
        HttpProtocol protocol = new HttpProtocol();
        Config conf = protocolConfig();
        conf.put("http.store.headers", true);
        protocol.configure(conf);

        ProtocolResponse response =
                protocol.getProtocolOutput("http://localhost:" + HTTP_PORT, Metadata.empty);

        assertEquals(200, response.getStatusCode());
        Metadata metadata = response.getMetadata();
        assertEquals(
                "http/1.1",
                metadata.getFirstValue(ProtocolResponse.PROTOCOL_VERSIONS_KEY),
                "The protocol version is expected to be stored for plain HTTP requests");
        assertNull(
                metadata.getFirstValue(ProtocolResponse.CIPHER_SUITE_KEY),
                "No cipher suite is expected without a TLS handshake");
    }

    private Config protocolConfig() {
        Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "test");
        conf.put("http.agent.url", "http://test.example.com");
        conf.put("http.agent.email", "test@example.com");
        return conf;
    }
}
