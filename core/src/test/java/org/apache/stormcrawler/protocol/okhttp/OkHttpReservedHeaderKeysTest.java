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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.Test;

/**
 * A response header name is not restricted to anything, so a server can return one named like the
 * metadata the crawler writes about its own fetch. Those values must never be stored, see issue
 * #2091 and the metrics part of #2099.
 */
class OkHttpReservedHeaderKeysTest extends AbstractProtocolTest {

    private static final String FORGED_IP = "6.6.6.6";

    private static final String FORGED_CIPHER = "TLS_FORGED_BY_THE_SERVER";

    private static final String FORGED_PROTOCOLS = "h2,tls/1.3";

    private static final String FORGED_DNS_TIME = "999999";

    /** Not valid base64: the decode used to throw out of getProtocolOutput and fail the fetch. */
    private static final String FORGED_HEADER_BLOCK = "!!!not base64!!!";

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {new ForgedHeaderHandler()};
    }

    @Test
    void reservedKeysFromTheWireAreNotStored() throws Exception {
        Metadata metadata = fetch(false);

        assertNull(metadata.getFirstValue(ProtocolResponse.REQUEST_HEADERS_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.RESPONSE_HEADERS_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.RESPONSE_IP_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.REQUEST_TIME_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.PROTOCOL_VERSIONS_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.CIPHER_SUITE_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.REDIRECTED_TO_KEY));
        assertNull(metadata.getFirstValue("metrics.injected"));
        // the protocol writes its own DNS timing under this key after copying the headers
        assertNotEquals(FORGED_DNS_TIME, metadata.getFirstValue("metrics.dns.resolution.msec"));

        // an ordinary header is still copied
        assertEquals("text/html", metadata.getFirstValue("content-type"));
    }

    /**
     * With http.store.headers the interceptor writes these keys itself, so its values are kept and
     * the forged ones are gone. The cipher suite is the case the interceptor does not write: there
     * is no handshake over http://, so it has to drop the header rather than leave it.
     */
    @Test
    void storedHeadersComeFromTheInterceptorNotTheWire() throws Exception {
        Metadata metadata = fetch(true);

        assertTrue(
                metadata.getFirstValue(ProtocolResponse.RESPONSE_HEADERS_KEY)
                        .startsWith("HTTP/1.1 200"),
                "the verbatim response block is expected to be the one the interceptor recorded");
        assertNotEquals(FORGED_IP, metadata.getFirstValue(ProtocolResponse.RESPONSE_IP_KEY));
        assertEquals("http/1.1", metadata.getFirstValue(ProtocolResponse.PROTOCOL_VERSIONS_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.CIPHER_SUITE_KEY));
        assertNull(metadata.getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
        assertNull(metadata.getFirstValue("metrics.injected"));
        assertNotEquals(FORGED_DNS_TIME, metadata.getFirstValue("metrics.dns.resolution.msec"));
    }

    private Metadata fetch(boolean storeHeaders) throws Exception {
        HttpProtocol protocol = new HttpProtocol();
        Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.store.headers", storeHeaders);
        protocol.configure(conf);

        ProtocolResponse response =
                protocol.getProtocolOutput("http://localhost:" + HTTP_PORT, Metadata.empty);
        assertEquals(
                200,
                response.getStatusCode(),
                "a response carrying these headers is expected to be fetched normally");
        return response.getMetadata();
    }

    /** Returns a response whose headers name the metadata the crawler writes about a fetch. */
    static class ForgedHeaderHandler extends AbstractHandler {

        @Override
        public void handle(
                String target,
                Request baseRequest,
                jakarta.servlet.http.HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {
            baseRequest.setHandled(true);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html");
            response.setHeader(ProtocolResponse.REQUEST_HEADERS_KEY, FORGED_HEADER_BLOCK);
            response.setHeader(ProtocolResponse.RESPONSE_HEADERS_KEY, FORGED_HEADER_BLOCK);
            response.setHeader(ProtocolResponse.RESPONSE_IP_KEY, FORGED_IP);
            response.setHeader(ProtocolResponse.REQUEST_TIME_KEY, "1");
            response.setHeader(ProtocolResponse.PROTOCOL_VERSIONS_KEY, FORGED_PROTOCOLS);
            response.setHeader(ProtocolResponse.CIPHER_SUITE_KEY, FORGED_CIPHER);
            response.setHeader(ProtocolResponse.TRIMMED_RESPONSE_KEY, "true");
            response.setHeader(ProtocolResponse.REDIRECTED_TO_KEY, "http://example.com/");
            response.setHeader("metrics.dns.resolution.msec", FORGED_DNS_TIME);
            response.setHeader("metrics.injected", FORGED_DNS_TIME);
            final String content = "Success!";
            response.setContentLength(content.length());
            try (OutputStream out = response.getOutputStream()) {
                out.write(content.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
