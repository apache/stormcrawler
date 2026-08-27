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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks that the url whose response set the cookies is recorded in the metadata and used to send
 * cookies without a domain attribute back to the host which set them.
 */
class HttpProtocolCookieTest extends AbstractProtocolTest {

    private static final String ORIGIN_KEY = "set-cookie-origin";

    private static final CookieHandler handler = new CookieHandler();

    @BeforeEach
    void resetHandler() {
        handler.lastCookieHeader = null;
    }

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {handler};
    }

    @Test
    void responseCarryingSetCookieRecordsTheOrigin() throws Exception {
        final String url = url("/setcookie");
        final Metadata md = fetch(protocol(false), url, new Metadata());
        assertEquals(url, md.getFirstValue(ORIGIN_KEY), "the origin is the url which was fetched");
    }

    @Test
    void responseWithoutSetCookieRecordsNoOrigin() throws Exception {
        final Metadata md = fetch(protocol(false), url("/nocookie"), new Metadata());
        assertNull(md.getFirstValue(ORIGIN_KEY), "no cookies, no origin");
    }

    @Test
    void serverSuppliedOriginHeaderIsDiscarded() throws Exception {
        final Metadata md = fetch(protocol(false), url("/forgedorigin"), new Metadata());
        assertNull(md.getFirstValue(ORIGIN_KEY), "the origin can not be set by the server");
    }

    @Test
    void serverSuppliedOriginHeaderIsOverwritten() throws Exception {
        final String url = url("/setcookieandforgedorigin");
        final Metadata md = fetch(protocol(false), url, new Metadata());
        assertEquals(url, md.getFirstValue(ORIGIN_KEY), "the origin can not be set by the server");
    }

    @Test
    void redirectResponseRecordsTheOrigin() throws Exception {
        final String url = url("/loginredirect");
        final Metadata md = fetch(protocol(false), url, new Metadata());
        assertEquals(url, md.getFirstValue(ORIGIN_KEY), "a 302 can set cookies too");
    }

    @Test
    void hostOnlyCookieIsSentBackToTheSameHost() throws Exception {
        final HttpProtocol protocol = protocol(true);
        final Metadata md = outlinkMetadata(protocol, url("/setcookie"));
        fetch(protocol, url("/echo"), md);
        assertEquals("sid=x", handler.lastCookieHeader, "the cookie is sent back to the same host");
    }

    @Test
    void hostOnlyCookieIsNotSentToAnotherHost() throws Exception {
        final HttpProtocol protocol = protocol(true);
        final Metadata md = outlinkMetadata(protocol, url("/setcookie"));
        fetch(protocol, "http://127.0.0.1:" + HTTP_PORT + "/echo", md);
        assertNull(handler.lastCookieHeader, "the cookie is bound to the host which set it");
    }

    @Test
    void cookieWithoutRecordedOriginIsNotSent() throws Exception {
        final HttpProtocol protocol = protocol(true);
        final Metadata md = outlinkMetadata(protocol, url("/setcookie"));
        md.remove("protocol." + ORIGIN_KEY);
        fetch(protocol, url("/echo"), md);
        assertNull(handler.lastCookieHeader, "without an origin the cookie can not be scoped");
    }

    @Test
    void unparseableOriginIsIgnored() throws Exception {
        final HttpProtocol protocol = protocol(true);
        final Metadata md = outlinkMetadata(protocol, url("/setcookieforlocalhost"));
        md.setValue("protocol." + ORIGIN_KEY, "not a url");
        fetch(protocol, url("/echo"), md);
        assertEquals(
                "domainsid=y",
                handler.lastCookieHeader,
                "only the cookie without a domain attribute needs the origin");
    }

    /** Fetches a url and returns the metadata of the response. */
    private Metadata fetch(HttpProtocol protocol, String url, Metadata md) throws Exception {
        return protocol.getProtocolOutput(url, md).getMetadata();
    }

    /** Fetches a url and returns its response metadata as an outlink would inherit it. */
    private Metadata outlinkMetadata(HttpProtocol protocol, String url) throws Exception {
        final Metadata md = new Metadata();
        md.putAll(fetch(protocol, url, new Metadata()), "protocol.");
        return md;
    }

    private String url(String path) {
        return "http://localhost:" + HTTP_PORT + path;
    }

    private HttpProtocol protocol(boolean useCookies) {
        final Config conf = new Config();
        conf.put("http.agent.name", "test");
        conf.put("http.agent.version", "1.0");
        conf.put("http.agent.description", "test");
        conf.put("http.agent.url", "http://test.example.com");
        conf.put("http.agent.email", "test@example.com");
        conf.put("http.use.cookies", useCookies);
        conf.put("protocol.md.prefix", "protocol.");
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    static class CookieHandler extends AbstractHandler {

        private volatile String lastCookieHeader;

        @Override
        public void handle(
                String target,
                Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {
            baseRequest.setHandled(true);
            switch (target) {
                case "/setcookie":
                    response.addHeader("Set-Cookie", "sid=x; Path=/");
                    break;
                case "/setcookieforlocalhost":
                    response.addHeader("Set-Cookie", "sid=x; Path=/");
                    response.addHeader("Set-Cookie", "domainsid=y; Domain=localhost; Path=/");
                    break;
                case "/forgedorigin":
                    response.addHeader("Set-Cookie-Origin", "http://evil.example.com/");
                    break;
                case "/setcookieandforgedorigin":
                    response.addHeader("Set-Cookie", "sid=x; Path=/");
                    response.addHeader("Set-Cookie-Origin", "http://evil.example.com/");
                    break;
                case "/loginredirect":
                    response.addHeader("Set-Cookie", "sid=x; Path=/");
                    response.setHeader("Location", "/home");
                    response.setStatus(HttpServletResponse.SC_FOUND);
                    return;
                case "/echo":
                    lastCookieHeader = request.getHeader("Cookie");
                    break;
                default:
                    break;
            }
            final byte[] content = "Success!".getBytes(StandardCharsets.UTF_8);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html");
            response.setContentLength(content.length);
            try (OutputStream out = response.getOutputStream()) {
                out.write(content);
            }
        }
    }
}
