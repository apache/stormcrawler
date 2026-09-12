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

package org.apache.stormcrawler.protocol;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.okhttp.HttpProtocol;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * When http.allow.redirects is enabled, every hop of the redirect chain runs through the URL
 * filters, and the final URL is recorded in the response metadata.
 */
class OkHttpFollowRedirectsTest extends AbstractProtocolTest {

    /**
     * Redirects /start to /elsewhere (same origin), /cross to localhost (cross origin), and
     * /nested/start through /nested/hop with a relative Location; serves plain text otherwise.
     */
    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {
            new AbstractHandler() {
                @Override
                public void handle(
                        String target,
                        Request baseRequest,
                        jakarta.servlet.http.HttpServletRequest request,
                        HttpServletResponse response)
                        throws IOException {
                    baseRequest.setHandled(true);
                    if (target.equals("/start")) {
                        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                        response.setHeader(
                                "Location", "http://127.0.0.1:" + HTTP_PORT + "/elsewhere");
                        response.setContentLength(0);
                        response.getOutputStream().close();
                        return;
                    }
                    if (target.equals("/cross")) {
                        // same port but different host: another origin
                        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                        response.setHeader(
                                "Location", "http://localhost:" + HTTP_PORT + "/elsewhere");
                        response.setContentLength(0);
                        response.getOutputStream().close();
                        return;
                    }
                    if (target.equals("/nested/start")) {
                        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                        response.setHeader(
                                "Location", "http://127.0.0.1:" + HTTP_PORT + "/nested/hop");
                        response.setContentLength(0);
                        response.getOutputStream().close();
                        return;
                    }
                    if (target.equals("/nested/hop")) {
                        // relative Location: resolves against /nested/hop, not /nested/start
                        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
                        response.setHeader("Location", "next");
                        response.setContentLength(0);
                        response.getOutputStream().close();
                        return;
                    }
                    if (target.equals("/elsewhere")) {
                        if (request.getHeader("Authorization") != null) {
                            seenSecondRequestHadAuthorization.set(true);
                        }
                        if (request.getHeader("X-Api-Key") != null) {
                            seenSecondRequestHadApiKey.set(true);
                        }
                    }
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("text/plain");
                    final byte[] content = ("body of " + target).getBytes(StandardCharsets.UTF_8);
                    response.setContentLength(content.length);
                    try (OutputStream out = response.getOutputStream()) {
                        out.write(content);
                    }
                }
            }
        };
    }

    private static HttpProtocol protocol(Config conf) {
        HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    private static Config config() {
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        conf.put("http.allow.redirects", true);
        return conf;
    }

    @Test
    void redirectTargetIsFollowedAndRecorded() throws Exception {
        // no urlfilters.config.file: the chain is empty, every target passes
        HttpProtocol protocol = protocol(config());
        ProtocolResponse response =
                protocol.getProtocolOutput(
                        "http://127.0.0.1:" + HTTP_PORT + "/start", new Metadata());
        protocol.cleanup();
        Assertions.assertEquals(200, response.getStatusCode());
        Assertions.assertEquals(
                "body of /elsewhere",
                new String(response.getContent(), StandardCharsets.UTF_8),
                "the redirect is followed");
        Assertions.assertEquals(
                "http://127.0.0.1:" + HTTP_PORT + "/elsewhere",
                response.getMetadata().getFirstValue(ProtocolResponse.REDIRECTED_TO_KEY),
                "the final URL must be recorded in the response metadata");
    }

    @Test
    void rejectedRedirectTargetIsNotFetched() throws Exception {
        // a chain which rejects everything must stop the hop from being taken
        Config conf = config();
        conf.put("urlfilters.config.file", "urlfilters-reject-all.json");
        HttpProtocol protocol = protocol(conf);
        ProtocolResponse response =
                protocol.getProtocolOutput(
                        "http://127.0.0.1:" + HTTP_PORT + "/start", new Metadata());
        protocol.cleanup();
        Assertions.assertEquals(
                302, response.getStatusCode(), "the redirect response itself is returned");
        Assertions.assertEquals(
                "http://127.0.0.1:" + HTTP_PORT + "/elsewhere",
                response.getMetadata().getFirstValue("location"),
                "the Location header tells the caller where the chain stopped");
    }

    /** Stands in for an exclusion rule which rejects every target. */
    public static class RejectAllURLFilter extends org.apache.stormcrawler.filtering.URLFilter {
        @Override
        public String filter(
                java.net.URL sourceUrl,
                Metadata sourceMetadata,
                @org.jetbrains.annotations.NotNull String urlToFilter) {
            return null;
        }
    }

    @Test
    void redirectsAreNotFollowedWhenDisabled() throws Exception {
        Config conf = config();
        conf.put("http.allow.redirects", false);
        HttpProtocol protocol = protocol(conf);
        ProtocolResponse response =
                protocol.getProtocolOutput(
                        "http://127.0.0.1:" + HTTP_PORT + "/start", new Metadata());
        protocol.cleanup();
        Assertions.assertEquals(
                302,
                response.getStatusCode(),
                "the redirect response itself is returned, nothing is followed");
        Assertions.assertNull(
                response.getMetadata().getFirstValue(ProtocolResponse.REDIRECTED_TO_KEY),
                "no redirect was followed, so no final URL is recorded");
    }

    /** Same-origin hops keep every credential header, including custom ones. */
    @Test
    void credentialsSurviveASameOriginHop() throws Exception {
        seenSecondRequestHadAuthorization.set(false);
        seenSecondRequestHadApiKey.set(false);
        Config conf = config();
        // the test server is cleartext HTTP: opt in so the initial hop sends
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.custom.headers", java.util.List.of("X-Api-Key=s3cret"));
        HttpProtocol protocol = protocol(conf);
        protocol.getProtocolOutput("http://127.0.0.1:" + HTTP_PORT + "/start", new Metadata());
        protocol.cleanup();
        Assertions.assertTrue(
                seenSecondRequestHadAuthorization.get(),
                "credentials must survive a same-origin redirect hop");
        Assertions.assertTrue(
                seenSecondRequestHadApiKey.get(),
                "custom credential headers must survive a same-origin redirect hop");
    }

    /** A hop to another origin (different host) must not carry any credential header. */
    @Test
    void credentialsAreStrippedOnACrossOriginHop() throws Exception {
        seenSecondRequestHadAuthorization.set(false);
        seenSecondRequestHadApiKey.set(false);
        Config conf = config();
        // opt in so the initial hop sends: stripping then proves the origin
        // change removes them even where the transport policy would allow them
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "user");
        conf.put("http.basicauth.password", "secret");
        conf.put("http.custom.headers", java.util.List.of("X-Api-Key=s3cret"));
        HttpProtocol protocol = protocol(conf);
        ProtocolResponse response =
                protocol.getProtocolOutput(
                        "http://127.0.0.1:" + HTTP_PORT + "/cross", new Metadata());
        protocol.cleanup();
        Assertions.assertEquals(200, response.getStatusCode(), "the cross-origin hop is followed");
        Assertions.assertFalse(
                seenSecondRequestHadAuthorization.get(),
                "Authorization must not reach the second origin");
        Assertions.assertFalse(
                seenSecondRequestHadApiKey.get(), "X-Api-Key must not reach the second origin");
    }

    /**
     * When a chain stops after an intermediate hop, the returned Location is resolved against the
     * last request URL: /start -> /nested/hop -> next with max 1 must yield /nested/next, not
     * /next.
     */
    @Test
    void relativeLocationResolvesAgainstLastHop() throws Exception {
        Config conf = config();
        conf.put("http.allow.redirects.max", 1);
        HttpProtocol protocol = protocol(conf);
        ProtocolResponse response =
                protocol.getProtocolOutput(
                        "http://127.0.0.1:" + HTTP_PORT + "/nested/start", new Metadata());
        protocol.cleanup();
        Assertions.assertEquals(
                302, response.getStatusCode(), "the chain stops after the intermediate hop");
        Assertions.assertEquals(
                "http://127.0.0.1:" + HTTP_PORT + "/nested/next",
                response.getMetadata().getFirstValue("location"),
                "relative Location resolves against the last request URL");
    }

    /** Intermediate hops must not leave DNS timing entries behind. */
    @Test
    void dnsEntriesAreCleanedForEveryHop() throws Exception {
        HttpProtocol protocol = protocol(config());
        protocol.getProtocolOutput("http://127.0.0.1:" + HTTP_PORT + "/start", new Metadata());
        java.lang.reflect.Field field = HttpProtocol.class.getDeclaredField("DNStimes");
        field.setAccessible(true);
        java.util.Map<?, ?> dnsTimes = (java.util.Map<?, ?>) field.get(protocol);
        protocol.cleanup();
        Assertions.assertTrue(
                dnsTimes.isEmpty(), "every hop's DNS entry is cleaned up, not just the final one");
    }

    static final java.util.concurrent.atomic.AtomicBoolean seenSecondRequestHadAuthorization =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    static final java.util.concurrent.atomic.AtomicBoolean seenSecondRequestHadApiKey =
            new java.util.concurrent.atomic.AtomicBoolean(false);
}
