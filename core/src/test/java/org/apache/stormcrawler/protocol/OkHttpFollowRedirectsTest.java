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

    /** Redirects /start to /elsewhere, serves plain text for anything else. */
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
                response.getMetadata().getFirstValue("_redirTo"),
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
                response.getMetadata().getFirstValue("_redirTo"),
                "no redirect was followed, so no final URL is recorded");
    }
}
