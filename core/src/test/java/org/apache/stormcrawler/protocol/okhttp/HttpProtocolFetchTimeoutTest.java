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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.InterruptedIOException;
import org.apache.storm.Config;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.FetchTimeoutException;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@WireMockTest
class HttpProtocolFetchTimeoutTest {

    private static HttpProtocol protocol(long fetchTimeoutSecs) {
        return protocol(fetchTimeoutSecs, new Config());
    }

    private static HttpProtocol protocol(long fetchTimeoutSecs, Config conf) {
        conf.put("http.agent.name", "this_is_only_a_test");
        // socket timeouts far above the fetch timeout so that only the latter can fire
        conf.put("http.timeout", 30_000);
        if (fetchTimeoutSecs > 0) {
            conf.put(Constants.FETCH_TIMEOUT_PARAM_KEY, fetchTimeoutSecs);
        }
        HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    @Test
    void supportsFetchTimeoutOnlyWhenConfigured() {
        Assertions.assertTrue(protocol(1).supportsFetchTimeout("http://a.net/", null));
        Assertions.assertFalse(protocol(-1).supportsFetchTimeout("http://a.net/", null));
    }

    @Test
    void slowResponseIsCancelledAtTheFetchTimeout(WireMockRuntimeInfo wm) {
        stubFor(
                get(urlEqualTo("/slow"))
                        .willReturn(aResponse().withStatus(200).withFixedDelay(5_000)));
        HttpProtocol protocol = protocol(1);
        long start = System.currentTimeMillis();
        Exception thrown =
                Assertions.assertThrows(
                        Exception.class,
                        () ->
                                protocol.getProtocolOutput(
                                        wm.getHttpBaseUrl() + "/slow", new Metadata()));
        long elapsed = System.currentTimeMillis() - start;
        Assertions.assertTrue(
                elapsed < 3_000, "fetch was not cancelled at the timeout, took " + elapsed + " ms");
        // the deadline, as opposed to a socket timeout
        Assertions.assertInstanceOf(FetchTimeoutException.class, thrown);
        Assertions.assertEquals(1, ((FetchTimeoutException) thrown).getTimeoutSecs());
    }

    /** A plain socket timeout (http.timeout) is not reported as the deadline. */
    @Test
    void socketTimeoutIsNotTheDeadline(WireMockRuntimeInfo wm) {
        stubFor(
                get(urlEqualTo("/slow"))
                        .willReturn(aResponse().withStatus(200).withFixedDelay(5_000)));
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        // socket timeout below the deadline: it fires first
        conf.put("http.timeout", 500);
        conf.put(Constants.FETCH_TIMEOUT_PARAM_KEY, 10L);
        HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        Exception thrown =
                Assertions.assertThrows(
                        Exception.class,
                        () ->
                                protocol.getProtocolOutput(
                                        wm.getHttpBaseUrl() + "/slow", new Metadata()));
        Assertions.assertInstanceOf(InterruptedIOException.class, thrown);
        Assertions.assertFalse(thrown instanceof FetchTimeoutException, thrown.toString());
    }

    /** The hops of a redirect chain share one deadline: it is per fetch, not per hop. */
    @Test
    void redirectChainSharesTheDeadline(WireMockRuntimeInfo wm) {
        // each hop takes 700 ms: with a 1 s deadline the chain can not complete, and a per-hop
        // deadline would have let it run for over 2 s
        stubFor(
                get(urlEqualTo("/a"))
                        .willReturn(
                                aResponse()
                                        .withStatus(302)
                                        .withHeader("Location", "/b")
                                        .withFixedDelay(700)));
        stubFor(
                get(urlEqualTo("/b"))
                        .willReturn(
                                aResponse()
                                        .withStatus(302)
                                        .withHeader("Location", "/c")
                                        .withFixedDelay(700)));
        stubFor(
                get(urlEqualTo("/c"))
                        .willReturn(
                                aResponse().withStatus(200).withBody("ok").withFixedDelay(700)));
        Config conf = new Config();
        conf.put("http.allow.redirects", true);
        HttpProtocol protocol = protocol(1, conf);
        long start = System.currentTimeMillis();
        Exception thrown =
                Assertions.assertThrows(
                        Exception.class,
                        () ->
                                protocol.getProtocolOutput(
                                        wm.getHttpBaseUrl() + "/a", new Metadata()));
        long elapsed = System.currentTimeMillis() - start;
        Assertions.assertInstanceOf(FetchTimeoutException.class, thrown);
        Assertions.assertTrue(elapsed < 1_800, "deadline was not shared by the hops: " + elapsed);
    }

    /** The fetch timeout must not loosen the deadline derived from the message timeout. */
    @Test
    void fetchTimeoutIsClampedToTheMessageTimeout(WireMockRuntimeInfo wm) {
        stubFor(
                get(urlEqualTo("/slow"))
                        .willReturn(aResponse().withStatus(200).withFixedDelay(5_000)));
        Config conf = new Config();
        conf.put("topology.message.timeout.secs", 1);
        HttpProtocol protocol = protocol(60, conf);
        long start = System.currentTimeMillis();
        Assertions.assertThrows(
                Exception.class,
                () -> protocol.getProtocolOutput(wm.getHttpBaseUrl() + "/slow", new Metadata()));
        long elapsed = System.currentTimeMillis() - start;
        Assertions.assertTrue(elapsed < 3_000, "message timeout was loosened, took " + elapsed);
    }

    /**
     * With http.content.partial.as.trimmed the content received before the deadline is kept and
     * flagged as trimmed for "time"; without it the deadline is a plain failure.
     */
    @Test
    void deadlineDuringBodyHonoursPartialContentAsTrimmed(WireMockRuntimeInfo wm) throws Exception {
        byte[] body = new byte[20_000];
        stubFor(
                get(urlEqualTo("/dribble"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        // uncompressed, so that bytes reach the buffer as they
                                        // arrive
                                        .withHeader("Content-Encoding", "identity")
                                        .withBody(body)
                                        .withChunkedDribbleDelay(40, 8_000)));
        Config keep = new Config();
        keep.put("http.content.partial.as.trimmed", true);
        ProtocolResponse response =
                protocol(1, keep)
                        .getProtocolOutput(wm.getHttpBaseUrl() + "/dribble", new Metadata());
        Assertions.assertEquals(200, response.getStatusCode());
        Assertions.assertTrue(response.getContent().length < body.length, "content was cut");
        Assertions.assertEquals(
                "true",
                response.getMetadata().getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_KEY));
        Assertions.assertEquals(
                "time",
                response.getMetadata().getFirstValue(ProtocolResponse.TRIMMED_RESPONSE_REASON_KEY));

        Assertions.assertThrows(
                Exception.class,
                () ->
                        protocol(1)
                                .getProtocolOutput(
                                        wm.getHttpBaseUrl() + "/dribble", new Metadata()));
    }
}
