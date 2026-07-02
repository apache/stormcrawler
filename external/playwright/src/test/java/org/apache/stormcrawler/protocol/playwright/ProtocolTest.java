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

package org.apache.stormcrawler.protocol.playwright;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.storm.Config;
import org.apache.storm.utils.MutableObject;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.eclipse.jetty.server.Handler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for Playwright protocol implementation. Chrome should be running on localhost, whether is
 * has been launched manually or by Playwright.
 */
class ProtocolTest extends AbstractProtocolTest {

    private static final String USER_AGENT = "StormCrawlerTest";

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {new LocalResourceHandler(), new WildcardResourceHandler()};
    }

    public HttpProtocol getProtocol() {
        Config conf = new Config();
        conf.put("http.agent.name", USER_AGENT);
        // "http://localhost:9222"
        String cdpurl = System.getProperty("playwright.cdp.url");
        if (cdpurl != null) {
            conf.put("playwright.cdp.url", cdpurl);
        }
        HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    @BeforeEach
    void setup() {
        assumeTrue("false".equals(System.getProperty("CI_ENV", "false")));
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testNotFound() throws Exception {
        HttpProtocol protocol = getProtocol();
        String url = "http://localhost:" + HTTP_PORT + "/doesNotExist";
        ProtocolResponse response = protocol.getProtocolOutput(url, new Metadata());
        Assertions.assertEquals(404, response.getStatusCode());
        Assertions.assertEquals(0, response.getContent().length);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testHTML() throws Exception {
        HttpProtocol protocol = getProtocol();
        String url = "http://localhost:" + HTTP_PORT + "/dynamic-scraping.html";
        ProtocolResponse response = protocol.getProtocolOutput(url, new Metadata());
        // check that we have the metadata we expect
        Assertions.assertNotNull(response.getMetadata().getFirstValue("key"));
        // the correct code
        Assertions.assertEquals(200, response.getStatusCode());
        final String content = new String(response.getContent(), StandardCharsets.UTF_8);
        Assertions.assertNotNull(content);
        // we expect that the given JS was executed, so the content should contain this HTML snippet
        Assertions.assertTrue(
                content.contains(
                        "<p><a href=\"https://stormcrawler.apache.org/\">StormCrawler Rocks!</a></p>"));
    }

    /** Calls 2 URLs on the same protocol instance - should block * */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testBlocking() {
        HttpProtocol p = getProtocol();
        run(p, p, true);
    }

    /** Calls 2 URLs on 2 different instances - should run in parallel * */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testParallel() {
        run(getProtocol(), getProtocol(), false);
    }

    private void run(HttpProtocol protocol, HttpProtocol protocol2, boolean expected) {
        MutableBoolean noException = new MutableBoolean(true);
        MutableObject endTimeFirst = new MutableObject();
        MutableObject startTimeSecond = new MutableObject();
        String url = "http://localhost:" + HTTP_PORT + "/";
        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol.getProtocolOutput(url, new Metadata());
                                endTimeFirst.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(HttpProtocol.MD_KEY_END)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();
        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol2.getProtocolOutput(url, new Metadata());
                                startTimeSecond.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(HttpProtocol.MD_KEY_START)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();
        await().atMost(2, TimeUnit.MINUTES)
                .until(
                        () ->
                                !noException.booleanValue()
                                        || (endTimeFirst.getObject() != null
                                                && startTimeSecond.getObject() != null));
        Assertions.assertEquals(true, noException.booleanValue());
        Instant etf = (Instant) endTimeFirst.getObject();
        Instant sts = (Instant) startTimeSecond.getObject();
        // check that the second call started while the first one is being processed
        // normal - dealing with different instances
        Assertions.assertEquals(expected, etf.isBefore(sts));
        protocol.cleanup();
    }
}
