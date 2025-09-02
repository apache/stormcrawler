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
package org.apache.stormcrawler.protocol.selenium;

import static org.awaitility.Awaitility.await;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.storm.Config;
import org.apache.storm.utils.MutableObject;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.AbstractProtocolTest;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.openqa.selenium.chrome.ChromeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BrowserWebDriverContainer;
import org.testcontainers.containers.BrowserWebDriverContainer.VncRecordingMode;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests the Selenium protocol implementation on a standalone Chrome instance and not through
 * Selenium Grid. https://java.testcontainers.org/modules/webdriver_containers/#example
 */
@Testcontainers(disabledWithoutDocker = true)
class ProtocolTest extends AbstractProtocolTest {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolTest.class);

    private static final DockerImageName SELENIUM_IMAGE =
            DockerImageName.parse("seleniarm/standalone-chromium:124.0")
                    .asCompatibleSubstituteFor("selenium/standalone-chrome");

    private BrowserWebDriverContainer<?> chrome =
            new BrowserWebDriverContainer<>(SELENIUM_IMAGE)
                    .withCapabilities(new ChromeOptions())
                    .withRecordingMode(VncRecordingMode.SKIP, null)
                    .withAccessToHost(true)
                    .withExtraHost("website.test", "host-gateway");

    public RemoteDriverProtocol getProtocol() {
        LOG.info(
                "Configuring protocol instance to connect to {}",
                chrome.getSeleniumAddress().toExternalForm());
        List<String> l = new ArrayList<>();
        // l.add("--no-sandbox");
        // l.add("--disable-dev-shm-usage");
        // l.add("--headless");
        // l.add("--disable-gpu");
        // l.add("--remote-allow-origins=*");
        Map<String, Object> m = new HashMap<>();
        m.put("args", l);
        // m.put("extensions", Collections.EMPTY_LIST);
        Map<String, Object> capabilities = new HashMap<>();
        capabilities.put("browserName", "chrome");
        capabilities.put("goog:chromeOptions", m);
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        conf.put("selenium.addresses", chrome.getSeleniumAddress().toExternalForm());
        Map<String, Object> timeouts = new HashMap<>();
        timeouts.put("implicit", 10000);
        timeouts.put("pageLoad", 10000);
        timeouts.put("script", 10000);
        conf.put("selenium.timeouts", timeouts);
        conf.put("selenium.capabilities", capabilities);
        RemoteDriverProtocol protocol = new RemoteDriverProtocol();
        protocol.configure(conf);
        return protocol;
    }

    @BeforeEach
    void init() {
        chrome.start();
    }

    /**
     * you can configure one instance of Selenium to talk to multiple drivers but can't have a
     * multiple instances of the protocol. If there is only one instance and one target, you must
     * wait...
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testBlocking() {
        RemoteDriverProtocol protocol = getProtocol();
        MutableBoolean noException = new MutableBoolean(true);
        MutableObject endTimeFirst = new MutableObject();
        MutableObject startTimeSecond = new MutableObject();
        await().until(() -> httpServer.isRunning());
        final String url = "http://website.test" + ":" + HTTP_PORT + "/";
        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol.getProtocolOutput(url, new Metadata());
                                endTimeFirst.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(
                                                                SeleniumProtocol.MD_KEY_END)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();
        new Thread(
                        () -> {
                            try {
                                ProtocolResponse response =
                                        protocol.getProtocolOutput(url, new Metadata());
                                startTimeSecond.setObject(
                                        Instant.parse(
                                                response.getMetadata()
                                                        .getFirstValue(
                                                                SeleniumProtocol.MD_KEY_START)));
                            } catch (Exception e) {
                                noException.setValue(false);
                            }
                        })
                .start();
        await().until(
                        () ->
                                endTimeFirst.getObject() != null
                                        && startTimeSecond.getObject() != null);
        Instant etf = (Instant) endTimeFirst.getObject();
        Instant sts = (Instant) startTimeSecond.getObject();
        // check that the second call started AFTER the first one finished
        Assertions.assertTrue(etf.isBefore(sts));
        Assertions.assertTrue(noException.booleanValue());
        protocol.cleanup();
    }
}
