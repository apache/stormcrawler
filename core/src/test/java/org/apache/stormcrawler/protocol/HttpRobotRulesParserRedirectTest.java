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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;
import org.apache.stormcrawler.protocol.okhttp.HttpProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpRobotRulesParserRedirectTest {

    private static final int[] ports = {8089, 8090, 8091, 8092, 8093, 8094, 8095, 8096, 8097};

    private Config conf = new Config();

    private Protocol protocol;

    private String body;

    private String url0 = "http://localhost:" + ports[0];
    private String url1 = "http://localhost:" + ports[1];
    private String url7 = "http://localhost:" + ports[7];
    private String url8 = "http://localhost:" + ports[8];

    private final MockServer mockServer0 =
            new MockServer(WireMockConfiguration.options().port(ports[0]));
    private final MockServer mockServer1 =
            new MockServer(WireMockConfiguration.options().port(ports[1]));
    private final MockServer mockServer2 =
            new MockServer(WireMockConfiguration.options().port(ports[2]));
    private final MockServer mockServer3 =
            new MockServer(WireMockConfiguration.options().port(ports[3]));
    private final MockServer mockServer4 =
            new MockServer(WireMockConfiguration.options().port(ports[4]));
    private final MockServer mockServer5 =
            new MockServer(WireMockConfiguration.options().port(ports[5]));
    private final MockServer mockServer6 =
            new MockServer(WireMockConfiguration.options().port(ports[6]));
    private final MockServer mockServer7 =
            new MockServer(WireMockConfiguration.options().port(ports[7]));
    private final MockServer mockServer8 =
            new MockServer(WireMockConfiguration.options().port(ports[8]));

    @BeforeEach
    void setUp() {
        mockServer0.start();
        mockServer1.start();
        mockServer2.start();
        mockServer3.start();
        mockServer4.start();
        mockServer5.start();
        mockServer6.start();
        mockServer7.start();
        mockServer8.start();
        conf.put("http.agent.name", "this_is_only_a_test");
        ProtocolFactory protocolFactory = ProtocolFactory.getInstance(conf);
        protocol = protocolFactory.getProtocol("http")[0];
        protocolFactory.cleanup();
        String newLine = System.getProperty("line.separator");
        body =
                new StringBuilder()
                        .append("User-agent: this_is_only_a_test")
                        .append(newLine)
                        .append("Disallow: /restricted/")
                        .toString();
    }

    @AfterEach
    void tearDown() {
        mockServer0.stop();
        mockServer1.stop();
        mockServer2.stop();
        mockServer3.stop();
        mockServer4.stop();
        mockServer5.stop();
        mockServer6.stop();
        mockServer7.stop();
        mockServer8.stop();
    }

    @Test
    void testRedirects() {
        // the redirect chains tested here point at other ports of the same host
        Config crossOriginConf = new Config();
        crossOriginConf.putAll(conf);
        crossOriginConf.put("http.robots.redirect.crossorigin.allow", true);
        // Test for 5 consecutive redirects
        configureFor(mockServer1.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[2] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(mockServer2.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[3] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(mockServer3.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[4] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(mockServer4.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[5] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(mockServer5.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[6] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(mockServer6.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(200)));
        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(crossOriginConf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url1);
        Assertions.assertFalse(robotRules.isAllowAll());
        Assertions.assertFalse(robotRules.isAllowNone());
        Assertions.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assertions.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));
        // Test for infinite loop of redirects
        configureFor(mockServer1.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader("location", url1 + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(crossOriginConf);
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url1);
        Assertions.assertTrue(robotRules.isAllowAll());
        // Test relative redirects
        configureFor(mockServer7.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[8] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(301)));
        configureFor(mockServer8.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader("location", "http-redirect-robots.txt")
                                        .withBody(body)
                                        .withStatus(302)));
        configureFor(mockServer8.getClient());
        stubFor(
                get(urlPathEqualTo("/http-redirect-robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader(
                                                "location",
                                                "http://localhost:" + ports[5] + "/robots.txt")
                                        .withBody(body)
                                        .withStatus(302)));
        // from here the redirect leads to the same robots.txt as in the first test block
        httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(crossOriginConf);
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url7);
        Assertions.assertFalse(robotRules.isAllowAll());
        Assertions.assertFalse(robotRules.isAllowNone());
        Assertions.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assertions.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));
        // repeat without creating a new instance of HttpRobotRulesParser:
        // result should be the same, now with cached rules, even for other locations
        // on the redirect chain where the robots.txt is at the root (URL path: /robots.txt)
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url7);
        Assertions.assertFalse(robotRules.isAllowAll());
        Assertions.assertFalse(robotRules.isAllowNone());
        Assertions.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assertions.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));
        robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url8);
        Assertions.assertFalse(robotRules.isAllowAll());
        Assertions.assertFalse(robotRules.isAllowNone());
        Assertions.assertTrue(robotRules.isAllowed(url1 + "/index.html"));
        Assertions.assertFalse(robotRules.isAllowed(url1 + "/restricted/index.html"));
    }

    @Test
    void testRedirectOnTheSameAuthorityIsFollowedByDefault() {
        configureFor(mockServer0.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader("location", "/robots/rules.txt")
                                        .withStatus(301)));
        stubFor(
                get(urlPathEqualTo("/robots/rules.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(200)));
        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url0);
        Assertions.assertFalse(robotRules.isAllowAll());
        Assertions.assertFalse(robotRules.isAllowNone());
        Assertions.assertTrue(robotRules.isAllowed(url0 + "/index.html"));
        Assertions.assertFalse(robotRules.isAllowed(url0 + "/restricted/index.html"));
    }

    @Test
    void testRedirectToAnotherAuthorityIsNotFollowedByDefault() {
        configureFor(mockServer0.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(
                                aResponse()
                                        .withHeader("location", url1 + "/robots.txt")
                                        .withStatus(301)));
        configureFor(mockServer1.getClient());
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(200)));
        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules = httpRobotRulesParser.getRobotRulesSet(protocol, url0);
        mockServer1.verify(0, getRequestedFor(urlPathEqualTo("/robots.txt")));
        Assertions.assertTrue(robotRules.isAllowNone());
    }

    @Test
    void testHeadersNotSentToAnUnfollowedRedirectTarget() {
        Config authConf = new Config();
        authConf.putAll(conf);
        authConf.put("http.basicauth.user", "this_is_only_a_test");
        authConf.put("http.basicauth.password", "this_is_only_a_test");
        HttpProtocol authProtocol = new HttpProtocol();
        authProtocol.configure(authConf);
        try {
            configureFor(mockServer0.getClient());
            stubFor(
                    get(urlPathEqualTo("/robots.txt"))
                            .willReturn(
                                    aResponse()
                                            .withHeader("location", url1 + "/robots.txt")
                                            .withStatus(301)));
            configureFor(mockServer1.getClient());
            stubFor(
                    get(urlPathEqualTo("/robots.txt"))
                            .willReturn(aResponse().withBody(body).withStatus(200)));
            HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
            httpRobotRulesParser.setConf(authConf);
            httpRobotRulesParser.getRobotRulesSet(authProtocol, url0);
            // the configured Authorization header is sent to the host the robots.txt belongs to
            mockServer0.verify(
                    1,
                    getRequestedFor(urlPathEqualTo("/robots.txt"))
                            .withHeader("Authorization", matching("Basic .+")));
            // the target of the redirect is on another port and is not contacted at all
            mockServer1.verify(0, getRequestedFor(urlPathEqualTo("/robots.txt")));
        } finally {
            authProtocol.cleanup();
        }
    }

    private static class MockServer extends WireMockServer {
        public MockServer(Options options) {
            super(options);
        }

        public WireMock getClient() {
            return client;
        }
    }
}
