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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import crawlercommons.robots.BaseRobotRules;
import org.apache.storm.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WireMockTest
class HttpRobotRulesParserTest {

    private Config conf = new Config();

    private Protocol protocol;

    private String body;

    @BeforeEach
    void setUp() throws Exception {
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

    private void parseRobotRules(int statusCode, Config conf, WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(statusCode)));
        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules =
                httpRobotRulesParser.getRobotRulesSet(protocol, wmRuntimeInfo.getHttpBaseUrl());
        Assertions.assertFalse(robotRules.isAllowAll());
        Assertions.assertFalse(robotRules.isAllowNone());
        Assertions.assertTrue(robotRules.isAllowed(wmRuntimeInfo.getHttpBaseUrl() + "/index.html"));
        Assertions.assertFalse(
                robotRules.isAllowed(wmRuntimeInfo.getHttpBaseUrl() + "/restricted/index.html"));
    }

    private void allowAll(int statusCode, Config conf, WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(statusCode)));
        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules =
                httpRobotRulesParser.getRobotRulesSet(protocol, wmRuntimeInfo.getHttpBaseUrl());
        Assertions.assertTrue(robotRules.isAllowAll());
    }

    private void allowNone(int statusCode, Config conf, WireMockRuntimeInfo wmRuntimeInfo) {
        stubFor(
                get(urlPathEqualTo("/robots.txt"))
                        .willReturn(aResponse().withBody(body).withStatus(statusCode)));
        HttpRobotRulesParser httpRobotRulesParser = new HttpRobotRulesParser();
        httpRobotRulesParser.setConf(conf);
        BaseRobotRules robotRules =
                httpRobotRulesParser.getRobotRulesSet(protocol, wmRuntimeInfo.getHttpBaseUrl());
        Assertions.assertTrue(robotRules.isAllowNone());
    }

    @Test
    void testRobotRulesParsing(WireMockRuntimeInfo wmRuntimeInfo) {
        // Parse as usual for status code 200
        parseRobotRules(200, conf, wmRuntimeInfo);
        // Allow all for range 300-499 (except 429)
        allowAll(300, conf, wmRuntimeInfo);
        allowAll(399, conf, wmRuntimeInfo);
        allowAll(400, conf, wmRuntimeInfo);
        allowAll(403, conf, wmRuntimeInfo);
        allowAll(499, conf, wmRuntimeInfo);
        // Allow none for 429 and range 500-599
        allowNone(429, conf, wmRuntimeInfo);
        allowNone(500, conf, wmRuntimeInfo);
        allowNone(599, conf, wmRuntimeInfo);
        // Allow all for other status codes
        allowAll(299, conf, wmRuntimeInfo);
        allowAll(777, conf, wmRuntimeInfo);
        // Special cases for 403 and 5xx
        Config modifiedConf = new Config();
        modifiedConf.putAll(conf);
        modifiedConf.put("http.robots.403.allow", false);
        modifiedConf.put("http.robots.5xx.allow", true);
        allowNone(403, modifiedConf, wmRuntimeInfo);
        allowAll(500, modifiedConf, wmRuntimeInfo);
    }
}
