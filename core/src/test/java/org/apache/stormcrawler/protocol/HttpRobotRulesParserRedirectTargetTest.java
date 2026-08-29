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

import crawlercommons.robots.BaseRobotRules;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Checks which URLs are fetched when a robots.txt responds with a redirect. */
class HttpRobotRulesParserRedirectTargetTest {

    private static final String RULES = "User-agent: this_is_only_a_test\nDisallow: /restricted/";

    /** Protocol stub recording the URLs it is asked to fetch. */
    private static class RecordingProtocol implements Protocol {

        final List<String> requested = new ArrayList<>();

        final Map<String, ProtocolResponse> responses = new HashMap<>();

        void redirect(String from, String to) {
            Metadata md = new Metadata();
            md.setValue("location", to);
            responses.put(from, new ProtocolResponse(new byte[0], 301, md));
        }

        void rules(String url) {
            Metadata md = new Metadata();
            md.setValue("content-type", "text/plain");
            responses.put(
                    url, new ProtocolResponse(RULES.getBytes(StandardCharsets.UTF_8), 200, md));
        }

        @Override
        public void configure(Config conf) {}

        @Override
        public ProtocolResponse getProtocolOutput(String url, Metadata metadata) {
            requested.add(url);
            ProtocolResponse response = responses.get(url);
            if (response == null) {
                return new ProtocolResponse(new byte[0], 404, new Metadata());
            }
            return response;
        }

        @Override
        public BaseRobotRules getRobotRules(String url) {
            return null;
        }

        @Override
        public void cleanup() {}
    }

    private static Config conf() {
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        return conf;
    }

    private static HttpRobotRulesParser parser(Config conf) {
        HttpRobotRulesParser parser = new HttpRobotRulesParser();
        parser.setConf(conf);
        return parser;
    }

    @Test
    void redirectToSameHostIsFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect("http://same.example.org/robots.txt", "/robots/rules.txt");
        protocol.rules("http://same.example.org/robots/rules.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "http://same.example.org/");
        Assertions.assertTrue(
                protocol.requested.contains("http://same.example.org/robots/rules.txt"),
                "expected the redirect target to be fetched, requested: " + protocol.requested);
        Assertions.assertTrue(rules.isAllowed("http://same.example.org/index.html"));
        Assertions.assertFalse(rules.isAllowed("http://same.example.org/restricted/index.html"));
    }

    @Test
    void redirectToOtherHostIsNotFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://host.example.org/robots.txt", "http://elsewhere.example.org/robots.txt");
        protocol.rules("http://elsewhere.example.org/robots.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "http://host.example.org/");
        Assertions.assertFalse(
                protocol.requested.contains("http://elsewhere.example.org/robots.txt"),
                "robots.txt of another host should not be fetched, requested: "
                        + protocol.requested);
        // no rules obtained, so nothing is crawled on that host
        Assertions.assertTrue(rules.isAllowNone());
    }

    @Test
    void redirectToOtherPortIsNotFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://port.example.org/robots.txt", "http://port.example.org:8080/robots.txt");
        protocol.rules("http://port.example.org:8080/robots.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "http://port.example.org/");
        Assertions.assertFalse(
                protocol.requested.contains("http://port.example.org:8080/robots.txt"),
                "robots.txt on another port should not be fetched, requested: "
                        + protocol.requested);
        Assertions.assertTrue(rules.isAllowNone());
    }

    @Test
    void redirectToOtherSchemeIsNotFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect("http://scheme.example.org/robots.txt", "file:/tmp/robots.txt");
        parser(conf()).getRobotRulesSet(protocol, "http://scheme.example.org/");
        Assertions.assertFalse(
                protocol.requested.contains("file:/tmp/robots.txt"),
                "robots.txt fetch should stay on http(s), requested: " + protocol.requested);
    }

    @Test
    void redirectToOtherHostIsFollowedIfConfigured() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect("http://cdn.example.org/robots.txt", "http://cdn.example.com/robots.txt");
        protocol.rules("http://cdn.example.com/robots.txt");
        Config conf = conf();
        conf.put("http.robots.redirect.crossorigin.allow", true);
        BaseRobotRules rules = parser(conf).getRobotRulesSet(protocol, "http://cdn.example.org/");
        Assertions.assertTrue(
                protocol.requested.contains("http://cdn.example.com/robots.txt"),
                "expected the redirect target to be fetched, requested: " + protocol.requested);
        Assertions.assertFalse(rules.isAllowed("http://cdn.example.org/restricted/index.html"));
    }

    @Test
    void schemeUpgradeOnSameHostIsFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://upgrade.example.org/robots.txt", "https://upgrade.example.org/robots.txt");
        protocol.rules("https://upgrade.example.org/robots.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "http://upgrade.example.org/");
        Assertions.assertTrue(
                protocol.requested.contains("https://upgrade.example.org/robots.txt"),
                "expected the redirect target to be fetched, requested: " + protocol.requested);
        Assertions.assertFalse(rules.isAllowed("http://upgrade.example.org/restricted/index.html"));
    }

    @Test
    void schemeUpgradeWithExplicitDefaultPortIsFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://upgrade.example.org/robots.txt",
                "https://upgrade.example.org:443/robots.txt");
        protocol.rules("https://upgrade.example.org:443/robots.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "http://upgrade.example.org/");
        Assertions.assertTrue(
                protocol.requested.contains("https://upgrade.example.org:443/robots.txt"),
                "expected the redirect target to be fetched, requested: " + protocol.requested);
        Assertions.assertFalse(rules.isAllowed("http://upgrade.example.org/restricted/index.html"));
    }

    @Test
    void schemeUpgradeKeepingExplicitPortIsFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://upgrade.example.org:8080/robots.txt",
                "https://upgrade.example.org:8080/robots.txt");
        protocol.rules("https://upgrade.example.org:8080/robots.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "http://upgrade.example.org:8080/");
        Assertions.assertTrue(
                protocol.requested.contains("https://upgrade.example.org:8080/robots.txt"),
                "expected the redirect target to be fetched, requested: " + protocol.requested);
        Assertions.assertFalse(
                rules.isAllowed("http://upgrade.example.org:8080/restricted/index.html"));
    }

    @Test
    void schemeDowngradeOnSameHostIsNotFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "https://downgrade.example.org/robots.txt",
                "http://downgrade.example.org/robots.txt");
        protocol.rules("http://downgrade.example.org/robots.txt");
        BaseRobotRules rules =
                parser(conf()).getRobotRulesSet(protocol, "https://downgrade.example.org/");
        Assertions.assertFalse(
                protocol.requested.contains("http://downgrade.example.org/robots.txt"),
                "robots.txt should not be fetched over http after https, requested: "
                        + protocol.requested);
        Assertions.assertTrue(rules.isAllowNone());
    }

    @Test
    void schemeUpgradeToOtherHostIsNotFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://one.example.org/robots.txt", "https://other.example.org/robots.txt");
        protocol.rules("https://other.example.org/robots.txt");
        parser(conf()).getRobotRulesSet(protocol, "http://one.example.org/");
        Assertions.assertFalse(
                protocol.requested.contains("https://other.example.org/robots.txt"),
                "robots.txt of another host should not be fetched, requested: "
                        + protocol.requested);
    }

    @Test
    void schemeUpgradeToOtherPortIsNotFollowed() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://upgrade.example.org:8080/robots.txt",
                "https://upgrade.example.org:9443/robots.txt");
        protocol.rules("https://upgrade.example.org:9443/robots.txt");
        parser(conf()).getRobotRulesSet(protocol, "http://upgrade.example.org:8080/");
        Assertions.assertFalse(
                protocol.requested.contains("https://upgrade.example.org:9443/robots.txt"),
                "robots.txt on another port should not be fetched, requested: "
                        + protocol.requested);
    }

    @Test
    void redirectWhichIsNotFollowedIsNotCachedAsSuccess() throws MalformedURLException {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://cached.example.org/robots.txt", "http://elsewhere.example.org/robots.txt");
        HttpRobotRulesParser parser = parser(conf());
        BaseRobotRules rules = parser.getRobotRulesSet(protocol, "http://cached.example.org/");
        Assertions.assertTrue(rules.isAllowNone());
        // the cache of successfully fetched rules, which the robots URL filter reads, is untouched
        Assertions.assertTrue(
                parser.getRobotRulesSetFromCache(new URL("http://cached.example.org/"))
                        .isAllowAll());
    }

    @Test
    void redirectWhichIsNotFollowedAllowsCrawlingIfConfigured() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://lenient.example.org/robots.txt", "http://elsewhere.example.org/robots.txt");
        Config conf = conf();
        conf.put("http.robots.redirect.refused.allow", true);
        BaseRobotRules rules =
                parser(conf).getRobotRulesSet(protocol, "http://lenient.example.org/");
        Assertions.assertFalse(
                protocol.requested.contains("http://elsewhere.example.org/robots.txt"),
                "robots.txt of another host should not be fetched, requested: "
                        + protocol.requested);
        Assertions.assertTrue(rules.isAllowAll());
    }

    @Test
    void redirectWhichIsNotFollowedDoesNotApplyToTheHostsOnTheChain() {
        RecordingProtocol protocol = new RecordingProtocol();
        protocol.redirect(
                "http://first.example.org/robots.txt", "http://second.example.org/robots.txt");
        protocol.redirect("http://second.example.org/robots.txt", "file:/tmp/robots.txt");
        Config conf = conf();
        conf.put("http.robots.redirect.crossorigin.allow", true);
        HttpRobotRulesParser parser = parser(conf);
        Assertions.assertTrue(
                parser.getRobotRulesSet(protocol, "http://first.example.org/").isAllowNone());
        // the second host is asked again instead of inheriting the outcome of the chain above
        protocol.rules("http://second.example.org/robots.txt");
        BaseRobotRules rules = parser.getRobotRulesSet(protocol, "http://second.example.org/");
        Assertions.assertFalse(rules.isAllowed("http://second.example.org/restricted/index.html"));
    }
}
