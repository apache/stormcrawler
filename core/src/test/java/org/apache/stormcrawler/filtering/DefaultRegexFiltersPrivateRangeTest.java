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

package org.apache.stormcrawler.filtering;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.regex.RegexURLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The private-range rules shipped in the archetype default-regex-filters.txt, applied to hosts the
 * JVM resolver maps into loopback, link-local and other non-routable space.
 */
class DefaultRegexFiltersPrivateRangeTest {

    private static final List<String> ARCHETYPE_RULES = new ArrayList<>();

    @BeforeAll
    static void loadShippedRules() throws IOException {
        try (InputStream in =
                DefaultRegexFiltersPrivateRangeTest.class.getResourceAsStream(
                        "/default-regex-filters.txt")) {
            Assertions.assertNotNull(in, "the archetype rules file must be on the test classpath");
            for (String line :
                    new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\\r?\\n")) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    ARCHETYPE_RULES.add(trimmed);
                }
            }
        }
    }

    private URLFilter createFilter() {
        ObjectNode filterParams = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode rules = filterParams.putArray("urlFilters");
        for (String rule : ARCHETYPE_RULES) {
            rules.add(rule);
        }
        RegexURLFilter filter = new RegexURLFilter();
        Map<String, Object> conf = new HashMap<>();
        filter.configure(conf, filterParams);
        return filter;
    }

    private void assertRejected(URLFilter filter, String url) throws MalformedURLException {
        URL source = URLUtil.toURL("http://www.example.com/index.html");
        Assertions.assertNull(filter.filter(source, new Metadata(), url), url);
    }

    @Test
    void dottedQuadFormsAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://127.0.0.1/");
        assertRejected(filter, "http://10.0.0.5/");
        assertRejected(filter, "http://192.168.1.1/");
        assertRejected(filter, "http://172.16.0.1/");
    }

    @Test
    void otherNonRoutableRangesAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://169.254.169.254/");
        assertRejected(filter, "http://100.64.0.1/");
        assertRejected(filter, "http://100.127.255.254/");
        assertRejected(filter, "http://0.0.0.0/");
        assertRejected(filter, "http://[fd00::1]/");
        assertRejected(filter, "http://[fe80::1]/");
    }

    /** Both host forms are resolved to 127.0.0.1 by InetAddress.getByName. */
    @Test
    void abbreviatedAndIntegerLoopbackFormsAreRejected() throws MalformedURLException {
        URLFilter filter = createFilter();
        assertRejected(filter, "http://127.1/");
        assertRejected(filter, "http://2130706433/");
    }

    /** The fetcher must not reach private space at all: the shipped IP filter default. */
    @Test
    void ipFilterExcludeListIsShippedEnabled() throws Exception {
        Map<String, Object> defaults =
                org.apache.storm.utils.Utils.findAndReadConfigFile("crawler-default.yaml", false);
        Map<String, Object> conf = org.apache.stormcrawler.util.ConfUtils.extractConfigElement(defaults);
        String exclude =
                org.apache.stormcrawler.util.ConfUtils.getString(
                        conf, "http.filter.ipaddress.exclude", null);
        Assertions.assertNotNull(
                exclude, "http.filter.ipaddress.exclude must be enabled in crawler-default.yaml");
        org.apache.stormcrawler.protocol.IPFilterRules ipFilter =
                new org.apache.stormcrawler.protocol.IPFilterRules(conf);
        Assertions.assertFalse(ipFilter.isEmpty());
        Assertions.assertFalse(
                ipFilter.accept(java.net.InetAddress.getByName("169.254.169.254")),
                "link-local must be excluded by the shipped default");
        Assertions.assertFalse(
                ipFilter.accept(java.net.InetAddress.getByName("127.0.0.1")),
                "loopback must be excluded by the shipped default");
        Assertions.assertTrue(
                ipFilter.accept(java.net.InetAddress.getByName("140.211.11.131")),
                "public addresses must still be accepted");
    }
}
