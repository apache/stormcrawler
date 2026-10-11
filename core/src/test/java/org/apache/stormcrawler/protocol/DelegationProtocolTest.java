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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.DelegatorProtocol.FilteredProtocol;
import org.apache.stormcrawler.util.ConfUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DelegationProtocolTest {

    @Test
    void getProtocolTest() throws FileNotFoundException {
        Config conf = new Config();
        ConfUtils.loadConf("src/test/resources/delegator-conf.yaml", conf);
        conf.put("http.agent.name", "this.is.only.a.test");
        DelegatorProtocol superProto = new DelegatorProtocol();
        superProto.configure(conf);
        // try single filter
        Metadata meta = new Metadata();
        meta.setValue("js", "true");
        FilteredProtocol pf = superProto.getProtocolFor("https://stormcrawler.apache.org", meta);
        Assertions.assertEquals("second", pf.id);
        // no filter at all
        meta = new Metadata();
        pf = superProto.getProtocolFor("https://www.example.com/robots.txt", meta);
        Assertions.assertEquals("default", pf.id);
        // should match the last instance
        // as the one above has more than one filter
        meta = new Metadata();
        meta.setValue("domain", "example.com");
        pf = superProto.getProtocolFor("https://example.com", meta);
        Assertions.assertEquals("default", pf.id);
        // everything should match
        meta = new Metadata();
        meta.setValue("test", "true");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");
        pf = superProto.getProtocolFor("https://www.example-two.com", meta);
        Assertions.assertEquals("first", pf.id);
        // should not match
        meta = new Metadata();
        meta.setValue("test", "false");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");
        pf = superProto.getProtocolFor("https://www.example-two.com", meta);
        // OR
        meta = new Metadata();
        meta.setValue("ping", null);
        pf = superProto.getProtocolFor("https://www.example-two.com", meta);
        // URLs
        meta = new Metadata();
        pf = superProto.getProtocolFor("https://www.example-two.com/large.pdf", meta);
        Assertions.assertEquals("fourth", pf.id);
        pf = superProto.getProtocolFor("https://www.example-two.com/large.doc", meta);
        Assertions.assertEquals("fourth", pf.id);
    }

    private static DelegatorProtocol delegator(String... classNames) {
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        conf.put("fetcher.thread.timeout", 1L);
        List<Map<String, Object>> entries = new ArrayList<>();
        for (int i = 0; i < classNames.length; i++) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("className", classNames[i]);
            entry.put("id", "p" + i);
            if (i < classNames.length - 1) {
                entry.put("filters", Map.of("key" + i, "value"));
            }
            entries.add(entry);
        }
        conf.put("protocol.delegator.config", entries);
        DelegatorProtocol delegator = new DelegatorProtocol();
        delegator.configure(conf);
        return delegator;
    }

    /**
     * The capability is resolved per URL: a URL routed to okhttp is timed by okhttp even when
     * another delegate can not cancel its fetches. The filters of the test delegator match on the
     * metadata key "key0" for the first delegate, the last one is the default.
     */
    @Test
    void supportsFetchTimeoutIsResolvedPerUrl() {
        String okhttp = org.apache.stormcrawler.protocol.okhttp.HttpProtocol.class.getName();
        String dummy = DummyProtocol.class.getName();
        String url = "http://example.com/page";
        Metadata toFirst = new Metadata();
        toFirst.setValue("key0", "value");
        Metadata toDefault = new Metadata();

        Assertions.assertTrue(delegator(okhttp, okhttp).supportsFetchTimeout(url, toDefault));
        // routed to the default delegate, okhttp
        Assertions.assertTrue(delegator(dummy, okhttp).supportsFetchTimeout(url, toDefault));
        // routed to the first delegate, which can not cancel
        Assertions.assertFalse(delegator(dummy, okhttp).supportsFetchTimeout(url, toFirst));
        Assertions.assertFalse(delegator(okhttp, dummy).supportsFetchTimeout(url, toDefault));
        // the page goes to okhttp but the robots.txt lookup, routed without "key0", goes to
        // the default delegate which can not cancel: the helper path is needed for the lookup
        Assertions.assertFalse(delegator(okhttp, dummy).supportsFetchTimeout(url, toFirst));
    }

    /**
     * The robots.txt lookup is routed with its own metadata: a delegate matching on it must support
     * the timeout too, otherwise the lookup would run inline on a protocol which can not cancel it.
     */
    @Test
    void supportsFetchTimeoutRequiresTheRobotsDelegateToo() {
        String okhttp = org.apache.stormcrawler.protocol.okhttp.HttpProtocol.class.getName();
        String dummy = DummyProtocol.class.getName();
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        conf.put("fetcher.thread.timeout", 1L);
        Map<String, Object> robotsDelegate = new HashMap<>();
        robotsDelegate.put("className", dummy);
        robotsDelegate.put("id", "robots");
        robotsDelegate.put("filters", Map.of(DelegatorProtocol.ROBOTS, "true"));
        Map<String, Object> pages = new HashMap<>();
        pages.put("className", okhttp);
        pages.put("id", "pages");
        conf.put("protocol.delegator.config", List.of(robotsDelegate, pages));
        DelegatorProtocol delegator = new DelegatorProtocol();
        delegator.configure(conf);
        Assertions.assertFalse(
                delegator.supportsFetchTimeout("http://example.com/page", new Metadata()));
    }
}
