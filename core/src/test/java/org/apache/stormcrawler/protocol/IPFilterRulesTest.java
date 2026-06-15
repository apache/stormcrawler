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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IPFilterRulesTest {

    private static InetAddress ip(String address) {
        return InetAddresses.forString(address);
    }

    private static IPFilterRules rules(Object include, Object exclude) {
        Map<String, Object> conf = new HashMap<>();
        if (include != null) {
            conf.put(IPFilterRules.INCLUDE_RULES_KEY, include);
        }
        if (exclude != null) {
            conf.put(IPFilterRules.EXCLUDE_RULES_KEY, exclude);
        }
        return new IPFilterRules(conf);
    }

    @Test
    void emptyConfigAcceptsEverything() {
        IPFilterRules r = rules(null, null);
        assertTrue(r.isEmpty());
        assertTrue(r.accept(ip("127.0.0.1")));
        assertTrue(r.accept(ip("8.8.8.8")));
    }

    @Test
    void excludeLoopbackAndSitelocalAsCommaSeparatedString() {
        IPFilterRules r = rules(null, "localhost,sitelocal");
        assertFalse(r.isEmpty());
        assertFalse(r.accept(ip("127.0.0.1")));
        assertFalse(r.accept(ip("::1")));
        assertFalse(r.accept(ip("192.168.1.10")));
        assertFalse(r.accept(ip("10.0.0.1")));
        assertTrue(r.accept(ip("8.8.8.8")));
    }

    @Test
    void excludeAsYamlList() {
        IPFilterRules r = rules(null, Arrays.asList("loopback", "192.168.0.0/16"));
        assertFalse(r.accept(ip("127.0.0.1")));
        assertFalse(r.accept(ip("192.168.5.5")));
        assertTrue(r.accept(ip("172.32.0.1")));
    }

    @Test
    void includeRestrictsToAllowedRangesOnly() {
        IPFilterRules r = rules("10.0.0.0/8", null);
        assertTrue(r.accept(ip("10.1.2.3")));
        assertFalse(r.accept(ip("8.8.8.8")));
        assertFalse(r.accept(ip("127.0.0.1")));
    }

    @Test
    void excludeTakesPrecedenceOverInclude() {
        IPFilterRules r = rules("sitelocal", "192.168.0.0/16");
        assertTrue(r.accept(ip("10.0.0.1")));
        assertFalse(r.accept(ip("192.168.0.1")));
    }

    @Test
    void invalidRuleIsIgnored() {
        IPFilterRules r = rules(null, "not-an-ip,localhost");
        assertFalse(r.accept(ip("127.0.0.1")));
        assertTrue(r.accept(ip("8.8.8.8")));
    }

    @Test
    void nullAddressIsRejectedWhenRulesConfigured() {
        IPFilterRules r = rules(null, "localhost");
        assertFalse(r.accept(null));
    }
}
