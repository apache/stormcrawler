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

package org.apache.stormcrawler.bolt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.RobotRules;
import org.apache.stormcrawler.protocol.RobotRulesParser;
import org.apache.stormcrawler.protocol.StuckProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RobotRulesLookupTest {

    private static final String URL = "http://a.test/page";

    private final Map<String, Long> counts = new HashMap<>();

    private final ScopedCounter counter = name -> n -> counts.merge(name, n, Long::sum);

    private FetchTimeoutHelpers helpers;

    @AfterEach
    void shutdownHelpers() {
        if (helpers != null) {
            helpers.shutdown();
        }
    }

    private RobotRulesLookup lookup(int timeoutSecs) {
        Config conf = new Config();
        conf.put(Constants.FETCH_TIMEOUT_PARAM_KEY, timeoutSecs);
        helpers = new FetchTimeoutHelpers(conf, 2, "Test-");
        return new RobotRulesLookup(helpers, counter, 0);
    }

    /** Rules as the HTTP protocols return them, with the lengths of the robots.txt fetched. */
    private static RobotRules rules(int... contentLengths) {
        RobotRules rules = new RobotRules(new SimpleRobotRules(RobotRulesMode.ALLOW_ALL));
        rules.setContentLengthFetched(contentLengths);
        return rules;
    }

    @Test
    void rulesFetchedFromTheServer() throws Exception {
        Protocol protocol = mock(Protocol.class);
        RobotRules rules = rules(120);
        when(protocol.getRobotRules(URL)).thenReturn(rules);

        RobotRulesLookup.Result result = lookup(0).lookup(protocol, URL, null);
        assertSame(rules, result.rules());
        assertFalse(result.fromCache());
        assertEquals(Map.of("robots.fetched", 1L), counts);
    }

    /** Rules served from the protocol's cache carry no fetched lengths. */
    @Test
    void rulesFromTheCache() throws Exception {
        Protocol protocol = mock(Protocol.class);
        RobotRules rules = rules();
        when(protocol.getRobotRules(URL)).thenReturn(rules);

        RobotRulesLookup.Result result = lookup(0).lookup(protocol, URL, null);
        assertSame(rules, result.rules());
        assertTrue(result.fromCache());
        assertEquals(Map.of("robots.fromCache", 1L), counts);
    }

    @Test
    void protocolExceptionPropagates() {
        Protocol protocol = mock(Protocol.class);
        IllegalStateException failure = new IllegalStateException("robots.txt lookup failed");
        when(protocol.getRobotRules(URL)).thenThrow(failure);

        RobotRulesLookup lookup = lookup(0);
        assertSame(
                failure,
                assertThrows(
                        IllegalStateException.class, () -> lookup.lookup(protocol, URL, null)));
        assertEquals(Map.of(), counts);
    }

    /** The empty rules are not a RobotRules, so a timed-out lookup also counts as fetched. */
    @Test
    void timedOutLookupYieldsEmptyRulesAndIsReported() throws Exception {
        StuckProtocol protocol = new StuckProtocol();
        Config conf = new Config();
        conf.put(StuckProtocol.HANG_ROBOTS_KEY, true);
        protocol.configure(conf);

        RobotRulesLookup.Result result = lookup(1).lookup(protocol, URL, null);
        assertSame(RobotRulesParser.EMPTY_RULES, result.rules());
        assertFalse(result.fromCache());
        assertEquals(Map.of("robots.timeout", 1L, "robots.fetched", 1L), counts);
        assertEquals(Set.of("a.test"), protocol.robotsTimedOut());
    }
}
