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

import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Checks the content limit used for the robots.txt fetch. */
class RobotsContentLimitTest {

    /** Protocol stub recording the metadata it is called with. */
    private static class RecordingProtocol implements Protocol {

        Metadata seen;

        @Override
        public void configure(Config conf) {}

        @Override
        public ProtocolResponse getProtocolOutput(String url, Metadata metadata) {
            seen = metadata;
            return new ProtocolResponse(new byte[0], 200, new Metadata());
        }

        @Override
        public crawlercommons.robots.BaseRobotRules getRobotRules(String url) {
            return null;
        }

        @Override
        public void cleanup() {}
    }

    @Test
    void robotsFetchKeepsTheGlobalContentLimit() {
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        // operator sets a finite limit and does not touch http.robots.content.limit
        conf.put("http.content.limit", 65536);

        RecordingProtocol protocol = new RecordingProtocol();
        HttpRobotRulesParser parser = new HttpRobotRulesParser();
        parser.setConf(conf);
        parser.getRobotRulesSet(protocol, "http://limit.example.org/");

        String limit = protocol.seen.getFirstValue("http.content.limit");
        Assertions.assertNull(
                limit,
                "the robots.txt fetch should not override the global content limit, but saw: "
                        + limit);
    }

    @Test
    void robotsSpecificLimitIsApplied() {
        Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        conf.put("http.content.limit", 65536);
        conf.put("http.robots.content.limit", 524288);

        RecordingProtocol protocol = new RecordingProtocol();
        HttpRobotRulesParser parser = new HttpRobotRulesParser();
        parser.setConf(conf);
        parser.getRobotRulesSet(protocol, "http://limit.example.org/");

        Assertions.assertEquals("524288", protocol.seen.getFirstValue("http.content.limit"));
    }
}
