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

import static java.nio.charset.StandardCharsets.UTF_8;

import crawlercommons.robots.BaseRobotRules;
import java.util.Arrays;
import java.util.List;
import org.apache.storm.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Checks that the agent name advertised via {@code http.agent.name} is always part of the set of
 * names used to select the matching groups in a robots.txt, regardless of the content of {@code
 * http.robots.agents}.
 */
class RobotRulesParserAgentNamesTest {

    private static final String ROBOTS_TXT =
            "User-agent: mybot\n"
                    + "Disallow: /restricted/\n"
                    + "\n"
                    + "User-agent: *\n"
                    + "Disallow: /\n";

    private static RobotRulesParser parserWith(String agentName, String robotsAgents) {
        Config conf = new Config();
        conf.put("http.agent.name", agentName);
        if (robotsAgents != null) {
            conf.put("http.robots.agents", robotsAgents);
        }
        RobotRulesParser parser = new HttpRobotRulesParser();
        parser.setConf(conf);
        return parser;
    }

    @Test
    void agentNameUsedWhenNoRobotsAgentsConfigured() {
        RobotRulesParser parser = parserWith("mybot", null);
        Assertions.assertEquals(List.of("mybot"), List.copyOf(parser.agentNames));
    }

    @Test
    void agentNameKeptWhenListedFirst() {
        RobotRulesParser parser = parserWith("mybot", "mybot,mybot-uk");
        Assertions.assertEquals(
                Arrays.asList("mybot", "mybot-uk"),
                List.copyOf(parser.agentNames),
                "http.agent.name must be the first name matched against robots.txt");
    }

    @Test
    void agentNameAddedWhenNotListedFirst() {
        RobotRulesParser parser = parserWith("mybot", "mybot-uk,other-bot");
        Assertions.assertTrue(
                parser.agentNames.contains("mybot"),
                "http.agent.name must be matched against robots.txt even when not listed first, but got "
                        + parser.agentNames);
    }

    @Test
    void groupAddressedToAdvertisedAgentIsSelected() {
        RobotRulesParser parser = parserWith("mybot", "mybot,mybot-uk");
        BaseRobotRules rules =
                parser.parseRules(
                        "http://example.com/robots.txt",
                        ROBOTS_TXT.getBytes(UTF_8),
                        "text/plain",
                        parser.agentNames);
        // the "User-agent: mybot" group only disallows /restricted/, the wildcard group
        // disallows everything - if the advertised name is dropped we fall back to "*"
        Assertions.assertTrue(
                rules.isAllowed("http://example.com/index.html"),
                "fell back to the wildcard group instead of the group for 'mybot'");
        Assertions.assertFalse(rules.isAllowed("http://example.com/restricted/index.html"));
    }
}
