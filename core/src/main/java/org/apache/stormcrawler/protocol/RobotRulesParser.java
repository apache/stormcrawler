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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import crawlercommons.robots.SimpleRobotRulesParser;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class uses crawler-commons for handling the parsing of {@code robots.txt} files. It emits
 * SimpleRobotRules objects, which describe the download permissions as described in
 * SimpleRobotRulesParser.
 */
public abstract class RobotRulesParser {

    public static final Logger LOG = LoggerFactory.getLogger(RobotRulesParser.class);

    protected static Cache<String, RobotRules> CACHE;

    // if a server or client error happened while fetching the robots
    // cache the result for a shorter period before trying again
    protected static Cache<String, RobotRules> ERRORCACHE;

    /**
     * Parameter name to configure the cache for robots. @see http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default value is
     * "maximumSize=10000,expireAfterWrite=6h"
     */
    public static final String cacheConfigParamName = "robots.cache.spec";

    /**
     * Parameter name to configure the cache for robots errors. @see
     * http://docs.guava-libraries.googlecode
     * .com/git/javadoc/com/google/common/cache/CacheBuilderSpec.html Default value is
     * "maximumSize=10000,expireAfterWrite=1h"
     */
    public static final String errorcacheConfigParamName = "robots.error.cache.spec";

    /**
     * A {@link BaseRobotRules} object appropriate for use when the {@code robots.txt} file is empty
     * or missing; all requests are allowed.
     */
    public static final BaseRobotRules EMPTY_RULES = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);

    /**
     * A {@link BaseRobotRules} object appropriate for use when the {@code robots.txt} file is not
     * fetched due to a {@code 403/Forbidden} response; all requests are disallowed.
     */
    public static final BaseRobotRules FORBID_ALL_RULES =
            new SimpleRobotRules(RobotRulesMode.ALLOW_NONE);

    private static final SimpleRobotRulesParser ROBOT_PARSER = new SimpleRobotRulesParser();

    static {
        ROBOT_PARSER.setMaxCrawlDelay(Long.MAX_VALUE);
    }

    protected final Collection<String> agentNames = new LinkedHashSet<>();

    /**
     * Pattern to match a valid user-agent product tokens as defined in <a
     * href="https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.1">RFC 9309, section 2.2.1</a>.
     */
    protected static final Pattern USER_AGENT_PRODUCT_TOKEN_MATCHER =
            Pattern.compile("[a-zA-Z_-]+");

    public RobotRulesParser() {}

    /** Set the {@link Configuration} object. */
    public void setConf(Config conf) {

        // Grab the agent names we advertise to robots files.
        String agentName = ConfUtils.getString(conf, "http.agent.name");
        if (null == agentName) {
            throw new RuntimeException("Agent name not configured!");
        }

        agentName = agentName.toLowerCase(Locale.ROOT);
        checkAgentValue(agentName);

        ArrayList<String> agents = new ArrayList<>();

        List<String> configuredAgentNames = ConfUtils.loadListFromConf("http.robots.agents", conf);
        // backward compatibility
        // if it has a single entry - parse it
        if (configuredAgentNames.size() == 1) {
            StringTokenizer tok = new StringTokenizer(configuredAgentNames.get(0), ",");
            while (tok.hasMoreTokens()) {
                String agent = tok.nextToken().trim().toLowerCase(Locale.ROOT);
                checkAgentValue(agent);
                agents.add(agent);
            }
        } else {
            for (String ag : configuredAgentNames) {
                String agent = ag.trim().toLowerCase(Locale.ROOT);
                checkAgentValue(agent);
                agents.add(agent);
            }
        }

        /*
         * If there are no agents for robots-parsing, use the default agent-string. If
         * both are present, our agent-string should be the first one we advertise to
         * robots-parsing.
         */
        if (agents.isEmpty()) {
            LOG.info(
                    "No agents listed in 'http.robots.agents' property! Using http.agent.name [{}]",
                    agentName);
            this.agentNames.add(agentName.toLowerCase(Locale.ROOT));
        } else {
            int index = 0;
            if ((agents.get(0)).equalsIgnoreCase(agentName)) {
                index++;
            } else {
                LOG.info(
                        "Agent we advertise ({}) not listed first in 'http.robots.agents' property!",
                        agentName);
            }

            // append all the agents from the http.robots.agents property
            for (; index < agents.size(); index++) {
                agentNames.add(agents.get(index));
            }
        }

        String spec =
                ConfUtils.getString(
                        conf, cacheConfigParamName, "maximumSize=10000,expireAfterWrite=6h");
        CACHE = Caffeine.from(spec).build();

        spec =
                ConfUtils.getString(
                        conf, errorcacheConfigParamName, "maximumSize=10000,expireAfterWrite=1h");
        ERRORCACHE = Caffeine.from(spec).build();
    }

    /**
     * Check that the agent is valid as defined in <a
     * href="https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.1">RFC / 9309, section
     * 2.2.1</a>.
     *
     * @param agentName
     */
    protected static void checkAgentValue(String agentName) {
        if (!USER_AGENT_PRODUCT_TOKEN_MATCHER.matcher(agentName).matches()) {
            String message =
                    "Invalid agent name: "
                            + agentName
                            + ". It MUST contain only uppercase and lowercase letters (\"a-z\" and \"A-Z\"), underscores (\"_\"), and hyphens (\"-\")";
            throw new RuntimeException(message);
        }
    }

    /**
     * Parses the robots content using the {@link SimpleRobotRulesParser} from crawler-commons.
     *
     * @param url A string representation of a URL
     * @param content Contents of the robots file in a byte array
     * @param contentType The
     * @param robotNames Collection of robot names
     * @return BaseRobotRules object
     */
    public BaseRobotRules parseRules(
            String url, byte[] content, String contentType, Collection<String> robotNames) {
        return ROBOT_PARSER.parseContent(url, content, contentType, robotNames);
    }

    public BaseRobotRules getRobotRulesSet(Protocol protocol, String url) {
        URL u;
        try {
            u = new URL(url);
        } catch (Exception e) {
            return EMPTY_RULES;
        }
        return getRobotRulesSet(protocol, u);
    }

    public abstract BaseRobotRules getRobotRulesSet(Protocol protocol, URL url);
}
