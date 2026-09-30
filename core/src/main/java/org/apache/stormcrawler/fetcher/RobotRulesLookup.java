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

package org.apache.stormcrawler.fetcher;

import crawlercommons.robots.BaseRobotRules;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.protocol.FetchTimeoutException;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.RobotRules;
import org.apache.stormcrawler.protocol.RobotRulesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Looks up the robots.txt rules of a URL under fetcher.thread.timeout, for the fetcher threads of
 * {@link FetcherBolt}. A lookup which times out yields empty rules. Each lookup which returns rules
 * counts in robots.fetched or robots.fromCache, each timeout also in robots.timeout.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
public final class RobotRulesLookup {

    // the bolt's category: log configurations for FetcherBolt keep covering these lines
    private static final Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

    /**
     * The rules, and whether the protocol served them from its cache: by the {@link RobotRules}
     * convention, cached rules carry no fetched content lengths. The empty rules of a timed-out
     * lookup are not from the cache.
     */
    public record Result(BaseRobotRules rules, boolean fromCache) {}

    private final FetchTimeoutHelpers helpers;
    private final ScopedCounter eventCounter;
    private final int taskId;

    public RobotRulesLookup(FetchTimeoutHelpers helpers, ScopedCounter eventCounter, int taskId) {
        this.helpers = helpers;
        this.eventCounter = eventCounter;
        this.taskId = taskId;
    }

    /**
     * Looks up the rules for the URL, or yields empty rules when the lookup times out.
     *
     * @param metadata the metadata of the URL, possibly null
     * @return the rules and whether they came from the protocol's cache
     * @throws FetchTimeoutHelpers.SaturatedException when every helper thread is busy
     * @throws Exception the protocol's own exception
     */
    public Result lookup(Protocol protocol, String url, Metadata metadata) throws Exception {
        BaseRobotRules rules;
        try {
            rules = helpers.call(() -> protocol.getRobotRules(url), protocol, url, metadata);
        } catch (FetchTimeoutException e) {
            // same outcome as with okhttp, where HttpRobotRulesParser turns a
            // failed lookup into empty rules: the page is fetched without rules.
            // The protocol is told, so that one which caches robots.txt can record
            // the failure and spare the next URLs of the host a full deadline each
            LOG.info("[Fetcher #{}] robots.txt lookup timed out for {}", taskId, url);
            eventCounter.scope("robots.timeout").incrBy(1);
            protocol.robotRulesTimedOut(url);
            rules = RobotRulesParser.EMPTY_RULES;
        }
        boolean fromCache = false;
        if (rules instanceof RobotRules
                && ((RobotRules) rules).getContentLengthFetched().length == 0) {
            fromCache = true;
            eventCounter.scope("robots.fromCache").incrBy(1);
        } else {
            eventCounter.scope("robots.fetched").incrBy(1);
        }
        return new Result(rules, fromCache);
    }
}
