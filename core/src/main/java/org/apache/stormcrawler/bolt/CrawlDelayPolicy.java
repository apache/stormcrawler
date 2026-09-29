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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what the crawl delay of robots.txt does to the fetch queue of a URL, following
 * fetcher.max.crawl.delay, fetcher.max.crawl.delay.force, fetcher.server.delay and
 * fetcher.server.delay.force. Delays are in milliseconds.
 */
final class CrawlDelayPolicy {

    // the bolt's category: log configurations for FetcherBolt keep covering these lines
    private static final Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

    enum Action {
        /**
         * Leave the fetch queue as it is: robots.txt has no delay, or the one the queue already
         * has, even above the cap. The caller must not write back the delay it read, which could
         * undo a concurrent raise.
         */
        UNCHANGED,
        /** Do not fetch the URL: robots.txt asks for a longer delay than accepted. */
        SKIP,
        APPLY
    }

    /**
     * @param delay the delay to set on the fetch queue, for {@link Action#APPLY}
     * @param robotsCrawlDelaySecs the robots.txt delay in seconds, rounded up, when it was longer
     *     than fetcher.max.crawl.delay and capped; null otherwise
     */
    record Decision(Action action, long delay, String robotsCrawlDelaySecs) {
        static final Decision UNCHANGED = new Decision(Action.UNCHANGED, 0, null);
        static final Decision SKIP = new Decision(Action.SKIP, 0, null);
    }

    // max. delay accepted from robots.txt, negative for no limit
    private final long maxCrawlDelay;
    // whether maxCrawlDelay overwrites the longer value in robots.txt
    // (otherwise URLs in this queue are skipped)
    private final boolean maxCrawlDelayForce;
    private final long serverDelay;
    // whether the default delay is used even if the robots.txt
    // specifies a shorter crawl-delay
    private final boolean serverDelayForce;

    CrawlDelayPolicy(
            long maxCrawlDelay,
            boolean maxCrawlDelayForce,
            long serverDelay,
            boolean serverDelayForce) {
        this.maxCrawlDelay = maxCrawlDelay;
        this.maxCrawlDelayForce = maxCrawlDelayForce;
        this.serverDelay = serverDelay;
        this.serverDelayForce = serverDelayForce;
    }

    /**
     * @param url the URL, for the log
     * @param queueId the ID of the fetch queue, for the log
     * @param robotsDelay the crawl delay of robots.txt, not positive when there is none
     */
    Decision decide(String url, String queueId, long robotsDelay, long queueDelay) {
        if (robotsDelay <= 0 || robotsDelay == queueDelay) {
            return Decision.UNCHANGED;
        }
        if (robotsDelay > maxCrawlDelay && maxCrawlDelay >= 0) {
            if (!maxCrawlDelayForce) {
                LOG.info("Crawl-Delay for {} too long ({}), skipping", url, robotsDelay);
                return Decision.SKIP;
            }
            LOG.info(
                    "Crawl-Delay for {} too long ({}), using value of fetcher.max.crawl.delay"
                            + " instead",
                    url,
                    robotsDelay);
            // report the delay the fetcher is not holding, so a frontier-side
            // consumer can enforce it at the source (#867)
            return new Decision(
                    Action.APPLY, maxCrawlDelay, Long.toString(1L + ((robotsDelay - 1L) / 1000L)));
        }
        if (robotsDelay < serverDelay && serverDelayForce) {
            LOG.info(
                    "Crawl delay for {} too short ({}), set to fetcher.server.delay",
                    url,
                    robotsDelay);
            return new Decision(Action.APPLY, serverDelay, null);
        }
        LOG.info(
                "Crawl delay for queue: {}  is set to {} as per robots.txt. url: {}",
                queueId,
                robotsDelay,
                url);
        return new Decision(Action.APPLY, robotsDelay, null);
    }
}
