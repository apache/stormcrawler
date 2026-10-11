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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.stormcrawler.fetcher.CrawlDelayPolicy.Action;
import org.apache.stormcrawler.fetcher.CrawlDelayPolicy.Decision;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CrawlDelayPolicyTest {

    /**
     * Delays in milliseconds. The columns: robots.txt delay, current delay of the fetch queue,
     * fetcher.max.crawl.delay, fetcher.max.crawl.delay.force, fetcher.server.delay,
     * fetcher.server.delay.force, then the expected decision: action, delay, robots crawl delay
     * reported in seconds.
     */
    @ParameterizedTest
    @CsvSource(
            textBlock =
                    """
                    # no delay in robots.txt, or the one the fetch queue already has
                    -9223372036854775808, 1000, 30000, false, 1000, false, UNCHANGED, 0,
                    0,                    1000, 30000, false, 1000, false, UNCHANGED, 0,
                    5000,                 5000, 30000, false, 1000, false, UNCHANGED, 0,
                    60000,               60000, 30000, false, 1000, false, UNCHANGED, 0,
                    # longer than fetcher.max.crawl.delay, which applies before the server delay
                    60000,                1000, 30000, false, 1000, false, SKIP,      0,
                    60000,                1000, 30000, true,  1000, false, APPLY, 30000, 60
                    30500,                1000, 30000, true,  1000, false, APPLY, 30000, 31
                    60000,                1000, 30000, false, 1000, true,  SKIP,      0,
                    60000,                1000, 30000, true,  1000, true,  APPLY, 30000, 60
                    5000,                 1000,     0, false, 1000, false, SKIP,      0,
                    # within fetcher.max.crawl.delay, or no limit when negative
                    30000,                1000, 30000, false, 1000, false, APPLY, 30000,
                    120000,               1000,    -1, false, 1000, false, APPLY, 120000,
                    5000,                 1000, 30000, false, 1000, true,  APPLY,  5000,
                    # shorter than fetcher.server.delay
                    500,                     0, 30000, false, 1000, true,  APPLY,  1000,
                    500,                     0, 30000, false, 1000, false, APPLY,   500,
                    """)
    void decision(
            long robotsDelay,
            long queueDelay,
            long maxCrawlDelay,
            boolean maxCrawlDelayForce,
            long serverDelay,
            boolean serverDelayForce,
            Action action,
            long delay,
            String reportedSecs) {
        CrawlDelayPolicy policy =
                new CrawlDelayPolicy(
                        maxCrawlDelay, maxCrawlDelayForce, serverDelay, serverDelayForce);

        assertEquals(
                new Decision(action, delay, reportedSecs),
                policy.decide("http://a.test/", "a.test", robotsDelay, queueDelay));
    }
}
