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

package org.apache.stormcrawler.urlfrontier;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link CrawlDelayPolicy} - whether a queue-stream tuple carries a robots
 * crawl-delay worth forwarding to the frontier, with per-host dedupe bounded by the decay window.
 */
class CrawlDelayPolicyTest {

    private static final String KEY = "example.com";

    private static final class FakeTicker implements Ticker {
        private long nanos;

        @Override
        public long read() {
            return nanos;
        }

        void advanceSecs(long secs) {
            nanos += TimeUnit.SECONDS.toNanos(secs);
        }
    }

    private final FakeTicker ticker = new FakeTicker();

    private CrawlDelayPolicy policy(Object... keysAndValues) {
        Map<String, Object> conf = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            conf.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return new CrawlDelayPolicy(conf, ticker);
    }

    private static Metadata md(String delay) {
        Metadata md = new Metadata();
        if (delay != null) {
            md.setValue(org.apache.stormcrawler.Constants.ROBOTS_CRAWL_DELAY_KEY, delay);
        }
        return md;
    }

    @Test
    void newValueIsForwarded() {
        assertEquals(120, policy().delaySecsFor(KEY, md("120")));
    }

    @Test
    void unchangedValueIsDeduplicatedWithinTheWindow() {
        CrawlDelayPolicy policy = policy();
        assertEquals(120, policy.delaySecsFor(KEY, md("120")));
        assertEquals(-1, policy.delaySecsFor(KEY, md("120")));
    }

    @Test
    void changedValueIsForwarded() {
        CrawlDelayPolicy policy = policy();
        assertEquals(120, policy.delaySecsFor(KEY, md("120")));
        assertEquals(60, policy.delaySecsFor(KEY, md("60")));
    }

    @Test
    void unchangedValueIsResentAfterTheWindow() {
        // the dedupe entry expires so a lost RPC converges within one window
        CrawlDelayPolicy policy = policy();
        assertEquals(120, policy.delaySecsFor(KEY, md("120")));
        ticker.advanceSecs(Constants.URLFRONTIER_BACKOFF_DECAY_DEFAULT + 1);
        assertEquals(120, policy.delaySecsFor(KEY, md("120")));
    }

    @Test
    void valueIsCappedAtBackoffMax() {
        CrawlDelayPolicy policy = policy(Constants.URLFRONTIER_BACKOFF_MAX_KEY, 3600);
        assertEquals(3600, policy.delaySecsFor(KEY, md("86400")));
    }

    @Test
    void malformedAbsentZeroAndDefaultQueueAreIgnored() {
        CrawlDelayPolicy policy = policy();
        assertEquals(-1, policy.delaySecsFor(KEY, md("not-a-number")));
        assertEquals(-1, policy.delaySecsFor(KEY, md("0")));
        assertEquals(-1, policy.delaySecsFor(KEY, md(null)));
        assertEquals(-1, policy.delaySecsFor(KEY, null));
        assertEquals(-1, policy.delaySecsFor("_DEFAULT_", md("120")));
    }
}
