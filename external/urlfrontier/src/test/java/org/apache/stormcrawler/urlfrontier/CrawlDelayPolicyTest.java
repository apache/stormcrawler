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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        conf.put("fetcher.max.crawl.delay.force", true);
        conf.put(Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_KEY, true);
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
        assertEquals(120, request(policy(), "120").delaySecs());
    }

    @Test
    void unchangedValueIsDeduplicatedWithinTheWindow() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.requestFor(KEY, md("120")).isPresent());
        assertFalse(policy.completed(first, true).isPresent());
        assertFalse(policy.requestFor(KEY, md("120")).isPresent());
    }

    @Test
    void changedValuesAreSerializedAndOnlyTheMaximumPendingValueIsSent() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.requestFor(KEY, md("60")).isPresent());
        assertFalse(policy.requestFor(KEY, md("180")).isPresent());

        CrawlDelayPolicy.DelayRequest second = policy.completed(first, true).orElseThrow();
        assertEquals(180, second.delaySecs());
        assertFalse(policy.completed(second, true).isPresent());
    }

    @Test
    void unchangedValueIsResentAfterTheWindow() {
        // the dedupe entry expires so a lost RPC converges within one window
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.completed(first, true).isPresent());
        ticker.advanceSecs(Constants.URLFRONTIER_ROBOTS_DELAY_DECAY_DEFAULT + 1);
        assertEquals(120, request(policy, "120").delaySecs());
    }

    @Test
    void lowerValueIsAppliedOnlyAfterTheMaximumExpiresFromTheWindow() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.completed(first, true).isPresent());
        assertFalse(policy.requestFor(KEY, md("60")).isPresent());

        ticker.advanceSecs(Constants.URLFRONTIER_ROBOTS_DELAY_DECAY_DEFAULT + 1);
        assertEquals(60, request(policy, "60").delaySecs());
    }

    @Test
    void valueIsCappedAtRobotsDelayMax() {
        CrawlDelayPolicy policy = policy(Constants.URLFRONTIER_ROBOTS_DELAY_MAX_KEY, 3600);
        assertEquals(3600, request(policy, "86400").delaySecs());
    }

    @Test
    void signalIsIgnoredWhenFetcherForceIsDisabled() {
        CrawlDelayPolicy policy = policy("fetcher.max.crawl.delay.force", false);
        assertFalse(policy.requestFor(KEY, md("120")).isPresent());
    }

    @Test
    void signalIsIgnoredWhenRobotsPacingIsDisabled() {
        CrawlDelayPolicy policy =
                policy(Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_KEY, false);
        assertFalse(policy.requestFor(KEY, md("120")).isPresent());
    }

    @Test
    void failedRpcDoesNotRetryALowerObservedValue() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.requestFor(KEY, md("60")).isPresent());

        // the lower value was dropped at observation time: nothing pends, no self-retry
        assertFalse(policy.completed(first, false).isPresent());
        // the failure invalidated the dedupe entry: the next signal reasserts the value
        assertEquals(120, request(policy, "120").delaySecs());
    }

    @Test
    void failedRpcRetriesAHigherPendingValue() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.requestFor(KEY, md("180")).isPresent());

        CrawlDelayPolicy.DelayRequest second = policy.completed(first, false).orElseThrow();
        assertEquals(180, second.delaySecs());
        assertFalse(policy.completed(second, false).isPresent());
    }

    @Test
    void failedChangedRpcBecomesTheFloorForTheNextSignal() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest initial = request(policy, "120");
        assertFalse(policy.completed(initial, true).isPresent());

        CrawlDelayPolicy.DelayRequest changed = request(policy, "180");
        assertFalse(policy.completed(changed, false).isPresent());

        // The server may have applied 180 before the response was lost. The next signal is not
        // deduplicated, and it must reassert the ambiguous maximum, not its own lower value.
        assertEquals(180, request(policy, "120").delaySecs());
    }

    @Test
    void theAmbiguousFloorExpiresWithTheDecayWindow() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest changed = request(policy, "180");
        assertFalse(policy.completed(changed, false).isPresent());

        ticker.advanceSecs(Constants.URLFRONTIER_ROBOTS_DELAY_DECAY_DEFAULT + 1);
        assertEquals(120, request(policy, "120").delaySecs());
    }

    @Test
    void repeatedInFlightValueNeverRetriesAndFailureReassertsOnTheNextSignal() {
        CrawlDelayPolicy failed = policy();
        CrawlDelayPolicy.DelayRequest failedRequest = request(failed, "120");
        assertFalse(failed.requestFor(KEY, md("120")).isPresent());
        assertFalse(failed.completed(failedRequest, false).isPresent());
        // the failure invalidated the dedupe entry: the next signal reasserts the value
        assertEquals(120, request(failed, "120").delaySecs());

        CrawlDelayPolicy succeeded = policy();
        CrawlDelayPolicy.DelayRequest succeededRequest = request(succeeded, "120");
        assertFalse(succeeded.requestFor(KEY, md("120")).isPresent());
        assertFalse(succeeded.completed(succeededRequest, true).isPresent());
        // the success recorded the value: an unchanged signal is deduplicated
        assertFalse(succeeded.requestFor(KEY, md("120")).isPresent());
    }

    @Test
    void staleCallbackCannotCompleteANewerRequest() {
        CrawlDelayPolicy policy = policy();
        CrawlDelayPolicy.DelayRequest first = request(policy, "120");
        assertFalse(policy.requestFor(KEY, md("180")).isPresent());
        CrawlDelayPolicy.DelayRequest retry = policy.completed(first, false).orElseThrow();
        assertEquals(180, retry.delaySecs());

        // the retry replaced the failed request: a late duplicate callback for it is ignored
        assertFalse(policy.completed(first, true).isPresent());
        assertFalse(policy.completed(retry, true).isPresent());
        // the retry recorded 180: an unchanged signal is deduplicated
        assertFalse(policy.requestFor(KEY, md("180")).isPresent());
    }

    @Test
    void malformedAbsentZeroAndDefaultQueueAreIgnored() {
        CrawlDelayPolicy policy = policy();
        assertFalse(policy.requestFor(KEY, md("not-a-number")).isPresent());
        assertFalse(policy.requestFor(KEY, md("0")).isPresent());
        assertFalse(policy.requestFor(KEY, md(null)).isPresent());
        assertFalse(policy.requestFor(KEY, null).isPresent());
        assertFalse(policy.requestFor("_DEFAULT_", md("120")).isPresent());
    }

    private static CrawlDelayPolicy.DelayRequest request(CrawlDelayPolicy policy, String delay) {
        var request = policy.requestFor(KEY, md(delay));
        assertTrue(request.isPresent());
        return request.orElseThrow();
    }
}
