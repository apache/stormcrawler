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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link HostBackoff} - the per-host decision whether a queue-stream tuple calls
 * for blocking the host's frontier queue, and until when. Time is driven by a controllable ticker
 * (escalation decay) and an explicit clock (block ends); jitter is pinned to 0 except where it is
 * the behaviour under test.
 */
class HostBackoffTest {

    private static final String KEY = "example.com";
    private static final String RETRY_AFTER_KEY = "protocol.retry-after";
    private static final long START_MS = 1_700_000_000_000L;
    private static final long BASE_SECS = Constants.URLFRONTIER_BACKOFF_BASE_DEFAULT;

    /** Both the decay ticker and the block clock, advanced together like wall time. */
    private static final class FakeClock implements Ticker {
        private long nowMs = START_MS;

        @Override
        public long read() {
            return TimeUnit.MILLISECONDS.toNanos(nowMs);
        }

        void advanceSecs(long secs) {
            nowMs += secs * 1000L;
        }
    }

    private static final class FixedRandom extends Random {
        private final double value;

        FixedRandom(double value) {
            this.value = value;
        }

        @Override
        public double nextDouble() {
            return value;
        }
    }

    private final FakeClock clock = new FakeClock();

    private static Map<String, Object> conf(Object... keysAndValues) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "protocol.");
        conf.put(Constants.URLFRONTIER_BACKOFF_JITTER_KEY, 0.0);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            conf.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return conf;
    }

    private HostBackoff backoff(Map<String, Object> conf) {
        return new HostBackoff(conf, clock, new FixedRandom(0.0d));
    }

    private static Metadata md(String statusCode, String retryAfter) {
        Metadata md = new Metadata();
        if (statusCode != null) {
            md.setValue(HostBackoff.STATUS_CODE_KEY, statusCode);
        }
        if (retryAfter != null) {
            md.setValue(RETRY_AFTER_KEY, retryAfter);
        }
        return md;
    }

    private long expectedAt(long delaySecs) {
        return (clock.nowMs + delaySecs * 1000L) / 1000L;
    }

    private long signal(HostBackoff backoff, Metadata metadata) {
        return backoff.blockUntilFor(KEY, metadata, clock.nowMs);
    }

    // --- explicit Retry-After path ---

    @Test
    void explicitRetryAfterHonoured() {
        HostBackoff backoff = backoff(conf());
        assertEquals(expectedAt(120), signal(backoff, md("429", "120")));
    }

    @Test
    void explicitRetryAfterCappedAtConfiguredMaximum() {
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_MAX_RETRY_AFTER_KEY, 3600));
        assertEquals(expectedAt(3600), signal(backoff, md("429", "86400")));
    }

    @Test
    void negativeCapMeansUncapped() {
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_MAX_RETRY_AFTER_KEY, -1));
        assertEquals(expectedAt(86400), signal(backoff, md("503", "86400")));
    }

    @Test
    void hugeUncappedDelayClampsInsteadOfOverflowing() {
        // 9223372036854775 seconds survive the parser's multiplyExact (just
        // below Long.MAX_VALUE in ms); the addition of now must clamp, not
        // wrap negative and silently skip the block (regression)
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_MAX_RETRY_AFTER_KEY, -1));
        long blockUntil = signal(backoff, md("429", "9223372036854775"));
        assertEquals(Long.MAX_VALUE / 1000L, blockUntil);
    }

    // --- pressure path: headerless rate limits ---

    @Test
    void headerlessRateLimitBlocksForBase() {
        HostBackoff backoff = backoff(conf());
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
    }

    @Test
    void malformedHeaderEscalatesLikeHeaderless() {
        // Phase 1 ignored a 429 with an unparseable header; the server still
        // rate-limited, so it now feeds the adaptive back-off instead
        HostBackoff backoff = backoff(conf());
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", "not-a-date")));
        clock.advanceSecs(BASE_SECS + 1);
        // a zero delay is no reason to skip the back-off either
        assertEquals(expectedAt(BASE_SECS * 2), signal(backoff, md("429", "0")));
    }

    @Test
    void escalationMultipliesAndCaps() {
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_BACKOFF_MAX_KEY, 200));
        assertEquals(expectedAt(60), signal(backoff, md("503", null)));
        clock.advanceSecs(61);
        assertEquals(expectedAt(120), signal(backoff, md("503", null)));
        clock.advanceSecs(121);
        assertEquals(expectedAt(200), signal(backoff, md("503", null)));
        clock.advanceSecs(201);
        assertEquals(expectedAt(200), signal(backoff, md("503", null)));
    }

    @Test
    void maximumBelowBaseIsRaisedToBase() {
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_BACKOFF_MAX_KEY, 30));
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
        clock.advanceSecs(BASE_SECS + 1);
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
    }

    @Test
    void negativeMaximumIsRaisedToBase() {
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_BACKOFF_MAX_KEY, -1));
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
        clock.advanceSecs(BASE_SECS + 1);
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
    }

    @Test
    void pressureDuringActiveBlockIsOneCongestionEvent() {
        HostBackoff backoff = backoff(conf());
        long block = signal(backoff, md("429", null));
        assertEquals(expectedAt(60), block);
        // the fetcher drains its internal backlog for the host: more 429s
        // arrive while the block is active and must not escalate — they
        // re-assert the unchanged block, in case the original call was lost
        clock.advanceSecs(10);
        assertEquals(block, signal(backoff, md("429", null)));
        clock.advanceSecs(10);
        assertEquals(block, signal(backoff, md("429", null)));
        // after the block expires the next incident escalates exactly one
        // step: the burst counted as a single event
        clock.advanceSecs(41);
        assertEquals(expectedAt(120), signal(backoff, md("429", null)));
    }

    @Test
    void capIsReachableUnderSustainedPressure() {
        // the quiet window counts from the END of the block: a host blocked
        // for longer than the decay window is silent because it is blocked,
        // not because it calmed down, so the escalation must survive
        // (regression: an expire-after-write decay forgot the escalation
        // during any block longer than the decay and capped the real-world
        // curve at the first duration above it)
        HostBackoff backoff = backoff(conf());
        long expected = 60;
        for (int i = 0; i < 6; i++) {
            assertEquals(expectedAt(expected), signal(backoff, md("429", null)));
            clock.advanceSecs(expected + 1);
            expected = Math.min(expected * 2, 86400);
        }
        // the previous block (1920s) outlived the 1800s decay window: the
        // escalation must still hold and keep climbing towards the cap
        assertEquals(expectedAt(3840), signal(backoff, md("429", null)));
    }

    @Test
    void quietHostIsForgivenAfterDecay() {
        HostBackoff backoff = backoff(conf());
        signal(backoff, md("429", null));
        clock.advanceSecs(61);
        assertEquals(expectedAt(120), signal(backoff, md("429", null)));
        // quiet for longer than the decay window: escalation restarts at base
        clock.advanceSecs(120 + Constants.URLFRONTIER_BACKOFF_DECAY_DEFAULT + 1);
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
    }

    // --- only-extend: a block is never shrunk ---

    @Test
    void shorterExplicitSignalNeverShrinksActiveBlock() {
        HostBackoff backoff = backoff(conf());
        long first = signal(backoff, md("429", "3600"));
        assertEquals(expectedAt(3600), first);
        clock.advanceSecs(10);
        // a racing shorter Retry-After must not shrink the active block: the
        // stored end is re-asserted unchanged
        assertEquals(first, signal(backoff, md("429", "60")));
        assertEquals(first, signal(backoff, md("429", "3500")));
    }

    @Test
    void longerExplicitSignalExtendsActiveBlock() {
        HostBackoff backoff = backoff(conf());
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("429", null)));
        // the server now sends an explicit, longer time: honour it even while
        // the pressure block is active
        long extended = signal(backoff, md("429", "3600"));
        assertEquals(expectedAt(3600), extended);
        // and the pressure path stays silenced by the extended block, which
        // is re-asserted as is
        clock.advanceSecs(BASE_SECS + 1);
        assertEquals(extended, signal(backoff, md("429", null)));
    }

    // --- signal gating ---

    @Test
    void statusCodesOutsideConfiguredSetAreIgnored() {
        HostBackoff backoff = backoff(conf());
        assertEquals(-1L, signal(backoff, md("200", "120")));
        assertEquals(-1L, signal(backoff, md("301", "120")));
        assertEquals(-1L, signal(backoff, md("403", "120")));
        assertEquals(-1L, signal(backoff, md(null, "120")));
    }

    @Test
    void configuredStatusCodesReplaceTheDefaults() {
        // YAML parses the codes as numbers
        HostBackoff backoff =
                backoff(conf(Constants.URLFRONTIER_BACKOFF_STATUS_CODES_KEY, List.of(403)));
        assertEquals(expectedAt(120), signal(backoff, md("403", "120")));
        clock.advanceSecs(121);
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md("403", null)));
        assertEquals(
                -1L, backoff.blockUntilFor("other.example.com", md("429", "120"), clock.nowMs));
    }

    @Test
    void defaultQueueSentinelIsNeverBlocked() {
        // a 429 on a URL whose partition key fell back to the shared catch-all
        // queue must not stall every unrelated URL in it
        HostBackoff backoff = backoff(conf());
        assertEquals(-1L, backoff.blockUntilFor("_DEFAULT_", md("429", "120"), clock.nowMs));
    }

    @Test
    void nullMetadataIsIgnored() {
        HostBackoff backoff = backoff(conf());
        assertEquals(-1L, backoff.blockUntilFor(KEY, null, clock.nowMs));
    }

    // --- fetch exceptions as pressure ---

    @Test
    void exceptionsAreIgnoredByDefault() {
        HostBackoff backoff = backoff(conf());
        Metadata md = new Metadata();
        md.setValue(HostBackoff.EXCEPTION_KEY, "Socket timeout fetching");
        assertEquals(-1L, signal(backoff, md));
    }

    @Test
    void exceptionsEscalateWhenEnabledAndStaleResponseKeysAreIgnored() {
        HostBackoff backoff = backoff(conf(Constants.URLFRONTIER_BACKOFF_ON_EXCEPTIONS_KEY, true));
        // the FetcherBolt clears fetch.exception before every fetch, so on an
        // exception tuple the status code and Retry-After header are leftovers
        // of a previous response and must not drive the decision
        Metadata md = md("200", "3600");
        md.setValue(HostBackoff.EXCEPTION_KEY, "Socket timeout fetching");
        assertEquals(expectedAt(BASE_SECS), signal(backoff, md));
    }

    // --- jitter ---

    @Test
    void jitterStretchesTheBlockByAtMostTheConfiguredFraction() {
        Map<String, Object> conf = conf(Constants.URLFRONTIER_BACKOFF_JITTER_KEY, 0.1);
        // lower bound of the jitter range: the block is untouched
        HostBackoff min = new HostBackoff(conf, clock, new FixedRandom(0.0d));
        assertEquals(expectedAt(1000), min.blockUntilFor(KEY, md("429", "1000"), clock.nowMs));
        // upper bound: the block is stretched by the full fraction
        HostBackoff max = new HostBackoff(conf, clock, new FixedRandom(1.0d));
        assertEquals(expectedAt(1100), max.blockUntilFor(KEY, md("429", "1000"), clock.nowMs));
        // and the escalated path is jittered the same way
        assertEquals(
                expectedAt(BASE_SECS + BASE_SECS / 10),
                max.blockUntilFor("other.example.com", md("429", null), clock.nowMs));
    }

    @Test
    void defaultJitterStaysWithinItsBound() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "protocol.");
        HostBackoff backoff = new HostBackoff(conf, clock, new Random(42));
        long blockUntil = backoff.blockUntilFor(KEY, md("429", "1000"), clock.nowMs);
        assertTrue(blockUntil >= expectedAt(1000), "jitter must never shorten a block");
        assertTrue(blockUntil <= expectedAt(1100), "jitter must stay within the fraction");
    }
}
