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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link HostBlockBolt#blockUntilFor(String, Metadata, String, long, long)} - the
 * decision whether a queue-stream tuple carries a server-requested back-off worth enforcing - and
 * for {@link HostBlockBolt#missingQueueStreamKeys(Map, String)} - the startup check on {@code
 * metadata.persist}.
 */
class HostBlockBoltDecisionTest {

    private static final String KEY = "example.com";
    private static final String RETRY_AFTER_KEY = "protocol.retry-after";
    private static final long NOW_MS = 1_700_000_000_000L;
    private static final long NO_CAP = -1L;

    private static Metadata md(String statusCode, String retryAfter) {
        Metadata md = new Metadata();
        if (statusCode != null) {
            md.setValue("fetch.statusCode", statusCode);
        }
        if (retryAfter != null) {
            md.setValue(RETRY_AFTER_KEY, retryAfter);
        }
        return md;
    }

    @Test
    void blocksOn429WithRetryAfterSeconds() {
        long blockUntil =
                HostBlockBolt.blockUntilFor(KEY, md("429", "120"), RETRY_AFTER_KEY, NO_CAP, NOW_MS);
        assertEquals((NOW_MS + 120_000L) / 1000L, blockUntil);
    }

    @Test
    void blocksOn503WithRetryAfterSeconds() {
        long blockUntil =
                HostBlockBolt.blockUntilFor(KEY, md("503", "60"), RETRY_AFTER_KEY, NO_CAP, NOW_MS);
        assertEquals((NOW_MS + 60_000L) / 1000L, blockUntil);
    }

    @Test
    void ignoresOtherStatusCodesEvenWithHeader() {
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(
                        KEY, md("200", "120"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(
                        KEY, md("301", "120"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(
                        KEY, md("403", "120"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
    }

    @Test
    void ignores429WithoutHeader() {
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(KEY, md("429", null), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
    }

    @Test
    void ignoresMissingStatusCode() {
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(KEY, md(null, "120"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
    }

    @Test
    void ignoresMalformedHeaderAndZeroDelay() {
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(
                        KEY, md("429", "not-a-date"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
        // a zero delay is not worth a frontier round-trip
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(KEY, md("429", "0"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
    }

    @Test
    void capsDelayAtConfiguredMaximum() {
        long capMs = 3_600_000L; // one hour
        long blockUntil =
                HostBlockBolt.blockUntilFor(
                        KEY, md("429", "86400"), RETRY_AFTER_KEY, capMs, NOW_MS);
        assertEquals((NOW_MS + capMs) / 1000L, blockUntil);
    }

    @Test
    void negativeCapMeansUncapped() {
        long blockUntil =
                HostBlockBolt.blockUntilFor(
                        KEY, md("429", "86400"), RETRY_AFTER_KEY, NO_CAP, NOW_MS);
        assertEquals((NOW_MS + 86_400_000L) / 1000L, blockUntil);
    }

    @Test
    void hugeUncappedDelayClampsInsteadOfOverflowing() {
        // 9223372036854775 seconds survive the parser's multiplyExact (just
        // below Long.MAX_VALUE in ms); the addition of nowMs must clamp, not
        // wrap negative and silently skip the block (regression)
        long blockUntil =
                HostBlockBolt.blockUntilFor(
                        KEY, md("429", "9223372036854775"), RETRY_AFTER_KEY, NO_CAP, NOW_MS);
        assertEquals(Long.MAX_VALUE / 1000L, blockUntil);
        assertTrue(blockUntil > NOW_MS / 1000L);
    }

    @Test
    void defaultQueueSentinelIsNeverBlocked() {
        // a 429 on a URL whose partition key fell back to the shared catch-all
        // queue must not stall every unrelated URL in it
        assertEquals(
                -1L,
                HostBlockBolt.blockUntilFor(
                        "_DEFAULT_", md("429", "120"), RETRY_AFTER_KEY, NO_CAP, NOW_MS));
    }

    @Test
    void nullMetadataIsIgnored() {
        assertEquals(-1L, HostBlockBolt.blockUntilFor(KEY, null, RETRY_AFTER_KEY, NO_CAP, NOW_MS));
    }

    @Test
    void missingQueueStreamKeysReportsBothWithDefaultConfig() {
        // with the default metadata.persist neither key survives the filter
        Set<String> missing =
                HostBlockBolt.missingQueueStreamKeys(new HashMap<>(), RETRY_AFTER_KEY);
        assertEquals(Set.of("fetch.statusCode", RETRY_AFTER_KEY), missing);
    }

    @Test
    void missingQueueStreamKeysEmptyWhenBothPersisted() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("metadata.persist", List.of("fetch.statusCode", RETRY_AFTER_KEY));
        assertTrue(HostBlockBolt.missingQueueStreamKeys(conf, RETRY_AFTER_KEY).isEmpty());
    }

    @Test
    void missingQueueStreamKeysHonoursWildcards() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("metadata.persist", List.of("fetch.*", "protocol.*"));
        assertTrue(HostBlockBolt.missingQueueStreamKeys(conf, RETRY_AFTER_KEY).isEmpty());
    }
}
